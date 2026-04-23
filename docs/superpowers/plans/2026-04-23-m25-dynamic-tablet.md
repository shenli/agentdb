# M25 continuation: dynamic tablet creation

Status: in progress (2026-04-23).

Shed (PR #45) redistributes shards among *existing* tablets.
Real relief for a hot tablet sometimes needs a *new* tablet —
and the prototype's fixed `BoxRegistry.boxes:
Vec<Arc<Box_>>` hasn't supported runtime growth. This PR
closes that. Operators can:

1. `agentdb show-hot-tablets` — find a saturated tablet
2. `agentdb create-tablet` — grow the box array by one
3. `agentdb shed-tablet <hot> --to <new>` — move half of
   the hot tablet's shards onto the new one

## What's in / out

**In:**
- `BoxRegistry.boxes` changes from `Vec<Arc<Box_>>` to
  `parking_lot::RwLock<Vec<Arc<Box_>>>`. Hot-path readers
  (`route_tenant`, `box_by_id`, `route_session*`) take a
  `.read()` + index.
- `BoxRegistry` stores the `MetaStore`, `LeaseConfig`,
  `node_id`, and admission capacity so `add_box` can
  acquire a lease without the caller threading them
  through.
- `BoxRegistry::add_box(tablet_id)` — atomically:
  1. `meta::tablets::get_or_create(tablet_id)`
  2. `LeaseManager::acquire(tablet_id, node_id, config,
     metrics)`
  3. Build `Box_` with the lease
  4. Append under the write lock
  5. Return `Arc<Box_>`
- Admin endpoint `POST /api/admin/tablets/create` — body
  `{tablet_id: Option<i64>}`. Null → pick `max(existing) +
  1`. Creates meta row, acquires lease locally, returns
  `{tablet_id, owner_node_id}`.
- CLI: `agentdb create-tablet [--id N]`.
- Every `0..num_boxes` iteration updated to iterate the
  current snapshot under the lock (one `.read()` at the
  top of the loop, returns a cloned Vec of `Arc<Box_>` —
  cheap; boxes are Arc'd).
- `LifecycleEngine` for the new box: starts automatically
  when the server binary's existing lifecycle-spawn loop
  (which iterates boxes at startup) is extended to also
  run once per `add_box`. Simpler: the admin endpoint
  starts the lifecycle engine for the new box directly.

**Out of scope (explicit):**
- **Tablet deletion / shrink.** Once a tablet exists, it
  stays. Shrinking requires "where do the shards assigned
  to T go?" plus data migration (the quiesce/handoff gap).
- **Cross-node new-tablet placement.** `add_box` acquires
  the lease locally. An operator who wants the new tablet
  on a different node posts `create` to that node's admin
  endpoint.
- **Auto-create.** The hot-tablet detector doesn't call
  `create` on its own. Operators drive it manually
  (same philosophy as shed — signal + manual action).
- **Index rebuild for new tablet.** A fresh tablet has no
  sessions yet, so the index starts empty. When shed moves
  shards onto it, new writes land in the fresh index; old
  events on the source tablet remain indexed there (data
  handoff gap, still deferred).

## Design

### BoxRegistry interior mutability

```rust
pub struct BoxRegistry {
    boxes: RwLock<Vec<Arc<Box_>>>,
    kv: Arc<dyn KvStore>,
    object_store: Arc<dyn ObjectStore>,
    hlc: Arc<HlcClock>,
    node_id: Option<String>,
    draining: Arc<AtomicBool>,
    drained_at_ms: Arc<OnceLock<i64>>,
    location: OnceLock<Arc<LocationProxy>>,
    node_addr: OnceLock<Arc<NodeAddressProxy>>,
    // NEW: plumbing for add_box
    meta: Option<Arc<MetaStore>>,
    lease_config: LeaseConfig,
    admission_capacity: u32,
}
```

Hot-path read pattern:

```rust
pub fn route_tenant(&self, external: &str) -> Arc<Box_> {
    let boxes = self.boxes.read();
    // FNV hash mod len, as before. len() reads the current
    // snapshot — if a write happens concurrently, the old
    // box is still reachable via the Arc we return.
    ...
}
```

`parking_lot::RwLock` is non-async + very cheap for read-heavy workloads. No await points inside the hot path.

### add_box

```rust
pub async fn add_box(&self, tablet_id: BoxId) -> Result<Arc<Box_>> {
    let Some(meta) = &self.meta else {
        return Err(Error::Invalid {
            field: None,
            message: "cannot add_box on legacy (meta-less) registry".into(),
        });
    };
    // Refuse duplicates: under single-node, tablet_id ==
    // box slot, and add_box requires a fresh one.
    {
        let boxes = self.boxes.read();
        if boxes.iter().any(|b| b.id == tablet_id) {
            return Err(Error::Invalid {
                field: Some("tablet_id".into()),
                message: format!("tablet {tablet_id} already exists locally"),
            });
        }
    }
    meta_tablets::get_or_create(meta.pool(), tablet_id as i64, now_ms_i64()).await?;
    let metrics = Arc::new(BoxMetrics::new());
    let lease = LeaseManager::acquire(
        meta.clone(),
        tablet_id as i64,
        self.node_id.clone().unwrap_or_default(),
        self.lease_config,
        metrics.clone(),
    )
    .await?;
    let mut b = Box_::new_with_lease_and_capacity(
        tablet_id, self.hlc.clone(), Some(lease), self.admission_capacity,
    );
    b.metrics = metrics;
    let arc = Arc::new(b);
    self.boxes.write().push(arc.clone());
    Ok(arc)
}
```

### Admin endpoint

```
POST /api/admin/tablets/create
Body: { "tablet_id": i64 | null }
→ 200 { "tablet_id": i64, "owner_node_id": string }
→ 409 if tablet_id already exists locally
→ 503 if meta / node_id not configured on this server
```

When `tablet_id` is null, the server picks
`max_existing_tablet + 1`. This does a single
`meta_tablets::list_all` → `rows.iter().map(|t|
t.tablet_id).max()`. Low-contention (16K row scan at worst);
admin-plane endpoint, so not hot-path.

### CLI

```
agentdb create-tablet [--id N]   # id defaults to auto-pick
```

Prints the returned JSON so the operator can pipe it into
`shed-tablet --to <id>`.

### LifecycleEngine for the new box

The server binary starts a LifecycleEngine per box at
startup via `for i in 0..num_boxes`. For a new box added
at runtime, the admin endpoint wraps `registry.add_box`
with a helper that *also* spawns the LifecycleEngine for
that box. Clean split: the registry knows how to grow; the
binary knows how to start ticking.

## Test plan

Unit tests (agentdb-core):
- `add_box_grows_the_registry_and_persists_meta_row`
- `add_box_rejects_duplicate_tablet_id`
- `add_box_returns_arc_reachable_via_box_by_id`

Integration test (agentdb-api):
- Start with num_boxes=2
- POST `/api/admin/tablets/create` with no body → returns
  `tablet_id: 2`
- GET `/api/admin/tablets` → includes id 2 owned by this
  node
- POST `/api/admin/shards/<some_shard>` to route a session
  to tablet 2
- POST `/api/tenants/.../sessions/.../append_and_fetch` →
  response is 200, box 2's appends counter increments
- Create-then-shed: burst writes to tablet 0, create
  tablet 2, shed from 0 to 2, verify moved_shards shape

## Rollout

Backward compatible. Servers that never call `create`
behave identically to before (num_boxes is fixed at start,
boxes vec never grows). The RwLock adds a cheap atomic on
the hot path; not measurable under normal load.
