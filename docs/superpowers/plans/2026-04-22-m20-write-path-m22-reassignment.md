# M20 write-path integration + M22 reassignment proper

Status: in progress (2026-04-22).

The shard hash + `LocationProxy` + manual `POST /api/admin/shards/:id`
overrides have all landed (PR #38, #39). What's missing is the other
side of the story:

1. **M20 write-path integration** — today `LocationProxy` is
   advisory. The append path still hashes tenant → box directly via
   `BoxRegistry::route_tenant()`. An operator who sets a shard
   override sees it in `/api/admin/shards/:id` but writes go to the
   old box. This PR wires the proxy into the hot path so overrides
   actually route traffic.

2. **M22 reassignment proper** — today the stale-lease reaper clears
   `owner_node_id = NULL` on dead tablets and stops. Shards that had
   an override pointing at a dead tablet stay orphaned forever —
   writes for those sessions would route to "no live owner" if the
   hot path consulted the proxy (which it doesn't yet). This PR
   extends the reaper to *also* reassign orphaned shards to a
   surviving tablet, round-robin across live owners.

## What's in / out

**In:**
- `BoxRegistry::route_session(tenant, session)` — new routing entry
  point. Consults `LocationProxy::tablet_for_session`; if an
  override exists, routes to that tablet's box; else falls back to
  `route_tenant`.
- A single production hot path — `append_and_fetch_with_actor` in
  `agentdb-core/src/primitives.rs` — switches to `route_session`.
  The admin/read paths keep `route_tenant` (they operate on
  tenant-wide state like tenant registry lookups, not per-session
  data).
- Reaper extension: after `clear_expired_ownership`, scan `shards`
  for rows whose `tablet_id` is in the reaped set and reassign to a
  surviving tablet (round-robin across alive owners). On zero live
  tablets, log WARN and leave the shard orphaned (the cluster is
  down anyway).
- A `reassignments_by_reaper_total` counter on the reaper struct
  (distinct from the existing `reassignments_total`, which today
  counts tablet-clears, not shard moves).
- Integration tests: override routes → reverting routes back →
  reaper reassigns orphaned shards.

**Out of scope (explicitly deferred):**
- **Quiesce/handoff protocol.** Moving a shard mid-traffic today
  means "new writes go to new place, in-flight writes finish on old
  place." No data migration — acceptable for append-only event
  streams where the latest appends are still durable on whichever
  box accepted them. Real handoff requires cross-node state transfer
  and belongs in the multi-node PR.
- **M19 SCRAM auth** — truly orthogonal, belongs in its own PR.
- **M25 hot-tablet split** — requires per-tablet load tracking
  (`TabletMetrics`) that doesn't exist. Separate PR.
- **Cross-node routing.** Multi-node remains blocked on "tablet_id
  == box_id" coupling; a tablet on a different node is not yet
  reachable. This PR makes routing consult the proxy, which is a
  prerequisite for multi-node but not sufficient for it.

## Design

### M20: `route_session` on `BoxRegistry`

```rust
impl BoxRegistry {
    /// Route a (tenant, session) pair to the box that owns its
    /// shard. Consults LocationProxy for an explicit override;
    /// falls back to tenant-based hash routing when no override.
    pub fn route_session(&self, location: &LocationProxy, tenant_external: &str, session_external: &str) -> Arc<Box_> {
        if let Some(tablet_id) = location.tablet_for_session(session_external) {
            if let Some(b) = self.box_by_id(tablet_id as u16) {
                return b;
            }
            // Tablet id out of local range — proxy points at a
            // remote node. Fall back to the tenant hash so the
            // request at least lands *somewhere* rather than
            // panicking. Cross-node forwarding is future work.
        }
        self.route_tenant(tenant_external)
    }
}
```

- **Tablet → box mapping** is identity in the current single-node
  layout: at startup each box `i` in `0..num_boxes` acquires a
  lease on tablet `i`. `box_by_id(tablet_id as u16)` does the
  lookup. If `tablet_id >= num_boxes` (shard points at a tablet
  on another node), we fall through. Multi-node will add a real
  tablet→node→address resolver here.

- **Why not break on fallback?** The hot path must never panic on
  a stale or cross-node shard override. Falling back to the tenant
  hash gives us the pre-M20 behavior — writes go to *some* box,
  state is durable there, and the admin can fix the override.

- **Proxy access.** `AppState` already carries
  `Arc<BoxRegistry>`, `Arc<TenantRegistry>`, `Arc<SessionRegistry>`.
  Add `location: Arc<LocationProxy>` alongside. The server binary
  already constructs the proxy at startup (PR #38); we just wire
  it into `AppState` and through to `append_and_fetch_with_actor`.

### Hot-path integration

`agentdb-core/src/primitives.rs::append_and_fetch_with_actor` gains
an `Option<&LocationProxy>` parameter. `None` → existing
tenant-hash behavior (for tests and call sites that pre-date M20).
`Some(proxy)` → consult the proxy.

The REST handler (`agentdb-api/src/handlers.rs::append_and_fetch`)
passes `Some(&state.location)`. The public-API change is additive;
existing tests keep compiling because the new arg has a sensible
default path.

### M22: reaper extension

After `clear_expired_ownership` returns a list of reaped
`tablet_id`s, do a second pass:

1. `tablets::list_all` — get the full tablet table.
2. Filter to "live" tablets: `owner_node_id.is_some() &&
   lease_expires_at_ms > now_ms`.
3. If the live list is empty, log WARN and return — no one to
   reassign to.
4. For each reaped tablet id, query
   `meta_shards::list_by_tablet(tablet_id)` (new helper) → every
   shard that pointed at the reaped tablet.
5. Round-robin those shards across the live list. Update each via
   `set_assignment(shard_id, Some(new_tablet), now_ms)`.
6. Bump a new counter `reassignments_by_reaper_total`.
7. Log at INFO with the full (shard_id, old_tablet, new_tablet)
   tuples so operators have an audit trail.

**Why round-robin and not load-aware?** Load-aware needs per-tablet
metrics (M25 work). Round-robin is deterministic and the obvious
first-correct policy. Replacing it with a smarter selector later
is a one-function change.

**Interleaved sweeps.** The reaper runs on a 10s tick. Between
"clear tablet T" and "reassign shards of T" there's a window where
the shards point at a `NULL`-owner tablet. Workers hitting those
shards during that window would fall through `route_session` to
`route_tenant` (the override is still set, but
`location.tablet_for_session` returns the reaped tablet id and
`box_by_id` returns `None` because the lease is gone locally).
This is the same safety fallback the M20 path uses for
cross-node / stale overrides — writes land on the default box.
After the second pass, future writes land on the new owner.

**Meta helper needed.** `meta_shards::list_by_tablet(pool,
tablet_id)` — SELECT shard_id FROM shards WHERE tablet_id = ?. One
new 4-line function plus a test.

## Test plan

### Unit tests

`crates/agentdb-core/src/box_runtime.rs`:
- `route_session_falls_back_to_tenant_when_no_override`
- `route_session_uses_override_when_set`
- `route_session_falls_back_on_out_of_range_tablet`

`crates/agentdb-core/src/reaper.rs`:
- `reaper_reassigns_orphaned_shards_after_clearing_tablet`
- `reaper_round_robins_across_live_tablets`
- `reaper_leaves_shards_alone_when_no_live_tablet`

`crates/agentdb-meta/src/shards.rs`:
- `list_by_tablet_returns_only_matching_shards`

### Integration test

New file: `crates/agentdb-api/tests/shard_routing.rs`.

- Bring up a meta-backed registry with `num_boxes = 4`.
- Pick a session id `s`; compute its `shard_id`; observe which box
  `append_and_fetch(tenant=t, session=s, ...)` lands on (default
  tenant-hash path).
- `POST /api/admin/shards/<shard_id>` → assign to a *different*
  tablet/box (e.g. `(box_default + 1) % 4`).
- Refresh the proxy (admin endpoint does this synchronously).
- `append_and_fetch` again → verify it landed on the new box by
  inspecting the box's metrics (appends counter) or the session
  registry's box_id for that session.
- Revert the override → new writes route back to the hash default.

### Reaper integration test

New file: `crates/agentdb-core/tests/reaper_reassignment.rs`.

- Two tablets, two live leases, one shard with override pointing
  at tablet 0.
- Expire tablet 0's lease.
- Start reaper. Wait one tick.
- Verify tablet 0's ownership cleared *and* the shard has been
  reassigned to tablet 1.

## Rollout

Behind an env toggle? **No.** The change is transparent when no
overrides are set (which is the current state — the prototype
ships 0 overrides by default). The test suite exercises the
fallback path, so existing tests + existing behavior are unchanged.

Operators who didn't set overrides see no behavior change.
Operators who did set overrides (today only via the admin API, and
today the overrides were dead letters) see them start taking
effect. Since the overrides have never done anything before this
PR, there's no compatibility hazard.
