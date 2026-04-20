# M9: MVCC Snapshot Read — Design

## Goal

Give AgentDB consistent, point-in-time reads that survive concurrent
writes and tier transitions. Build the primitive once, route the core
read surfaces through it, and leave the write path untouched.

## Motivation

`docs/ARCHITECTURE.md` lists "No MVCC or snapshot read API. Fork is
implemented with a parent pointer trick instead; other queries are
read-committed" under what the prototype does NOT do. Today this
manifests as two concrete correctness hazards:

1. **Tier-transition tear.** `cold_transition` batch-deletes warm
   events and flips the session record in one KV batch. A reader that
   obtained the session record as `Warm` just before the batch and
   scans events just after sees an empty event range. Symmetric hazard
   on `reactivate`.
2. **Multi-call inconsistency.** A client doing `find` → `find` can
   see a session advance under its feet. For a single `find` the
   tear risk is the only hazard; for compound queries (planner +
   filter, or BM25 + vector) the risk is larger because each stage
   reads independently.

A snapshot-read primitive closes both gaps and is a natural foundation
for future features (time-travel debug, wider transaction scope,
snapshot-based fork). M9 builds the primitive and lights up the core
reader surface.

## Scope

**In:**

- New `ReadCtx` abstraction in `agentdb-core` that pins a RocksDB KV
  snapshot plus a per-box `SegmentPinRegistry` reference.
- `KvSnapshot` trait in `agentdb-storage` wrapping `rocksdb::Snapshot`;
  RocksDB-backed default impl + an in-memory stub for tests.
- Snapshot-aware versions of `fetch_last_n_merged`,
  `scan_session_events`, and `HybridPlanner::execute`. Each takes
  `&ReadCtx`.
- `agentdb-api` REST read endpoints and `agentdb-wire`
  `find`/`aggregate`/`count` open a `ReadCtx` at handler entry and
  drop it at handler exit.
- `append_and_fetch` opens a `ReadCtx` after the write, for the
  embedded `fetch_last_n_merged` call.
- Drop `reactivate`'s inline `delete_segment` call. Replace with a
  `SegmentGc` background task per box with configurable tick and grace
  window; defaults `tick=5s`, `grace=30s`.
- Metrics: `open_snapshots`, `segments_pinned_by_snapshots`,
  `segment_gc_reclaimed`, `segment_gc_skipped_pinned` on `BoxMetrics`.
- `StreamBus` broadcasts `SegmentGcRun { box_id, reclaimed,
  skipped_pinned }` when `reclaimed > 0`.
- Update `docs/ARCHITECTURE.md`: remove MVCC from the "NOT" list, add
  a short section describing `ReadCtx` + segment GC.
- Add a "Snapshot read overhead" section to `docs/PERFORMANCE.md` with
  measured `append_and_fetch` and `find` latencies with vs. without
  `ReadCtx`.

**Out:**

- Client-held snapshot handles (`openSnapshot` / `closeSnapshot` over
  the wire). Snapshots are request-scoped.
- Write-side MVCC. Writes keep serializing through the box
  `write_lock`; no conflict detection, no multi-version keys.
- Cross-box global snapshots. `ReadCtx` is opened from one box; reads
  cross box boundaries only if the caller opens multiple `ReadCtx`s
  (not wired up in this milestone).
- Read-your-own-writes inside a multi-step "transaction".
- Versioned text/vector indexes. Indexes stay unversioned; visibility
  is a post-filter on candidate hits.
- Replacing fork. Fork continues to use `parent_seq_at_fork` pointer.
  `ReadCtx` composes with the existing `fetch_last_n_merged` fork
  path; it doesn't change the fork data model.
- Admin endpoints (`/api/admin/boxes`, session list, session detail).
  Keep read-committed behavior; documented as a known limitation.
- Persisting snapshots across box restart. In-memory only; box
  shutdown releases all pins.

## Design

### ReadCtx

```rust
// crates/agentdb-core/src/read_ctx.rs  (new)

use std::sync::Arc;
use std::collections::BTreeSet;
use parking_lot::Mutex;

use agentdb_storage::{BoxId, KvSnapshot, Namespace, TenantId};
use crate::box_runtime::Box_;
use crate::error::Result;
use crate::registry::SessionRegistry;
use crate::types::Session;

pub struct ReadCtx {
    pub(crate) box_id: BoxId,
    snapshot: Arc<dyn KvSnapshot>,
    registry: Arc<SegmentPinRegistry>,
    pinned_segments: Mutex<BTreeSet<String>>,
    session_cache: Mutex<std::collections::HashMap<(TenantId, u64), Session>>,
    opened_at: u64,
}

impl ReadCtx {
    pub fn open(box_: &Box_) -> Self { /* pin snapshot, bump gauge */ }

    pub fn scan_prefix(&self, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + '_>> { .. }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { .. }

    /// Load a session record under the snapshot. If Cold, adds
    /// `segment_path` to the pinned set (idempotent).
    pub fn session(
        &self,
        sessions: &SessionRegistry,
        tenant: TenantId,
        external: &str,
    ) -> Result<Option<Session>> { .. }

    /// Same as `session` but by internal id. Used by planner filters.
    pub fn session_by_internal(
        &self,
        sessions: &SessionRegistry,
        tenant: TenantId,
        internal: u64,
    ) -> Result<Option<Session>> { .. }
}

impl Drop for ReadCtx {
    fn drop(&mut self) {
        let pins = std::mem::take(&mut *self.pinned_segments.lock());
        self.registry.release(pins);
        // `snapshot: Arc<dyn KvSnapshot>` Drop releases the RocksDB handle.
    }
}
```

Key properties:

- **`Send + Sync`.** Must cross `.await` points; planner may share
  across parallel scans.
- **Lazy segment pin.** We do not enumerate every session at open
  time. `ReadCtx::session` pins on first access. Worst case a ReadCtx
  pins O(distinct sessions read) segments.
- **Per-box.** Cross-box reads require multiple `ReadCtx`s; out of
  scope.
- **RAII.** `Drop` releases pins and implicitly the KV snapshot.

### KvSnapshot trait

```rust
// crates/agentdb-storage/src/snapshot.rs  (new)

pub trait KvSnapshot: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn scan_prefix<'a>(&'a self, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + 'a>>;
}

// Trait addition on KvStore:
pub trait KvStore: Send + Sync {
    // existing methods ...
    fn snapshot(&self) -> Arc<dyn KvSnapshot>;
}
```

- `RocksKvStore::snapshot` returns an `Arc<RocksSnapshot>` that holds
  an `Arc<DB>` and a `SnapshotHandle` internally. Lifetime self-owning
  to avoid borrowing `&self` across `.await`.
- In-memory test implementation clones the current `BTreeMap` at
  snapshot time (acceptable for small test datasets).

### SegmentPinRegistry

```rust
// crates/agentdb-core/src/segment_pin.rs  (new)

pub struct SegmentPinRegistry {
    counts: parking_lot::Mutex<HashMap<String, usize>>,
}

impl SegmentPinRegistry {
    pub fn pin(&self, path: &str) { /* increment */ }
    pub fn release(&self, paths: BTreeSet<String>) { /* decrement */ }
    pub fn snapshot_pinned(&self) -> HashSet<String> {
        self.counts.lock().iter()
            .filter_map(|(p, c)| if *c > 0 { Some(p.clone()) } else { None })
            .collect()
    }
}
```

One instance per `Box_`. Cheap operation; contention bounded by
queries per box.

### Snapshot-aware read primitives

All three reader functions change signature to take `&ReadCtx` and
route their reads through it.

```rust
pub fn scan_session_events(ctx: &ReadCtx, session: &Session) -> Result<Vec<Event>> {
    let prefix = prefix_for_session_branch(
        Namespace::Events, session.box_id, session.tenant_internal_id,
        session.internal_id, DEFAULT_BRANCH_ID);
    let mut out = Vec::new();
    for res in ctx.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        if ev.seq < session.next_seq { out.push(ev); }
    }
    Ok(out)
}
```

The `ev.seq < session.next_seq` guard is belt-and-suspenders: the
snapshot should already filter newer events, but the guard also
handles the edge case where a reader is given a stale `Session` value
from outside the ReadCtx.

`fetch_last_n_merged`:
- Own events: `scan_session_events(ctx, &session)` into the N-bounded
  ring.
- Fork: `ctx.session_by_internal(sessions, tenant, fork.parent_session_internal_id)`.
  Pin kicks in if parent is Cold.
- Parent-warm branch: `scan_session_events(ctx, &parent)`.
- Parent-cold branch: read via `parent.segment_path`
  **as captured in the snapshot**. Not the live session record.
- Merge: filter parent events with `e.seq < fork.parent_seq_at_fork`,
  sort by `ts`, return last N.

`HybridPlanner::execute(&self, ctx: &ReadCtx, query: Query)`:
- Text source (`Text(q)`): BM25 lookup returns candidates as
  `DocId { session_internal_id, seq }`.
- Vector source (`Vector(emb, k)`): ANN lookup (HNSW post-M8, or
  brute force pre-M8) returns the same shape.
- Filter source (`Filter(tenant_id, session_id)`): scan via
  `ctx.scan_prefix`.
- **Post-filter visibility pass.** For each candidate:
    1. Resolve `session_internal_id → Session` via `ctx.session_by_internal`
       (memoized in the `ReadCtx::session_cache`).
    2. Include iff `hit.seq < session.next_seq`.
    3. If the session has `forked_from`, and the candidate came from
       an ancestor, require `hit.seq < parent_seq_at_fork` as well.

### Writer / tier-transition interaction

No change to `append_event` or `fork_session`. No change to
`cold_transition`'s write-side behavior: it still writes the segment
first, then batches the warm-event deletes + session record update.

**Single change: `reactivate` stops deleting the segment inline.**

```rust
// tiering.rs::reactivate
kv.write_batch(ops)?;
// removed:
// if let Err(err) = delete_segment(store, &path).await {
//     tracing::warn!(?err, %path, "orphan segment delete failed");
// }
Ok(())
```

Segment lives until GC picks it up.

### Segment GC

```rust
// crates/agentdb-core/src/segment_gc.rs  (new)

pub struct SegmentGcConfig {
    pub tick: Duration,   // default 5s
    pub grace: Duration,  // default 30s
}

pub struct SegmentGc { _handle: JoinHandle<()> }

impl SegmentGc {
    pub fn start(
        store: Arc<dyn ObjectStore>,
        sessions: Arc<SessionRegistry>,
        box_: Arc<Box_>,
        config: SegmentGcConfig,
    ) -> Self { .. }
}

async fn sweep(/* ... */) -> Result<()> {
    let now = now_millis();
    let mut referenced: HashSet<String> = HashSet::new();

    // (1) Live references: current session records in this box.
    for tenant in tenants.list_in_box(box_.id)? {
        for ext in sessions.list_in_tenant(box_.id, tenant.internal_id)? {
            if let Some(s) = sessions.lookup(box_.id, tenant.internal_id, &ext)? {
                if let Some(p) = s.segment_path { referenced.insert(p); }
            }
        }
    }

    // (2) Pinned by open snapshots.
    referenced.extend(box_.segment_pin_registry.snapshot_pinned());

    // (3) List + reap.
    let prefix = format!("objects/segments/{}/", box_.id);
    let candidates = store.list(Some(&prefix.into())).await?;
    for obj in candidates {
        let path = obj.location.to_string();
        let age_ms = now.saturating_sub(obj.last_modified_ms);
        if referenced.contains(&path) {
            box_.metrics.record_gc_skipped_pinned();
            continue;
        }
        if age_ms < config.grace.as_millis() as u64 { continue; }
        match store.delete(&obj.location).await {
            Ok(()) => box_.metrics.record_gc_reclaimed(),
            Err(e) => tracing::warn!(?e, %path, "segment gc delete failed"),
        }
    }
    Ok(())
}
```

`grace` handles the race where a segment has just been written by
`cold_transition` but the session record commit hasn't landed in the
listing yet. Real-world: the batch is a single RocksDB write and the
listing reads current records, so this race is narrow but nonzero
under memtable flush timing.

### Visibility model (summary)

For a `ReadCtx` opened at wall-time T:

- Non-forked session S: visible events = `{ e : e.seq < next_seq_at_T(S) }`.
- Forked child C with `forked_from = Some(fork)`:
  - own events: same rule as above.
  - ancestor events (currently one hop):
    `{ e : e.seq < min(next_seq_at_T(parent), fork.parent_seq_at_fork) }`.

Snapshot boundary ∩ fork boundary. The `min` falls out naturally
because `scan_session_events(ctx, &parent)` iterates the pinned
snapshot (snapshot boundary) and the merge filter applies
`e.seq < fork.parent_seq_at_fork` (fork boundary).

### Observability

Extend `BoxMetrics` (counter unless noted):

- `open_snapshots` (gauge) — bumped by `ReadCtx::open`, dropped by
  `Drop`.
- `segments_pinned_by_snapshots` (gauge) — `registry.snapshot_pinned().len()`
  sampled on each metrics tick.
- `segment_gc_reclaimed` — total segments deleted by GC.
- `segment_gc_skipped_pinned` — times a candidate was skipped because
  still pinned.

`StreamBus` broadcasts:

- `SegmentGcRun { box_id, reclaimed, skipped_pinned }` on any sweep
  where `reclaimed > 0` (avoid noise when the system is idle).

Dashboard: add one column "snapshots" in the per-box tile, and a log
line in the event feed for `SegmentGcRun`. Wire work is minimal.

## Tests

Unit tests in the relevant crate modules:

1. **`snapshot_isolates_cold_transition`** (`tiering.rs`): open
   `ReadCtx`, call `cold_transition` on the session, scan via the
   ReadCtx — must return the pre-transition warm events.
2. **`snapshot_isolates_reactivate`** (`tiering.rs`): Cold session,
   open `ReadCtx` (pins segment), call `reactivate`, read via
   `ReadCtx` — must return the original segment events, and segment
   must still exist on disk.
3. **`segment_pin_survives_reactivate_then_redemote`**
   (`segment_gc.rs`): Cold session A, open ReadCtx pinning segment
   s1. Reactivate, append, re-demote (produces segment s2). Drop
   ReadCtx, run GC with grace passed, s1 reclaimed, s2 still
   referenced.
4. **`gc_respects_grace_window`** (`segment_gc.rs`): orphan segment
   newer than grace is NOT deleted; older is.
5. **`planner_visibility_filters_post_snapshot_writes`**
   (`planner.rs`): text and vector sources return candidates;
   post-snapshot appends that add new events must not appear in the
   planner result returned via the old ReadCtx.
6. **`planner_visibility_respects_fork_boundary`** (`planner.rs`):
   candidates for a session that forked earlier must be filtered by
   both `next_seq_at_T` and `parent_seq_at_fork`.
7. **`fetch_last_n_merged_through_read_ctx_fork_snapshot`**
   (`primitives.rs`): Parent has 5 events. Open ReadCtx. Fork, append
   3 to child, append 2 to parent. Read child via original ReadCtx —
   expect original 5 parent events (no fork in snapshot). Fresh
   ReadCtx — expect child 3 + parent 5 (fork boundary); no parent 6-7.
8. **`concurrent_append_does_not_affect_snapshot_scan`**
   (`primitives.rs`): long-running `scan_session_events(ctx, &s)` in
   task A while task B hammers `append_and_fetch`. A's result count
   equals `next_seq_at_open - 1`.
9. **`read_ctx_drop_releases_pins`** (`segment_pin.rs`): 1k ReadCtxs
   pinning the same segment; `open_snapshots` returns to 0 and
   registry count returns to 0 after all drops.
10. **`mongo_find_consistent_across_tier_transition`** (wire
    integration): concurrent `find` and forced tier transition;
    response contains either all-pre-transition or all-post-transition
    events, never a tear.

Performance additions in `docs/PERFORMANCE.md`:

- `append_and_fetch` p50/p99 with and without ReadCtx (expect <10%
  overhead at prototype scale).
- `find` p50/p99 on a warm session with 10k events, with and without
  ReadCtx.

## Risks / open questions

1. **RocksDB snapshot × long-running reader.** A snapshot pins SSTs
   alive and blocks compaction from reclaiming space. Mitigated by
   request scope — no client handles, so snapshots live milliseconds
   to seconds. If a pathological query holds one open minutes,
   operations would see disk growth. A future follow-up can add a
   `max_snapshot_age` enforced by `Drop` warnings.
2. **Segment GC listing cost.** `list()` over an object store prefix
   is cheap for the prototype (local filesystem) but not free for S3
   at scale. Acceptable at prototype size; follow-up can switch to
   tracking a per-box segment manifest.
3. **Lazy segment pinning missed-race.** Between `ctx.session(..)`
   returning a Cold session and `pin_registry.pin(path)` being called,
   a concurrent reactivate could flip the session and GC could reap
   the old segment. Fix: `ReadCtx::session` pins BEFORE it returns,
   under the registry lock, and the GC sweep reads the pin set after
   reading live session records and after the grace window — three
   belts holding the pants up.
4. **Stub `KvSnapshot` for tests.** The in-memory snapshot (BTreeMap
   clone) does not catch a bug where our production path uses
   `&kv.get(..)` instead of `&snapshot.get(..)`. Mitigation: the test
   suite runs both backends for the snapshot-isolation tests. A stub
   that panics on non-snapshot reads during a ReadCtx's lifetime is a
   follow-up to consider.
5. **M8 (HNSW) already landed.** Merged in main at `aa7850e`. The M8
   work was confined to `crates/agentdb-index/src/vector.rs` (HNSW
   impl), a 6-line plumbing change in `crates/agentdb-core/src/indexes.rs`,
   and `docs/ARCHITECTURE.md` / `docs/PERFORMANCE.md`. None overlap
   with the surfaces M9 modifies. The visibility post-filter operates
   on `DocId { session_internal_id, seq }` which is identical in both
   the brute-force and HNSW implementations, so the filter works
   unchanged against the HNSW-backed planner. M9's edits to
   `ARCHITECTURE.md` (removing the MVCC bullet, adding a ReadCtx
   section) coexist cleanly with M8's edits.

## Success criteria

- All existing `agentdb-core`, `agentdb-wire`, `agentdb-api` tests
  pass unchanged.
- New MVCC tests 1-10 above pass.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `docs/ARCHITECTURE.md` no longer lists MVCC under "What the
  prototype does NOT do" and has a ~1-paragraph MVCC section.
- `docs/PERFORMANCE.md` has a "Snapshot read overhead" section with
  measured numbers and a short interpretation.
- Dashboard's per-box tile shows `open_snapshots`; SSE stream fires
  `SegmentGcRun` when reclamations happen.
