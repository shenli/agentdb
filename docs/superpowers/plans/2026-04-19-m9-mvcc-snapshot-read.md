# M9 MVCC Snapshot Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add consistent point-in-time read semantics to AgentDB via a request-scoped `ReadCtx` that pins a RocksDB snapshot + segment files, threaded through `fetch_last_n_merged`, `scan_session_events`, and `HybridPlanner`.

**Architecture:** A per-box `SegmentPinRegistry` tracks segments referenced by open snapshots. `ReadCtx` bundles an owned `KvSnapshot` + a reference to the registry; on `Drop` it releases its pins. `reactivate` stops deleting segments inline; a new per-box `SegmentGc` sweep reclaims them after a grace window.

**Tech Stack:** Rust, RocksDB (via `rocksdb` crate), `object_store`, `parking_lot::Mutex`, `tokio`, `axum`.

**Source spec:** `docs/superpowers/specs/2026-04-19-m9-mvcc-snapshot-read-design.md`

---

## Phase A — Foundation

### Task 1: Owned KvSnapshot handle in agentdb-storage

**Why:** `KvStore::snapshot<'a>` today returns a snapshot borrowing from `&self`, which the borrow checker won't allow across `.await` points. `ReadCtx` needs to hold a snapshot across async segment reads. Add an `Arc`-flavored variant that owns the RocksDB Arc and extends the snapshot lifetime to `'static`.

**Files:**
- Modify: `crates/agentdb-storage/src/kv.rs` (add trait method)
- Modify: `crates/agentdb-storage/src/rocks.rs` (implement for `RocksKvStore`)
- Test: `crates/agentdb-storage/src/rocks.rs` (tests module at bottom of file — or new if none)

- [ ] **Step 1: Add trait method signature to `KvStore`**

Edit `crates/agentdb-storage/src/kv.rs`, extending the trait:

```rust
pub trait KvStore: Send + Sync {
    // ... existing methods ...

    /// Like `snapshot`, but the returned handle owns its DB reference and
    /// is therefore `'static`, so it is safe to hold across an `.await`
    /// point. Intended for `ReadCtx` and other cross-await readers.
    ///
    /// Takes `Arc<Self>` to ensure the store outlives the snapshot.
    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>>;
}
```

Add `use std::sync::Arc;` at the top of `kv.rs` if not already present.

- [ ] **Step 2: Run existing tests to confirm trait extension compiles for non-Rocks impls**

Run: `cargo build -p agentdb-storage --lib`

Expected: FAIL. Compiler will report that `RocksKvStore` doesn't implement `snapshot_owned`. This confirms the signature landed.

- [ ] **Step 3: Implement `snapshot_owned` on `RocksKvStore`**

Edit `crates/agentdb-storage/src/rocks.rs`. Add a new type `OwnedRocksSnapshotHandle` and implement the trait method. The snapshot's lifetime is extended via a single `unsafe` transmute; safety justification is written as a doc-comment.

```rust
impl KvStore for RocksKvStore {
    // ... existing methods ...

    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>> {
        Ok(Arc::new(OwnedRocksSnapshotHandle::new(self.db.clone())))
    }
}

/// Owned snapshot: holds `Arc<DB>` itself and stores a snapshot whose
/// lifetime has been extended to `'static`. This is safe because:
///   - the `Arc<DB>` field is dropped AFTER the snapshot (Rust drops
///     struct fields in declaration order; snapshot is declared first),
///   - `Arc<DB>` keeps the underlying DB alive for the duration of the
///     snapshot, and
///   - `DB` is not moved once heap-allocated inside the Arc.
///
/// The `unsafe` lives inside `new`. Outside this module, the handle is
/// a safe abstraction.
struct OwnedRocksSnapshotHandle {
    // Drop order: snap drops first, then _db.
    snap: rocksdb::Snapshot<'static>,
    _db: Arc<DB>,
}

impl OwnedRocksSnapshotHandle {
    fn new(db: Arc<DB>) -> Self {
        let snap: rocksdb::Snapshot<'_> = db.snapshot();
        // SAFETY: see the struct-level doc comment. We extend the lifetime
        // from the `&DB` borrow to `'static`; the Arc held in `_db` outlives
        // this transmuted reference because fields are dropped in declared
        // order (snap before _db).
        let snap: rocksdb::Snapshot<'static> = unsafe { std::mem::transmute(snap) };
        Self { snap, _db: db }
    }
}

impl KvSnapshot for OwnedRocksSnapshotHandle {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snap.get(key)?)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let prefix_vec = prefix.to_vec();
        let mut out: Vec<std::result::Result<(Vec<u8>, Vec<u8>), Error>> = Vec::new();
        let iter = self
            .snap
            .iterator(rocksdb::IteratorMode::From(&prefix_vec, rocksdb::Direction::Forward));
        for res in iter {
            match res {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix_vec) {
                        break;
                    }
                    out.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    out.push(Err(Error::from(e)));
                    break;
                }
            }
        }
        Ok(Box::new(out.into_iter()))
    }
}
```

- [ ] **Step 4: Write a unit test for owned snapshot isolation**

Append to the tests module in `crates/agentdb-storage/src/rocks.rs` (or create a `#[cfg(test)] mod tests { ... }` at the bottom if one doesn't exist):

```rust
#[cfg(test)]
mod owned_snapshot_tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn owned_snapshot_sees_put_after_open_as_absent() {
        let dir = TempDir::new().unwrap();
        let store: Arc<RocksKvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        store.put(b"k1", b"v1").unwrap();

        let snap = Arc::clone(&store).snapshot_owned().unwrap();
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));

        store.put(b"k1", b"v2").unwrap();
        // Snapshot still sees v1.
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));

        store.put(b"k2", b"v2").unwrap();
        // Snapshot doesn't see the new key.
        assert!(snap.get(b"k2").unwrap().is_none());
    }

    #[tokio::test]
    async fn owned_snapshot_is_held_across_await() {
        let dir = TempDir::new().unwrap();
        let store: Arc<RocksKvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        store.put(b"k1", b"v1").unwrap();
        let snap = Arc::clone(&store).snapshot_owned().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        // Compiles + runs: proves handle is Send across await.
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-storage --lib`

Expected: PASS, both new tests included. The existing `KvSnapshot` tests continue to pass unchanged.

- [ ] **Step 6: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-storage/
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(storage): owned KvSnapshot that crosses await points"
```

---

### Task 2: `SegmentPinRegistry` in agentdb-core

**Why:** Track which segment paths are pinned by any open `ReadCtx`. `SegmentGc` consults it to avoid deleting a segment still referenced by an in-flight snapshot.

**Files:**
- Create: `crates/agentdb-core/src/segment_pin.rs`
- Modify: `crates/agentdb-core/src/lib.rs` (register module)

- [ ] **Step 1: Write the failing test**

Create `crates/agentdb-core/src/segment_pin.rs`:

```rust
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Tracks segment-file object-store paths currently pinned by open
/// `ReadCtx` instances for one box. Multiple `ReadCtx`s may pin the
/// same path; the registry refcounts.
#[derive(Debug, Default)]
pub struct SegmentPinRegistry {
    counts: Mutex<HashMap<String, usize>>,
}

impl SegmentPinRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one pin for `path`. Idempotent per ReadCtx via the caller's
    /// own local set — see `ReadCtx::pinned_segments`.
    pub fn pin(&self, path: &str) {
        let mut m = self.counts.lock();
        *m.entry(path.to_string()).or_insert(0) += 1;
    }

    /// Release one pin per entry in `paths`. Entries whose count reaches
    /// zero are removed from the map.
    pub fn release(&self, paths: BTreeSet<String>) {
        let mut m = self.counts.lock();
        for p in paths {
            if let Some(c) = m.get_mut(&p) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    m.remove(&p);
                }
            }
        }
    }

    /// Set of currently pinned paths. Used by `SegmentGc`.
    pub fn pinned_paths(&self) -> HashSet<String> {
        self.counts.lock().keys().cloned().collect()
    }

    /// Number of distinct paths currently pinned. Used for observability.
    pub fn pinned_len(&self) -> u64 {
        self.counts.lock().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_and_release_single_path() {
        let r = SegmentPinRegistry::new();
        r.pin("a");
        assert!(r.pinned_paths().contains("a"));
        r.release([String::from("a")].into_iter().collect());
        assert!(!r.pinned_paths().contains("a"));
        assert_eq!(r.pinned_len(), 0);
    }

    #[test]
    fn refcount_multiple_pins_same_path() {
        let r = SegmentPinRegistry::new();
        r.pin("a");
        r.pin("a");
        r.release([String::from("a")].into_iter().collect());
        assert!(r.pinned_paths().contains("a"), "still pinned: count was 2");
        r.release([String::from("a")].into_iter().collect());
        assert!(!r.pinned_paths().contains("a"));
    }

    #[test]
    fn release_unknown_path_is_noop() {
        let r = SegmentPinRegistry::new();
        r.release([String::from("nope")].into_iter().collect());
        assert_eq!(r.pinned_len(), 0);
    }

    #[test]
    fn pinned_len_counts_distinct_paths_only() {
        let r = SegmentPinRegistry::new();
        r.pin("a");
        r.pin("a");
        r.pin("b");
        assert_eq!(r.pinned_len(), 2);
    }
}
```

- [ ] **Step 2: Register the module**

Edit `crates/agentdb-core/src/lib.rs` — add the module declaration in the sorted module list (between `registry` and `segment`):

```rust
pub mod segment_pin;
```

And export:

```rust
pub use segment_pin::SegmentPinRegistry;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-core --lib segment_pin::tests`

Expected: 4 tests PASS.

- [ ] **Step 4: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/segment_pin.rs crates/agentdb-core/src/lib.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(core): SegmentPinRegistry tracking pinned segment paths"
```

---

### Task 3: New BoxMetrics counters and gauges

**Why:** `ReadCtx::open`/`Drop` need to bump/drop `open_snapshots`. `SegmentGc` needs `segment_gc_reclaimed` and `segment_gc_skipped_pinned`. `segments_pinned_by_snapshots` is a gauge sampled from the registry.

**Files:**
- Modify: `crates/agentdb-core/src/metrics.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/agentdb-core/src/metrics.rs`'s tests module:

```rust
    #[test]
    fn open_snapshots_gauge_bump_and_drop() {
        let m = BoxMetrics::new();
        m.bump_open_snapshots();
        m.bump_open_snapshots();
        m.drop_open_snapshot();
        let s = m.snapshot();
        assert_eq!(s.open_snapshots, 1);
    }

    #[test]
    fn open_snapshots_saturates_at_zero() {
        let m = BoxMetrics::new();
        m.drop_open_snapshot();
        assert_eq!(m.snapshot().open_snapshots, 0);
    }

    #[test]
    fn segment_gc_counters_increment() {
        let m = BoxMetrics::new();
        m.record_segment_gc_reclaimed();
        m.record_segment_gc_reclaimed();
        m.record_segment_gc_skipped_pinned();
        let s = m.snapshot();
        assert_eq!(s.segment_gc_reclaimed, 2);
        assert_eq!(s.segment_gc_skipped_pinned, 1);
    }

    #[test]
    fn segments_pinned_snapshot_reads_back_whatever_was_set() {
        let m = BoxMetrics::new();
        m.set_segments_pinned(3);
        assert_eq!(m.snapshot().segments_pinned_by_snapshots, 3);
        m.set_segments_pinned(0);
        assert_eq!(m.snapshot().segments_pinned_by_snapshots, 0);
    }
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p agentdb-core --lib metrics::tests`

Expected: FAIL — the four new methods and the four new `MetricsSnapshot` fields don't exist yet.

- [ ] **Step 3: Extend `BoxMetrics` struct and methods**

Edit `crates/agentdb-core/src/metrics.rs`. Add four fields to the struct and the corresponding methods:

```rust
#[derive(Debug, Default)]
pub struct BoxMetrics {
    pub inserts: AtomicU64,
    pub queries_find: AtomicU64,
    pub queries_aggregate: AtomicU64,
    pub appends: AtomicU64,
    pub forks: AtomicU64,
    pub tier_transitions: AtomicU64,
    pub quota_hits: AtomicU64,
    pub faults_injected: AtomicU64,
    // M9 additions:
    pub open_snapshots: AtomicU64,                  // gauge
    pub segments_pinned_by_snapshots: AtomicU64,    // gauge, sampled from pin registry
    pub segment_gc_reclaimed: AtomicU64,            // counter
    pub segment_gc_skipped_pinned: AtomicU64,       // counter
    pub gauges: RwLock<GaugeSnapshot>,
}
```

Add methods (insert adjacent to the existing counter methods; order doesn't matter for correctness):

```rust
    pub fn bump_open_snapshots(&self) {
        self.open_snapshots.fetch_add(1, Ordering::Relaxed);
    }
    pub fn drop_open_snapshot(&self) {
        // Saturating: avoid underflow if drop races open.
        let prev = self.open_snapshots.load(Ordering::Relaxed);
        if prev > 0 {
            self.open_snapshots
                .compare_exchange(prev, prev - 1, Ordering::Relaxed, Ordering::Relaxed)
                .ok();
        }
    }
    pub fn set_segments_pinned(&self, n: u64) {
        self.segments_pinned_by_snapshots.store(n, Ordering::Relaxed);
    }
    pub fn record_segment_gc_reclaimed(&self) {
        self.segment_gc_reclaimed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_segment_gc_skipped_pinned(&self) {
        self.segment_gc_skipped_pinned.fetch_add(1, Ordering::Relaxed);
    }
```

Extend `MetricsSnapshot`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub inserts: u64,
    pub queries_find: u64,
    pub queries_aggregate: u64,
    pub appends: u64,
    pub forks: u64,
    pub tier_transitions: u64,
    pub quota_hits: u64,
    pub faults_injected: u64,
    pub open_snapshots: u64,
    pub segments_pinned_by_snapshots: u64,
    pub segment_gc_reclaimed: u64,
    pub segment_gc_skipped_pinned: u64,
    pub gauges: GaugeSnapshot,
}
```

Extend `BoxMetrics::snapshot()`:

```rust
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            inserts: self.inserts.load(Ordering::Relaxed),
            queries_find: self.queries_find.load(Ordering::Relaxed),
            queries_aggregate: self.queries_aggregate.load(Ordering::Relaxed),
            appends: self.appends.load(Ordering::Relaxed),
            forks: self.forks.load(Ordering::Relaxed),
            tier_transitions: self.tier_transitions.load(Ordering::Relaxed),
            quota_hits: self.quota_hits.load(Ordering::Relaxed),
            faults_injected: self.faults_injected.load(Ordering::Relaxed),
            open_snapshots: self.open_snapshots.load(Ordering::Relaxed),
            segments_pinned_by_snapshots: self.segments_pinned_by_snapshots.load(Ordering::Relaxed),
            segment_gc_reclaimed: self.segment_gc_reclaimed.load(Ordering::Relaxed),
            segment_gc_skipped_pinned: self.segment_gc_skipped_pinned.load(Ordering::Relaxed),
            gauges: self.gauges.read().clone(),
        }
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core --lib metrics::tests`

Expected: 7 tests PASS (3 existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/metrics.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(metrics): open_snapshots + segment_gc counters/gauges"
```

---

### Task 4: Attach `SegmentPinRegistry` to `Box_`

**Why:** Each `Box_` needs its own registry so per-box `SegmentGc` can consult pinned paths and `ReadCtx::open(box_)` can register its pins.

**Files:**
- Modify: `crates/agentdb-core/src/box_runtime.rs`

- [ ] **Step 1: Write the failing test**

Append to the tests module in `crates/agentdb-core/src/box_runtime.rs`:

```rust
    #[test]
    fn box_has_segment_pin_registry() {
        let b = Box_::new(0);
        b.segment_pins.pin("seg1");
        assert_eq!(b.segment_pins.pinned_len(), 1);
    }
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p agentdb-core --lib box_runtime::tests::box_has_segment_pin_registry`

Expected: FAIL — `Box_::segment_pins` doesn't exist.

- [ ] **Step 3: Add the field to `Box_`**

Edit `crates/agentdb-core/src/box_runtime.rs`:

```rust
use std::sync::Arc;
// ...
use crate::segment_pin::SegmentPinRegistry;

pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
    pub metrics: Arc<BoxMetrics>,
    pub fault: RwLock<Option<InjectedFault>>,
    pub quota: RwLock<Quota>,
    pub segment_pins: Arc<SegmentPinRegistry>,
}

impl Box_ {
    fn new(id: BoxId) -> Self {
        Self {
            id,
            write_lock: Mutex::new(()),
            indexes: tokio::sync::RwLock::new(None),
            metrics: Arc::new(BoxMetrics::new()),
            fault: RwLock::new(None),
            quota: RwLock::new(Quota::default()),
            segment_pins: Arc::new(SegmentPinRegistry::new()),
        }
    }
    // ... existing methods unchanged ...
}
```

- [ ] **Step 4: Run all core tests**

Run: `cargo test -p agentdb-core --lib`

Expected: all existing tests PASS + the new `box_has_segment_pin_registry` test PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/box_runtime.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(core): attach SegmentPinRegistry to Box_"
```

---

### Task 5: `ReadCtx` abstraction

**Why:** Bundles the owned KV snapshot + pin registry + session cache. Request-scoped; drops release all pins and the snapshot.

**Files:**
- Create: `crates/agentdb-core/src/read_ctx.rs`
- Modify: `crates/agentdb-core/src/lib.rs` (register module + export)

- [ ] **Step 1: Write the failing tests**

Create `crates/agentdb-core/src/read_ctx.rs`:

```rust
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use agentdb_storage::{BoxId, KvIter, KvSnapshot, KvStore, Namespace, TenantId};
use parking_lot::Mutex;

use crate::box_runtime::Box_;
use crate::error::Result;
use crate::registry::SessionRegistry;
use crate::segment_pin::SegmentPinRegistry;
use crate::types::{Session, Tier};

/// A request-scoped point-in-time view over one box. Holds a pinned KV
/// snapshot and a set of segment paths pinned in the box's
/// `SegmentPinRegistry`. Drops release all pins and the snapshot.
///
/// Not persisted, not durable — a process restart loses all ReadCtxs
/// and their pins simultaneously, which is safe because all readers
/// would be gone too.
pub struct ReadCtx {
    box_id: BoxId,
    snapshot: Arc<dyn KvSnapshot>,
    registry: Arc<SegmentPinRegistry>,
    metrics: Arc<crate::metrics::BoxMetrics>,
    pinned_segments: Mutex<BTreeSet<String>>,
    session_cache: Mutex<HashMap<(TenantId, u64), Session>>,
}

impl ReadCtx {
    /// Open a new ReadCtx against `box_`'s KV snapshot.
    pub fn open(box_: &Box_, kv: &Arc<dyn KvStore>) -> Result<Self> {
        let snapshot = Arc::clone(kv).snapshot_owned()?;
        box_.metrics.bump_open_snapshots();
        Ok(Self {
            box_id: box_.id,
            snapshot,
            registry: box_.segment_pins.clone(),
            metrics: box_.metrics.clone(),
            pinned_segments: Mutex::new(BTreeSet::new()),
            session_cache: Mutex::new(HashMap::new()),
        })
    }

    pub fn box_id(&self) -> BoxId {
        self.box_id
    }

    /// Scan a key prefix against the pinned snapshot.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        Ok(self.snapshot.scan_prefix(prefix)?)
    }

    /// Point lookup against the pinned snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key)?)
    }

    /// Resolve a session at snapshot time by external id. If the session
    /// is Cold, its `segment_path` is pinned in the registry so GC
    /// cannot delete it while this ReadCtx is live.
    ///
    /// Results are cached per ReadCtx.
    pub fn session(
        &self,
        sessions: &SessionRegistry,
        tenant_internal_id: TenantId,
        session_external: &str,
    ) -> Result<Option<Session>> {
        // First pass: resolve external -> internal via a snapshot-backed KV read.
        let session = sessions.lookup_at(
            self.snapshot.as_ref(),
            self.box_id,
            tenant_internal_id,
            session_external,
        )?;
        let Some(ref s) = session else {
            return Ok(None);
        };
        self.cache_and_pin(s.clone());
        Ok(session)
    }

    /// Resolve a session at snapshot time by internal id.
    pub fn session_by_internal(
        &self,
        sessions: &SessionRegistry,
        tenant_internal_id: TenantId,
        session_internal_id: u64,
    ) -> Result<Option<Session>> {
        let key = (tenant_internal_id, session_internal_id);
        if let Some(cached) = self.session_cache.lock().get(&key).cloned() {
            return Ok(Some(cached));
        }
        let session = sessions.lookup_by_internal_at(
            self.snapshot.as_ref(),
            self.box_id,
            tenant_internal_id,
            session_internal_id,
        )?;
        if let Some(ref s) = session {
            self.cache_and_pin(s.clone());
        }
        Ok(session)
    }

    fn cache_and_pin(&self, s: Session) {
        if s.tier == Tier::Cold {
            if let Some(path) = s.segment_path.as_deref() {
                let mut pins = self.pinned_segments.lock();
                if pins.insert(path.to_string()) {
                    self.registry.pin(path);
                    self.metrics
                        .set_segments_pinned(self.registry.pinned_len());
                }
            }
        }
        let key = (s.tenant_internal_id, s.internal_id);
        self.session_cache.lock().insert(key, s);
    }
}

impl Drop for ReadCtx {
    fn drop(&mut self) {
        let pins = std::mem::take(&mut *self.pinned_segments.lock());
        self.registry.release(pins);
        self.metrics
            .set_segments_pinned(self.registry.pinned_len());
        self.metrics.drop_open_snapshot();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::box_runtime::BoxRegistry;
    use crate::primitives::append_and_fetch;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use agentdb_storage::RocksKvStore;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde_json::json;
    use tempfile::TempDir;

    async fn ctx_setup() -> (
        TempDir,
        TempDir,
        Arc<dyn KvStore>,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        (kv_dir, obj_dir, kv, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn open_bumps_gauge_and_drop_decrements() {
        let (_k, _o, kv, reg, _t, _s) = ctx_setup().await;
        let box_ = reg.route_tenant("t-1");
        {
            let ctx = ReadCtx::open(&box_, &kv).unwrap();
            let snap = box_.metrics.snapshot();
            assert_eq!(snap.open_snapshots, 1);
            drop(ctx);
        }
        let snap = box_.metrics.snapshot();
        assert_eq!(snap.open_snapshots, 0);
    }

    #[tokio::test]
    async fn session_lookup_pins_cold_segment() {
        let (_k, _o, kv, reg, tenants, sessions) = ctx_setup().await;
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"x": 1}), 0, 100).await.unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            crate::tiering::cold_transition(&kv, reg.object_store(), &sessions, &guard, &mut session)
                .await.unwrap();
        }
        assert_eq!(session.tier, Tier::Cold);

        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let _s = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap();
        assert_eq!(box_.segment_pins.pinned_len(), 1);

        drop(ctx);
        assert_eq!(box_.segment_pins.pinned_len(), 0);
    }

    #[tokio::test]
    async fn session_cache_returns_same_value_on_second_call() {
        let (_k, _o, kv, reg, tenants, sessions) = ctx_setup().await;
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"x": 1}), 0, 100).await.unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let a = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap().unwrap();
        let b = ctx.session_by_internal(&sessions, tenant.internal_id, a.internal_id).unwrap().unwrap();
        assert_eq!(a.external_id, b.external_id);
        assert_eq!(a.next_seq, b.next_seq);
    }
}
```

- [ ] **Step 2: Add `SessionRegistry::lookup_at` and `lookup_by_internal_at`**

`ReadCtx` reads sessions via snapshot-backed registry lookups. Add the methods to `crates/agentdb-core/src/registry.rs`. Open the file; locate `SessionRegistry`'s existing `lookup` and `lookup_by_internal` methods; add the snapshot variants directly below each:

```rust
    /// Same as `lookup` but reads via `snapshot` instead of the live KV.
    pub fn lookup_at(
        &self,
        snapshot: &dyn agentdb_storage::KvSnapshot,
        box_id: BoxId,
        tenant_internal_id: TenantId,
        external: &str,
    ) -> Result<Option<Session>> {
        let key = crate::meta::key_session_by_external(box_id, tenant_internal_id, external);
        let Some(by_ext) = snapshot.get(&key)? else {
            return Ok(None);
        };
        let internal_id: u64 = serde_json::from_slice(&by_ext)?;
        self.lookup_by_internal_at(snapshot, box_id, tenant_internal_id, internal_id)
    }

    /// Same as `lookup_by_internal` but reads via `snapshot`.
    pub fn lookup_by_internal_at(
        &self,
        snapshot: &dyn agentdb_storage::KvSnapshot,
        box_id: BoxId,
        tenant_internal_id: TenantId,
        internal_id: u64,
    ) -> Result<Option<Session>> {
        let key = crate::meta::key_session_record(box_id, tenant_internal_id, internal_id);
        let Some(bytes) = snapshot.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }
```

If the existing method names in `registry.rs` differ or the external-id mapping uses a different helper, match the existing pattern exactly. Check `crates/agentdb-core/src/registry.rs` for the precise key functions; they live in `crate::meta`.

- [ ] **Step 3: Register the module**

Edit `crates/agentdb-core/src/lib.rs`:

```rust
pub mod read_ctx;
// ...
pub use read_ctx::ReadCtx;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core --lib read_ctx::tests`

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/read_ctx.rs crates/agentdb-core/src/lib.rs crates/agentdb-core/src/registry.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(core): ReadCtx — request-scoped snapshot + segment pins"
```

---

## Phase B — Thread snapshots through read primitives

### Task 6: Snapshot-aware `scan_session_events`

**Why:** Make `scan_session_events` read from `ReadCtx`'s snapshot. Also add a `seq < next_seq` guard against stale session records.

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`

- [ ] **Step 1: Write the failing test**

Append to the tests module in `crates/agentdb-core/src/primitives.rs`:

```rust
    #[tokio::test]
    async fn scan_session_events_via_read_ctx_is_isolated_from_new_appends() {
        use crate::read_ctx::ReadCtx;
        let (_dir, _obj_dir, registry, tenants, sessions) = context();
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "s-1",
            json!({"i": 1}), 0, 10).await.unwrap();
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let session_at_open = sessions.lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap().unwrap();

        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();

        // Write more events after the ReadCtx opens.
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "s-1",
            json!({"i": 2}), 0, 20).await.unwrap();
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "s-1",
            json!({"i": 3}), 0, 30).await.unwrap();

        let events = scan_session_events(&ctx, &session_at_open).unwrap();
        assert_eq!(events.len(), 1, "snapshot should see only events visible at open");
        assert_eq!(events[0].seq, 1);
    }
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p agentdb-core --lib primitives::tests::scan_session_events_via_read_ctx_is_isolated_from_new_appends`

Expected: FAIL — `scan_session_events` takes `&Arc<dyn KvStore>`, not `&ReadCtx`.

- [ ] **Step 3: Change the signature and body**

Edit `crates/agentdb-core/src/primitives.rs`. Replace the existing `scan_session_events`:

```rust
pub fn scan_session_events(ctx: &crate::read_ctx::ReadCtx, session: &Session) -> Result<Vec<Event>> {
    let prefix = agentdb_storage::prefix_for_session_branch(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut all: Vec<Event> = Vec::new();
    for res in ctx.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        // Belt-and-suspenders: guard against stale session records passed in
        // from outside the snapshot. Under normal use the snapshot filters
        // newer writes already.
        if ev.seq < session.next_seq {
            all.push(ev);
        }
    }
    Ok(all)
}
```

Also update the existing `fetch_last_n` helper (which currently uses the KvStore directly) to take `&ReadCtx`:

```rust
pub fn fetch_last_n(ctx: &crate::read_ctx::ReadCtx, session: &Session, n: usize) -> Result<Vec<Event>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let prefix = agentdb_storage::prefix_for_session_branch(
        Namespace::Events,
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        DEFAULT_BRANCH_ID,
    );
    let mut ring: std::collections::VecDeque<Event> =
        std::collections::VecDeque::with_capacity(n);
    for res in ctx.scan_prefix(&prefix)? {
        let (_k, v) = res?;
        let ev: Event = serde_json::from_slice(&v)?;
        if ev.seq >= session.next_seq {
            continue;
        }
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(ev);
    }
    Ok(ring.into())
}
```

- [ ] **Step 4: Update in-file tests and other callers**

Every existing test in `primitives.rs` that calls `fetch_last_n(&kv, ...)` or `scan_session_events(&kv, ...)` must be updated to open a `ReadCtx` and pass it.

Scan the file and convert each call. Pattern to apply:

```rust
// BEFORE:
let events = fetch_last_n(&kv, &session, 3).unwrap();

// AFTER:
let ctx = ReadCtx::open(&box_, &kv).unwrap();
let events = fetch_last_n(&ctx, &session, 3).unwrap();
```

Tests that don't already have a `Box_` to pass to `ReadCtx::open` (e.g. the `setup()` that only returns a `kv`) should be promoted to use the `context()` helper that returns a `BoxRegistry`, or create an ad-hoc single `Box_` via `BoxRegistry::new(kv.clone(), store, 1)?.box_by_id(0).unwrap()`.

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-core --lib primitives`

Expected: all existing primitives tests + the new `scan_session_events_via_read_ctx_is_isolated_from_new_appends` test PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/primitives.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "refactor(core): scan_session_events + fetch_last_n take &ReadCtx"
```

---

### Task 7: Snapshot-aware `fetch_last_n_merged`

**Why:** The fork-aware merge must use the ReadCtx's snapshot for the parent's warm reads and the snapshot-captured segment path for the parent's cold reads.

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`

- [ ] **Step 1: Write the failing test**

Append to the tests module in `crates/agentdb-core/src/primitives.rs`:

```rust
    #[tokio::test]
    async fn fetch_last_n_merged_via_read_ctx_isolates_post_snapshot_writes() {
        use crate::read_ctx::ReadCtx;
        let (_dir, _obj_dir, registry, tenants, sessions) = context();

        // Parent: 3 events.
        for i in 1..=3u64 {
            append_and_fetch(&registry, &tenants, &sessions, "t-1", "parent",
                json!({"i": i}), 0, i * 10).await.unwrap();
        }
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

        // Fork child before snapshot.
        let parent = sessions.lookup(box_.id, tenant.internal_id, "parent").unwrap().unwrap();
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: parent.internal_id,
            parent_seq_at_fork: parent.next_seq,
        };
        sessions.get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
            .unwrap();
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "child",
            json!({"c": 1}), 0, 100).await.unwrap();

        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();

        // After snapshot open: two more writes (parent + child) invisible to ctx.
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "parent",
            json!({"i": 99}), 0, 200).await.unwrap();
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "child",
            json!({"c": 2}), 0, 210).await.unwrap();

        let child_at_open = ctx.session(&sessions, tenant.internal_id, "child").unwrap().unwrap();
        let got = fetch_last_n_merged(&ctx, registry.object_store(), &sessions, &child_at_open, 10)
            .await.unwrap();
        let seqs: Vec<(u64, u64)> = got.iter().map(|e| (e.ts, e.seq)).collect();
        // Expect: 3 parent events (ts 10, 20, 30) + 1 child event (ts 100).
        // No post-snapshot writes: parent's seq 99 / child's seq 210.
        assert_eq!(seqs.len(), 4);
        assert_eq!(got.iter().filter(|e| e.ts == 200).count(), 0);
        assert_eq!(got.iter().filter(|e| e.ts == 210).count(), 0);
    }
```

- [ ] **Step 2: Run test**

Run: `cargo test -p agentdb-core --lib primitives::tests::fetch_last_n_merged_via_read_ctx_isolates_post_snapshot_writes`

Expected: FAIL — `fetch_last_n_merged` still takes `&Arc<dyn KvStore>`.

- [ ] **Step 3: Update `fetch_last_n_merged` signature and body**

Replace the existing `fetch_last_n_merged` in `crates/agentdb-core/src/primitives.rs`:

```rust
pub async fn fetch_last_n_merged(
    ctx: &crate::read_ctx::ReadCtx,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    session: &Session,
    n: usize,
) -> Result<Vec<Event>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let own = fetch_last_n(ctx, session, n)?;
    let Some(fork) = session.forked_from.as_ref() else {
        return Ok(own);
    };
    let Some(parent) = ctx.session_by_internal(
        sessions,
        session.tenant_internal_id,
        fork.parent_session_internal_id,
    )?
    else {
        return Ok(own);
    };
    let parent_events: Vec<Event> = match parent.tier {
        Tier::Warm => scan_session_events(ctx, &parent)?,
        Tier::Cold => {
            if let Some(path) = parent.segment_path.as_deref() {
                match crate::segment::read_segment(store, path).await {
                    Ok(seg) => seg.events,
                    Err(_) => Vec::new(),
                }
            } else {
                Vec::new()
            }
        }
        Tier::Archive => Vec::new(),
    };
    let mut all: Vec<Event> = own;
    all.extend(
        parent_events
            .into_iter()
            .filter(|e| e.seq < fork.parent_seq_at_fork),
    );
    all.sort_by_key(|e| e.ts);
    let start = all.len().saturating_sub(n);
    Ok(all.split_off(start))
}
```

Add `use crate::types::Tier;` at the top of the file if not already present.

- [ ] **Step 4: Update existing `fetch_last_n_merged_on_forked_session_pulls_parent_tail` test**

The existing test constructs its own `kv` and passes it as `&kv`. Rewrite it to open a `ReadCtx`:

```rust
    #[tokio::test]
    async fn fetch_last_n_merged_on_forked_session_pulls_parent_tail() {
        use crate::read_ctx::ReadCtx;
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;

        let (_dir, _obj_dir, registry, tenants, sessions) = context();
        for i in 1..=3u64 {
            append_and_fetch(&registry, &tenants, &sessions, "t-1", "parent",
                json!({"i": i}), 0, i * 10).await.unwrap();
        }
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let parent = sessions.lookup(box_.id, tenant.internal_id, "parent").unwrap().unwrap();
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: parent.internal_id,
            parent_seq_at_fork: 4,
        };
        sessions.get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
            .unwrap();

        append_and_fetch(&registry, &tenants, &sessions, "t-1", "child",
            json!({"c": 1}), 0, 100).await.unwrap();
        append_and_fetch(&registry, &tenants, &sessions, "t-1", "child",
            json!({"c": 2}), 0, 110).await.unwrap();

        let child = sessions.lookup(box_.id, tenant.internal_id, "child").unwrap().unwrap();
        let ctx = ReadCtx::open(&box_, registry.kv()).unwrap();
        let got = fetch_last_n_merged(&ctx, registry.object_store(), &sessions, &child, 4)
            .await.unwrap();
        let tss: Vec<u64> = got.iter().map(|e| e.ts).collect();
        assert_eq!(tss, vec![20, 30, 100, 110]);
    }
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-core --lib primitives`

Expected: all primitives tests PASS, including the new one and the rewritten fork test.

- [ ] **Step 6: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/primitives.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "refactor(core): fetch_last_n_merged takes &ReadCtx"
```

---

### Task 8: `append_and_fetch` opens a `ReadCtx`

**Why:** `append_and_fetch` currently writes and then reads in one call. After M9 the embedded read should go through a ReadCtx so that the caller gets a consistent view of the just-appended session (including across tier transitions from another task).

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`

- [ ] **Step 1: Update `append_and_fetch` body**

In `crates/agentdb-core/src/primitives.rs`, replace the post-lock-drop read section of `append_and_fetch`:

```rust
    drop(guard);
    // M9: read through a freshly opened ReadCtx so fetch sees a consistent
    // snapshot including the write we just committed. The snapshot opens
    // AFTER the box write lock is dropped, so RocksDB picks up our batch.
    let ctx = crate::read_ctx::ReadCtx::open(&box_, registry.kv())?;
    let recent = fetch_last_n_merged(
        &ctx,
        registry.object_store(),
        sessions,
        &session,
        fetch_last_n_,
    )
    .await?;
    Ok(AppendFetchOutput {
        new_seq: event.seq,
        recent,
        session,
    })
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-core --lib primitives`

Expected: all primitives tests PASS unchanged (the external behavior of `append_and_fetch` is identical; ReadCtx is an implementation detail).

- [ ] **Step 3: Run the wider workspace test suite**

Run: `cargo test --workspace --lib`

Expected: all green. Watch for callers of `scan_session_events` / `fetch_last_n_merged` in other crates that may have broken — those will be addressed in Tasks 9 and 14.

If any tests in `agentdb-wire` or `agentdb-api` fail because of the signature change, note them but do NOT fix in this task — they are addressed in dedicated tasks below.

- [ ] **Step 4: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/primitives.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(core): append_and_fetch reads through ReadCtx"
```

---

### Task 9: `HybridPlanner` takes `&ReadCtx` + visibility post-filter

**Why:** The planner reads KV and segment data during query execution. Thread `&ReadCtx` through the struct, route all KV reads through it, and add a visibility post-filter that drops candidate hits with `seq >= session.next_seq_at_snapshot`.

**Files:**
- Modify: `crates/agentdb-core/src/planner.rs`

- [ ] **Step 1: Write the failing test for visibility filter**

Append to `crates/agentdb-core/src/planner.rs`'s tests module:

```rust
    #[tokio::test]
    async fn planner_filters_out_candidates_beyond_snapshot_next_seq() {
        use crate::read_ctx::ReadCtx;
        let (_kd, _od, kv, store, reg, tenants, sessions) = setup_with_store().await;
        for i in 1..=5u64 {
            append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
                json!({"text": format!("alpha {i}")}), 0, i).await.unwrap();
        }
        let tenant = tenants.get_or_mint(reg.route_tenant("t-1").id, "t-1", 0).unwrap();
        let mut indexes = PerBoxIndexes::new();
        indexes.build(&kv, &store, &tenants, &sessions, tenant.box_id).await.unwrap();

        let box_ = reg.route_tenant("t-1");
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        // Post-snapshot append must NOT appear in planner results.
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"text": "alpha 6"}), 0, 6).await.unwrap();
        // Rebuild to simulate index seeing the newer event (planner visibility
        // is the guard, not the index).
        let mut indexes2 = PerBoxIndexes::new();
        indexes2.build(&kv, &store, &tenants, &sessions, tenant.box_id).await.unwrap();

        let p = HybridPlanner {
            ctx: &ctx,
            store: &store,
            indexes: &indexes2,
            sessions: &sessions,
            box_id: tenant.box_id,
            tenant_internal_id: tenant.internal_id,
        };
        let out = p.plan(
            &Source::Text { path: "payload.text".into(), query: "alpha".into(), limit: 10 },
            Box::new(|_, _| true),
            None,
        ).await.unwrap();
        // Original 5 events visible; the 6th is post-snapshot and filtered out.
        assert_eq!(out.hits.len(), 5, "visibility filter must drop post-snapshot candidates");
    }
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p agentdb-core --lib planner::tests::planner_filters_out_candidates_beyond_snapshot_next_seq`

Expected: FAIL — `HybridPlanner` has no `ctx` field.

- [ ] **Step 3: Update `HybridPlanner` struct and impl**

Replace the `HybridPlanner` struct and all its methods in `crates/agentdb-core/src/planner.rs`:

```rust
pub struct HybridPlanner<'a> {
    pub ctx: &'a crate::read_ctx::ReadCtx,
    pub store: &'a Arc<dyn ObjectStore>,
    pub indexes: &'a PerBoxIndexes,
    pub sessions: &'a SessionRegistry,
    pub box_id: BoxId,
    pub tenant_internal_id: u64,
}

impl<'a> HybridPlanner<'a> {
    pub async fn plan(
        &self,
        source: &Source,
        filter: FilterFn,
        session_id_hint: Option<&str>,
    ) -> Result<PlanOutput> {
        if let Some(session_ext) = session_id_hint {
            self.plan_locality_bound(source, filter, session_ext).await
        } else {
            self.plan_over_fetch(source, filter).await
        }
    }

    async fn plan_locality_bound(
        &self,
        source: &Source,
        filter: FilterFn,
        session_ext: &str,
    ) -> Result<PlanOutput> {
        let session = match self
            .ctx
            .session(self.sessions, self.tenant_internal_id, session_ext)?
        {
            Some(s) => s,
            None => {
                return Ok(PlanOutput {
                    strategy: Strategy::LocalityBound,
                    hits: Vec::new(),
                });
            }
        };

        let all: Vec<Event> = match session.tier {
            Tier::Warm => crate::primitives::scan_session_events(self.ctx, &session)?,
            Tier::Cold => {
                let Some(path) = session.segment_path.as_deref() else {
                    return Ok(PlanOutput {
                        strategy: Strategy::LocalityBound,
                        hits: Vec::new(),
                    });
                };
                let seg = crate::segment::read_segment(self.store, path).await?;
                seg.events.into_iter().filter(|e| e.seq < session.next_seq).collect()
            }
            Tier::Archive => Vec::new(),
        };

        let mut scored: Vec<Hit> = Vec::new();
        for event in all {
            if !filter(&session.external_id, &event) {
                continue;
            }
            let score = score_single(source, &event);
            if score > 0.0 {
                scored.push(Hit {
                    doc_id: DocId::new(session.internal_id, event.seq),
                    score,
                    event,
                    session_external_id: session.external_id.clone(),
                });
            }
        }
        scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(match source {
            Source::Text { limit, .. } => *limit,
            Source::Vector { limit, .. } => *limit,
        });
        Ok(PlanOutput { strategy: Strategy::LocalityBound, hits: scored })
    }

    async fn plan_over_fetch(&self, source: &Source, filter: FilterFn) -> Result<PlanOutput> {
        let candidates: Vec<(DocId, f32)> = match source {
            Source::Text { path, query, limit } => {
                use agentdb_index::text::TextIndex;
                self.indexes.text.search(path, query, limit * DEFAULT_OVER_FETCH)
            }
            Source::Vector { path, query, limit, num_candidates } => {
                use agentdb_index::vector::VectorIndex;
                self.indexes.vector.knn(path, query, (*num_candidates).max(*limit))
            }
        };
        let limit = match source {
            Source::Text { limit, .. } | Source::Vector { limit, .. } => *limit,
        };
        let mut hits: Vec<Hit> = Vec::new();
        for (doc_id, score) in candidates {
            // Snapshot-time session. Cache hit after the first lookup per session.
            let Some(session) = self.ctx.session_by_internal(
                self.sessions, self.tenant_internal_id, doc_id.session_internal_id,
            )? else {
                continue;
            };
            // M9 visibility filter: drop candidates past the snapshot's next_seq.
            if doc_id.seq >= session.next_seq {
                continue;
            }
            let Some(event) = self.resolve(doc_id, &session).await? else {
                continue;
            };
            if !filter(&session.external_id, &event) {
                continue;
            }
            hits.push(Hit { doc_id, score, event, session_external_id: session.external_id });
            if hits.len() >= limit {
                break;
            }
        }
        Ok(PlanOutput { strategy: Strategy::OverFetchPostFilter, hits })
    }

    async fn resolve(&self, doc_id: DocId, session: &crate::types::Session) -> Result<Option<Event>> {
        let key = agentdb_storage::encode_key(
            Namespace::Events,
            self.box_id,
            self.tenant_internal_id,
            doc_id.session_internal_id,
            crate::primitives::DEFAULT_BRANCH_ID,
            doc_id.seq,
        );
        if let Some(bytes) = self.ctx.get(&key)? {
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
        let Some(path) = session.segment_path.as_deref() else {
            return Ok(None);
        };
        let seg = match crate::segment::read_segment(self.store, path).await {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        Ok(seg.events.into_iter().find(|e| e.seq == doc_id.seq && e.seq < session.next_seq))
    }
}
```

Note that `resolve` now takes `&Session` so it avoids a duplicate lookup; the caller has already resolved the session via `ctx.session_by_internal`.

Also remove the `kv: &'a Arc<dyn KvStore>` field from the struct initialization in every test in this file — replace with `ctx: &ctx` (tests will need to open a ReadCtx first).

- [ ] **Step 4: Update existing planner tests**

Every planner test in `planner.rs` currently constructs:

```rust
let p = HybridPlanner {
    kv: &kv,
    store: &store,
    indexes: &indexes,
    sessions: &sessions,
    box_id: tenant.box_id,
    tenant_internal_id: tenant.internal_id,
};
```

Replace each with:

```rust
let box_ = reg.route_tenant("t-1");
let ctx = ReadCtx::open(&box_, &kv).unwrap();
let p = HybridPlanner {
    ctx: &ctx,
    store: &store,
    indexes: &indexes,
    sessions: &sessions,
    box_id: tenant.box_id,
    tenant_internal_id: tenant.internal_id,
};
```

Add `use crate::read_ctx::ReadCtx;` to the test module.

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-core --lib planner`

Expected: all planner tests PASS, including the new visibility test.

- [ ] **Step 6: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/planner.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(core): HybridPlanner takes &ReadCtx, post-filters by snapshot next_seq"
```

---

## Phase C — Tiering interaction + Segment GC

### Task 10: Drop inline `delete_segment` in `reactivate`

**Why:** Reactivate's inline delete races with any open ReadCtx that captured the Cold session. Defer deletion to the GC sweep.

**Files:**
- Modify: `crates/agentdb-core/src/tiering.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/agentdb-core/src/tiering.rs`'s tests module:

```rust
    #[tokio::test]
    async fn reactivate_leaves_segment_on_disk_for_gc() {
        let (ctx, mut session) = seed("round trip", "s-2").await;
        let box_ = ctx.registry.route_tenant("t-1");
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(ctx.registry.kv(), ctx.registry.object_store(),
                &ctx.sessions, &guard, &mut session).await.unwrap();
        }
        let path = session.segment_path.clone().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            reactivate(ctx.registry.kv(), ctx.registry.object_store(),
                &ctx.sessions, &guard, &mut session).await.unwrap();
        }
        // After M9 change: segment is still on disk; GC will reap it later.
        assert!(
            read_segment(ctx.registry.object_store(), &path).await.is_ok(),
            "segment should still exist after reactivate"
        );
    }
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p agentdb-core --lib tiering::tests::reactivate_leaves_segment_on_disk_for_gc`

Expected: FAIL — current `reactivate` deletes the segment inline.

- [ ] **Step 3: Modify `reactivate`**

In `crates/agentdb-core/src/tiering.rs`, locate the `delete_segment` call at the end of `reactivate` and remove it. The function should end:

```rust
    kv.write_batch(ops)?;
    let _ = sessions; // signature retained for future archive transitions
    // M9: segment deletion deferred to SegmentGc. reactivate must not
    // delete inline because any open ReadCtx that captured this session
    // as Cold still needs to read from the segment path it saw.
    Ok(())
}
```

Also remove the import of `delete_segment` at the top of the file if it's no longer used (`use crate::segment::{... delete_segment ...}`).

- [ ] **Step 4: Update the existing `reactivate_restores_warm_events_and_deletes_segment` test**

This test currently asserts:

```rust
assert!(read_segment(ctx.registry.object_store(), &path_before).await.is_err());
```

Change the assertion to match the new behavior: the segment MUST still exist immediately after reactivate.

```rust
// After M9, reactivate no longer deletes the segment; GC handles it.
assert!(read_segment(ctx.registry.object_store(), &path_before).await.is_ok());
```

Rename the test to `reactivate_restores_warm_events_and_keeps_segment_for_gc` to reflect the new contract.

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-core --lib tiering`

Expected: all tiering tests PASS, including the new and the renamed.

- [ ] **Step 6: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/tiering.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "refactor(tiering): reactivate defers segment delete to SegmentGc"
```

---

### Task 11: `SegmentGc` background task

**Why:** Periodically reclaim orphan segments — those not referenced by any current session record, not pinned by any open snapshot, and older than the grace window.

**Files:**
- Create: `crates/agentdb-core/src/segment_gc.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/agentdb-core/src/segment_gc.rs`:

```rust
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use agentdb_storage::BoxId;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use tokio::task::JoinHandle;

use crate::box_runtime::Box_;
use crate::error::Result;
use crate::registry::{SessionRegistry, TenantRegistry};

#[derive(Debug, Clone, Copy)]
pub struct SegmentGcConfig {
    pub tick: Duration,
    pub grace: Duration,
}

impl Default for SegmentGcConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(5),
            grace: Duration::from_secs(30),
        }
    }
}

pub struct SegmentGc {
    _handle: JoinHandle<()>,
}

/// Called after each sweep that reclaimed at least one segment, with
/// `(box_id, reclaimed, skipped_pinned)` deltas for that sweep.
/// Wired from the server binary to the SSE stream bus; core stays
/// independent of the API crate.
pub type GcRunReporter = Arc<dyn Fn(u16, u64, u64) + Send + Sync>;

impl SegmentGc {
    pub fn start(
        store: Arc<dyn ObjectStore>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
        box_: Arc<Box_>,
        config: SegmentGcConfig,
        reporter: Option<GcRunReporter>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let before_reclaimed = box_.metrics.snapshot().segment_gc_reclaimed;
                let before_skipped = box_.metrics.snapshot().segment_gc_skipped_pinned;
                if let Err(err) = sweep(&store, &tenants, &sessions, &box_, config).await {
                    tracing::warn!(box_id = box_.id, ?err, "segment_gc sweep errored");
                }
                let after = box_.metrics.snapshot();
                let reclaimed = after.segment_gc_reclaimed.saturating_sub(before_reclaimed);
                let skipped = after.segment_gc_skipped_pinned.saturating_sub(before_skipped);
                if reclaimed > 0 {
                    if let Some(rep) = reporter.as_ref() {
                        rep(box_.id, reclaimed, skipped);
                    }
                }
            }
        });
        Self { _handle: handle }
    }
}

pub(crate) async fn sweep(
    store: &Arc<dyn ObjectStore>,
    tenants: &Arc<TenantRegistry>,
    sessions: &Arc<SessionRegistry>,
    box_: &Arc<Box_>,
    config: SegmentGcConfig,
) -> Result<()> {
    // (1) Live references: scan current session records in this box.
    let mut referenced: HashSet<String> = HashSet::new();
    for tenant_ext in tenants.list_in_box(box_.id)? {
        let Some(tenant) = tenants.lookup(box_.id, &tenant_ext)? else {
            continue;
        };
        for session_ext in sessions.list_in_tenant(box_.id, tenant.internal_id)? {
            let Some(s) = sessions.lookup(box_.id, tenant.internal_id, &session_ext)? else {
                continue;
            };
            if let Some(p) = s.segment_path {
                referenced.insert(p);
            }
        }
    }

    // (2) Pinned by open snapshots.
    for p in box_.segment_pins.pinned_paths() {
        referenced.insert(p);
    }

    // (3) List + reap.
    let prefix = format!("objects/segments/{}/", box_.id);
    let obj_prefix = ObjPath::from(prefix.as_str());
    use futures_util::StreamExt;
    let mut stream = store.list(Some(&obj_prefix));
    let grace_ms = config.grace.as_millis() as i64;
    let now_ms = now_millis() as i64;
    while let Some(obj) = stream.next().await {
        let obj = match obj {
            Ok(o) => o,
            Err(err) => {
                tracing::warn!(?err, "segment_gc list errored; aborting sweep");
                return Ok(());
            }
        };
        let path = obj.location.to_string();
        if referenced.contains(&path) {
            // Could be live-referenced or pinned. Record the pin case separately.
            if box_.segment_pins.pinned_paths().contains(&path) {
                box_.metrics.record_segment_gc_skipped_pinned();
            }
            continue;
        }
        let age_ms = now_ms.saturating_sub(obj.last_modified.timestamp_millis());
        if age_ms < grace_ms {
            continue;
        }
        match store.delete(&obj.location).await {
            Ok(()) => {
                box_.metrics.record_segment_gc_reclaimed();
            }
            Err(err) => tracing::warn!(?err, %path, "segment_gc delete failed"),
        }
    }
    Ok(())
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use crate::read_ctx::ReadCtx;
    use crate::tiering::cold_transition;
    use crate::BoxRegistry;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use tempfile::TempDir;

    async fn setup() -> (
        TempDir, TempDir, Arc<BoxRegistry>, Arc<TenantRegistry>, Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (kv_dir, obj_dir, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn sweep_skips_referenced_segment() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"x": 1}), 0, 10).await.unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await.unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_millis(0),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config).await.unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path).await.is_ok(),
            "live-referenced segment must not be reaped"
        );
        let m = box_.metrics.snapshot();
        assert_eq!(m.segment_gc_reclaimed, 0);
    }

    #[tokio::test]
    async fn sweep_skips_snapshot_pinned_segment() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"x": 1}), 0, 10).await.unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await.unwrap();
        }
        // Open a ReadCtx that pins the segment.
        let ctx = ReadCtx::open(&box_, reg.kv()).unwrap();
        let _s = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap();
        assert_eq!(box_.segment_pins.pinned_len(), 1);

        // Reactivate so the session's current record no longer references the
        // segment — it's only referenced by the pin.
        {
            let guard = box_.write_lock.lock().await;
            let mut fresh = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
            crate::tiering::reactivate(reg.kv(), reg.object_store(), &sessions, &guard, &mut fresh)
                .await.unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_millis(0),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config).await.unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path).await.is_ok(),
            "pinned segment must not be reaped"
        );
        let m = box_.metrics.snapshot();
        assert_eq!(m.segment_gc_skipped_pinned, 1);
        drop(ctx);
    }

    #[tokio::test]
    async fn sweep_reclaims_orphan_past_grace() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"x": 1}), 0, 10).await.unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await.unwrap();
        }
        // Reactivate, nobody pins the segment: orphan.
        {
            let guard = box_.write_lock.lock().await;
            let mut fresh = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
            crate::tiering::reactivate(reg.kv(), reg.object_store(), &sessions, &guard, &mut fresh)
                .await.unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        // Grace 0 — the orphan is eligible immediately.
        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_millis(0),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config).await.unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path).await.is_err(),
            "orphan segment past grace must be reaped"
        );
        let m = box_.metrics.snapshot();
        assert_eq!(m.segment_gc_reclaimed, 1);
    }

    #[tokio::test]
    async fn sweep_respects_grace_window() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"x": 1}), 0, 10).await.unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await.unwrap();
        }
        {
            let guard = box_.write_lock.lock().await;
            let mut fresh = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
            crate::tiering::reactivate(reg.kv(), reg.object_store(), &sessions, &guard, &mut fresh)
                .await.unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        // Grace 60s — the orphan is NOT yet eligible.
        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_secs(60),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config).await.unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path).await.is_ok(),
            "segment younger than grace must not be reaped"
        );
        assert_eq!(box_.metrics.snapshot().segment_gc_reclaimed, 0);
    }
}
```

- [ ] **Step 2: Add module to lib.rs**

Edit `crates/agentdb-core/src/lib.rs`:

```rust
pub mod segment_gc;
// ...
pub use segment_gc::{SegmentGc, SegmentGcConfig};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-core --lib segment_gc`

Expected: 4 tests PASS.

- [ ] **Step 4: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/src/segment_gc.rs crates/agentdb-core/src/lib.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(core): SegmentGc background sweep reclaims orphan segments"
```

---

## Phase D — Server wiring + API/wire integration

### Task 12: Start `SegmentGc` in the server binary

**Why:** Spin up one SegmentGc task per box at process startup, alongside the existing LifecycleEngine.

**Files:**
- Modify: `bin/agentdb-server/src/main.rs`

- [ ] **Step 1: Add SegmentGc startup loop**

Edit `bin/agentdb-server/src/main.rs`. Find the loop that starts `LifecycleEngine::start` (around line 72). Add a parallel loop that starts `SegmentGc`. The `SegmentGc` configuration reads from environment variables:

```rust
use agentdb_core::{SegmentGc, SegmentGcConfig};
// ...

    let segment_gc_cfg = SegmentGcConfig {
        tick: Duration::from_secs(
            std::env::var("AGENTDB_SEGMENT_GC_TICK_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
        ),
        grace: Duration::from_secs(
            std::env::var("AGENTDB_SEGMENT_GC_GRACE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
        ),
    };

    let mut gcs: Vec<SegmentGc> = Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        gcs.push(SegmentGc::start(
            store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_,
            segment_gc_cfg,
            None,  // reporter hooked up in Task 15
        ));
    }
    std::mem::forget(gcs);
```

Place this block immediately after the existing `LifecycleEngine` startup loop (after `std::mem::forget(engines);`).

- [ ] **Step 2: Compile**

Run: `cargo build --bin agentdb-server`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add bin/agentdb-server/src/main.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(server): start SegmentGc per box at boot"
```

---

### Task 13: Thread ReadCtx through `agentdb-wire` `find` / `aggregate`

**Why:** Mongo read commands must see a consistent snapshot across all session scans in one request.

**Files:**
- Modify: `crates/agentdb-wire/src/handlers/find.rs`
- Modify: `crates/agentdb-wire/src/handlers/aggregate.rs`

- [ ] **Step 1: Update `find_response` signature**

Edit `crates/agentdb-wire/src/handlers/find.rs`. Change `find_response` to open a `ReadCtx` at the top and use it throughout:

```rust
use agentdb_core::{scan_session_events, BoxRegistry, Event, ReadCtx, SessionRegistry, TenantRegistry};

pub async fn find_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    filter_doc: &Document,
    limit: Option<i64>,
) -> WireResult<Document> {
    let box_ = registry.route_tenant(db);
    box_.metrics.record_query_find();
    let filter = Filter::parse(filter_doc)?;
    let tenant = match tenants.lookup(box_.id, db)? {
        Some(t) => t,
        None => return Ok(empty_batch(db, collection)),
    };
    let ctx = ReadCtx::open(&box_, registry.kv())?;
    let mut batch: Vec<Value> = Vec::new();
    let cap = limit.unwrap_or(i64::MAX).max(0) as usize;

    match collection {
        "events" => {
            let session_externals = sessions.list_in_tenant(tenant.box_id, tenant.internal_id)?;
            let scoped: Vec<String> = match filter.top_level_eq("session_id") {
                Some(Value::String(s)) => vec![s.clone()],
                _ => session_externals,
            };
            for s_ext in scoped {
                let Some(session) = ctx.session(sessions, tenant.internal_id, &s_ext)? else {
                    continue;
                };
                for ev in scan_session_events(&ctx, &session)? {
                    let row = project_event(&session.external_id, ev);
                    if filter.matches(&row) {
                        batch.push(row);
                        if batch.len() >= cap { break; }
                    }
                }
                if batch.len() >= cap { break; }
            }
        }
        "sessions" => {
            let session_externals = sessions.list_in_tenant(tenant.box_id, tenant.internal_id)?;
            for s_ext in session_externals {
                let Some(session) = ctx.session(sessions, tenant.internal_id, &s_ext)? else {
                    continue;
                };
                let row = serde_json::to_value(&session)?;
                if filter.matches(&row) {
                    batch.push(row);
                    if batch.len() >= cap { break; }
                }
            }
        }
        other => {
            return Err(WireError::Protocol(format!(
                "unknown collection {other}; only 'events' and 'sessions' exist"
            )));
        }
    }

    let first_batch: Vec<Bson> = batch.into_iter().map(json_to_bson).collect();
    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(first_batch),
        },
        "ok": 1.0,
    })
}

fn routing_box(registry: &BoxRegistry, tenant_external: &str) -> u16 {
    registry.route_tenant(tenant_external).id
}
```

Note: the function is now `async` because `ReadCtx::open` returns a Result; wait — `open` is sync. But the caller may chain `find_response` with async ops. For symmetry with `fetch_last_n_merged`, keep `find_response` sync here (it doesn't await anything). If the signature change to `async fn` breaks callers, propagate accordingly.

- [ ] **Step 2: Update every existing `find_response` test**

Every test in `find.rs` currently calls `find_response(...).unwrap()` synchronously. If the signature stayed sync, tests don't change beyond any session lookup pattern using `ctx.session` — but those are inside `find_response` already.

Run existing tests to check:

Run: `cargo test -p agentdb-wire --lib handlers::find::tests`

Expected: PASS unchanged.

- [ ] **Step 3: Update `aggregate.rs`**

Read `crates/agentdb-wire/src/handlers/aggregate.rs` and apply the same ReadCtx pattern: open once at function top, route all reads (session lookups and event scans) through it.

- [ ] **Step 4: Compile + run all wire tests**

Run: `cargo test -p agentdb-wire --lib`

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-wire/
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(wire): find/aggregate open ReadCtx for consistent reads"
```

---

### Task 14: Thread ReadCtx through `agentdb-api` handlers

**Why:** REST read handlers must also go through ReadCtx. The main mutating handler `append_and_fetch` already gets ReadCtx semantics via `agentdb_core::append_and_fetch`.

**Files:**
- Modify: `crates/agentdb-api/src/*.rs` as needed

- [ ] **Step 1: Audit read paths**

Run: `cargo build -p agentdb-api`

Expected: PASS (or reveal broken signatures from Tasks 6–9 that need propagation).

Scan the admin handlers for direct use of `scan_session_events` / `fetch_last_n_merged` / `HybridPlanner`:

Run: `grep -rn "scan_session_events\|fetch_last_n_merged\|HybridPlanner" crates/agentdb-api/src/`

For each match, wrap the call site in a `ReadCtx::open(&box_, registry.kv())?` and pass the ctx.

Typical admin handler pattern (pseudo-code for each match site):

```rust
let box_ = state.registry.route_tenant(&tenant);
let ctx = ReadCtx::open(&box_, state.registry.kv())?;
// ... existing logic but using &ctx ...
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-api --lib`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-api/
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(api): admin read handlers open ReadCtx"
```

---

### Task 15: `StreamBus` broadcasts `SegmentGcRun`

**Why:** Dashboard needs visibility into GC activity. `SegmentGc::start` already accepts a reporter closure (added in Task 11); Task 15 just wires the server side.

**Files:**
- Modify: `crates/agentdb-api/src/stream.rs` (add variant)
- Modify: `bin/agentdb-server/src/main.rs` (pass `Some(reporter)` instead of `None`)

- [ ] **Step 1: Add the enum variant**

Edit `crates/agentdb-api/src/stream.rs`. Add a new variant to `StreamEvent`:

```rust
    SegmentGcRun {
        ts_ms: u64,
        box_id: u16,
        reclaimed: u64,
        skipped_pinned: u64,
    },
```

- [ ] **Step 2: Wire the reporter in `main.rs`**

Edit `bin/agentdb-server/src/main.rs`. Construct a closure that captures the `StreamBus` and emits `SegmentGcRun`, then pass `Some(reporter.clone())` to `SegmentGc::start` where Task 12 passed `None`:

```rust
    let reporter: agentdb_core::GcRunReporter = {
        let stream = stream.clone();
        std::sync::Arc::new(move |box_id, reclaimed, skipped_pinned| {
            stream.emit(StreamEvent::SegmentGcRun {
                ts_ms: now_ms(),
                box_id,
                reclaimed,
                skipped_pinned,
            });
        })
    };
```

Construct `reporter` just before the existing `SegmentGc::start` loop from Task 12. In that loop, replace the final argument `None` with `Some(reporter.clone())`.

- [ ] **Step 3: Compile**

Run: `cargo build --workspace`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-api/src/stream.rs bin/agentdb-server/src/main.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(stream): SegmentGcRun SSE event wired from GC to bus"
```

---

### Task 16: Dashboard shows `open_snapshots` and logs `SegmentGcRun`

**Why:** Make the MVCC machinery visible on the dashboard.

**Files:**
- Modify: `crates/agentdb-dashboard/static/app.js`
- Modify: `crates/agentdb-dashboard/static/index.html`
- Modify: `crates/agentdb-api/src/stream.rs::TickBox` (add `open_snapshots`)
- Modify: `bin/agentdb-server/src/main.rs` MetricsTick population

- [ ] **Step 1: Extend `TickBox`**

Edit `crates/agentdb-api/src/stream.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickBox {
    pub id: u16,
    pub warm_sessions: u64,
    pub cold_sessions: u64,
    pub appends: u64,
    pub forks: u64,
    pub fault_active: bool,
    pub open_snapshots: u64,
    pub segments_pinned_by_snapshots: u64,
}
```

- [ ] **Step 2: Populate new fields in the metrics tick loop**

Edit `bin/agentdb-server/src/main.rs`. Locate the `TickBox { id: i, warm_sessions: ... }` construction in the metrics tick loop and add the two new fields:

```rust
                    boxes.push(TickBox {
                        id: i,
                        warm_sessions: snap.gauges.warm_sessions,
                        cold_sessions: snap.gauges.cold_sessions,
                        appends: snap.appends,
                        forks: snap.forks,
                        fault_active,
                        open_snapshots: snap.open_snapshots,
                        segments_pinned_by_snapshots: snap.segments_pinned_by_snapshots,
                    });
```

- [ ] **Step 3: Render in `app.js`**

Read the existing `app.js` render function for per-box tiles. Add a small "snapshots" row showing `open_snapshots` and, if > 0, `segments_pinned_by_snapshots` in parens:

```js
  // Inside the per-box tile render, after the appends/forks rows:
  const snaps = box.open_snapshots || 0;
  const pins = box.segments_pinned_by_snapshots || 0;
  const snapsLine = pins > 0
      ? `snapshots: ${snaps} (${pins} seg pins)`
      : `snapshots: ${snaps}`;
```

Append `snapsLine` to the tile body. The current pattern will be visible in `app.js` — follow it.

- [ ] **Step 4: Handle `SegmentGcRun` in the event feed**

In `app.js`'s SSE event handler, add a case for the new event type:

```js
  case 'SegmentGcRun':
    appendFeed(`[gc] box ${event.box_id}: reclaimed ${event.reclaimed}, skipped ${event.skipped_pinned} pinned`);
    break;
```

Match the existing `TierChange` / `ForkCreated` feed-line pattern.

- [ ] **Step 5: Manual smoke check**

```bash
cd /Users/lishen/work/agentdb/.worktrees/m9-mvcc
AGENTDB_SEGMENT_GC_TICK_SECS=2 AGENTDB_SEGMENT_GC_GRACE_SECS=2 \
  AGENTDB_T_COLD_SECS=2 cargo run --bin agentdb-server &
SERVER_PID=$!
sleep 3
curl -s http://127.0.0.1:7878/api/admin/boxes | head -c 500
echo
kill $SERVER_PID
```

Expected: JSON shows `open_snapshots`, `segments_pinned_by_snapshots`, `segment_gc_reclaimed`, `segment_gc_skipped_pinned` in the per-box snapshot.

- [ ] **Step 6: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-dashboard/ crates/agentdb-api/src/stream.rs bin/agentdb-server/src/main.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "feat(dashboard): show snapshots + log SegmentGcRun events"
```

---

## Phase E — Integration tests, docs, performance

### Task 17: Integration tests for tier-transition isolation

**Why:** Cover the headline MVCC guarantee end-to-end.

**Files:**
- Create: `crates/agentdb-core/tests/mvcc_isolation.rs`

- [ ] **Step 1: Write integration tests**

Create `crates/agentdb-core/tests/mvcc_isolation.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use agentdb_core::{
    append_and_fetch, cold_transition, fetch_last_n_merged, reactivate, scan_session_events,
    BoxRegistry, ReadCtx, SessionRegistry, TenantRegistry,
};
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

async fn setup() -> (
    TempDir, TempDir, Arc<dyn KvStore>, Arc<BoxRegistry>, Arc<TenantRegistry>, Arc<SessionRegistry>,
) {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));
    (kv_dir, obj_dir, kv, reg, tenants, sessions)
}

#[tokio::test]
async fn snapshot_isolates_cold_transition_midread() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    for i in 1..=3u64 {
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
            json!({"i": i}), 0, i).await.unwrap();
    }
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

    let ctx = ReadCtx::open(&box_, &kv).unwrap();
    let at_open = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap().unwrap();

    // Mid-read: tier transition to Cold.
    {
        let guard = box_.write_lock.lock().await;
        let mut fresh = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        cold_transition(&kv, reg.object_store(), &sessions, &guard, &mut fresh)
            .await.unwrap();
    }

    // Warm events already captured in the snapshot.
    let events = scan_session_events(&ctx, &at_open).unwrap();
    assert_eq!(events.len(), 3, "snapshot must see the pre-transition warm events");
}

#[tokio::test]
async fn snapshot_isolates_reactivate_midread() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
        json!({"x": 1}), 0, 1).await.unwrap();
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

    // Pre-cold.
    {
        let guard = box_.write_lock.lock().await;
        let mut fresh = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        cold_transition(&kv, reg.object_store(), &sessions, &guard, &mut fresh)
            .await.unwrap();
    }

    // Open ReadCtx — sees Cold.
    let ctx = ReadCtx::open(&box_, &kv).unwrap();
    let cold_view = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap().unwrap();
    assert_eq!(cold_view.tier, agentdb_core::Tier::Cold);
    let seg_path = cold_view.segment_path.clone().unwrap();

    // Mid-read: reactivate.
    {
        let guard = box_.write_lock.lock().await;
        let mut fresh = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();
        reactivate(&kv, reg.object_store(), &sessions, &guard, &mut fresh)
            .await.unwrap();
    }

    // Segment still readable through the pin.
    assert!(agentdb_core::read_segment(reg.object_store(), &seg_path).await.is_ok());
    drop(ctx);
    assert!(agentdb_core::read_segment(reg.object_store(), &seg_path).await.is_ok(),
        "segment still on disk; GC hasn't run");
}

#[tokio::test]
async fn fork_and_snapshot_compose() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    // Parent: 5 events.
    for i in 1..=5u64 {
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "parent",
            json!({"i": i}), 0, i * 10).await.unwrap();
    }
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
    let parent = sessions.lookup(box_.id, tenant.internal_id, "parent").unwrap().unwrap();
    // Fork. Child sees parent with seq < parent.next_seq.
    let fork_ref = agentdb_core::ForkRef {
        parent_session_internal_id: parent.internal_id,
        parent_seq_at_fork: parent.next_seq,
    };
    sessions.get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
        .unwrap();
    append_and_fetch(&reg, &tenants, &sessions, "t-1", "child",
        json!({"c": 1}), 0, 100).await.unwrap();

    let ctx = ReadCtx::open(&box_, &kv).unwrap();

    // Post-snapshot: writes invisible to `ctx`.
    append_and_fetch(&reg, &tenants, &sessions, "t-1", "child",
        json!({"c": 2}), 0, 110).await.unwrap();
    append_and_fetch(&reg, &tenants, &sessions, "t-1", "parent",
        json!({"i": 999}), 0, 120).await.unwrap();

    let child = ctx.session(&sessions, tenant.internal_id, "child").unwrap().unwrap();
    let got = fetch_last_n_merged(&ctx, reg.object_store(), &sessions, &child, 100)
        .await.unwrap();
    // Expected: 5 parent events (ts 10..50) + 1 child event (ts 100) = 6.
    // Nothing with ts 110 or 120.
    assert_eq!(got.len(), 6);
    assert_eq!(got.iter().filter(|e| e.ts == 120).count(), 0);
    assert_eq!(got.iter().filter(|e| e.ts == 110).count(), 0);
}

#[tokio::test]
async fn concurrent_writes_do_not_affect_snapshot_scan() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
        json!({"i": 0}), 0, 1).await.unwrap();
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

    let ctx = ReadCtx::open(&box_, &kv).unwrap();
    let at_open = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap().unwrap();

    // Spawn a writer that adds 50 events concurrently.
    let writer = {
        let reg = reg.clone();
        let tenants = tenants.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            for i in 1..=50u64 {
                append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
                    json!({"i": i}), 0, 1000 + i).await.unwrap();
            }
        })
    };

    // While writer runs, scan the snapshot — result should be just the single event.
    for _ in 0..20 {
        let events = scan_session_events(&ctx, &at_open).unwrap();
        assert_eq!(events.len(), 1, "snapshot must stay frozen at 1 event");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    writer.await.unwrap();
}
```

- [ ] **Step 2: Run integration tests**

Run: `cargo test -p agentdb-core --test mvcc_isolation`

Expected: 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/tests/mvcc_isolation.rs
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "test(core): MVCC isolation integration tests"
```

---

### Task 18: Update `docs/ARCHITECTURE.md`

**Why:** Remove the "No MVCC" bullet now that M9 is done; add a short section describing ReadCtx + SegmentGc.

**Files:**
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Remove the MVCC line from "What the prototype does NOT do"**

Open `docs/ARCHITECTURE.md`. Locate the bullet:

```markdown
- **No MVCC or snapshot read API.** Fork is implemented with a parent
  pointer trick instead; other queries are read-committed.
```

Delete it (and its follow-on lines if any).

- [ ] **Step 2: Add an MVCC section**

Insert a new section titled "MVCC / snapshot reads" after the "Hybrid retrieval" section (or wherever fits the document's flow):

```markdown
## MVCC / snapshot reads

Read primitives in the prototype are snapshot-consistent. A
`ReadCtx` (in `crates/agentdb-core/src/read_ctx.rs`) pins a RocksDB
snapshot + a set of segment paths referenced by any Cold sessions
read through it. Handlers in `agentdb-api` and `agentdb-wire` open one
`ReadCtx` per request; the HybridPlanner, `fetch_last_n_merged`, and
`scan_session_events` all read through it.

Writers are unchanged — they still serialize through each box's
`write_lock`. MVCC in this prototype only provides **read-side**
isolation; there is no multi-version write tracking or conflict
detection.

Segment files on the object store are not deleted inline by
`reactivate`. A per-box `SegmentGc` task sweeps every 5s and deletes
segments that (a) are not referenced by any current session record
and (b) are not pinned by any open `ReadCtx` and (c) are older than a
30s grace window. Configure via `AGENTDB_SEGMENT_GC_TICK_SECS` /
`AGENTDB_SEGMENT_GC_GRACE_SECS`.

Still out of scope: client-held snapshot handles (`openSnapshot` over
the wire), cross-box transactional snapshots, write-side MVCC, and
versioned indexes (the text/vector index is unversioned and the
planner post-filters hits by snapshot-visible `next_seq`).
```

- [ ] **Step 3: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add docs/ARCHITECTURE.md
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "docs(architecture): MVCC section, drop no-MVCC bullet"
```

---

### Task 19: Measure and document snapshot read overhead

**Why:** The spec calls for a `docs/PERFORMANCE.md` section with before/after numbers.

**Files:**
- Create: `crates/agentdb-core/benches/mvcc_bench.rs`
- Modify: `crates/agentdb-core/Cargo.toml` (add bench entry)
- Modify: `docs/PERFORMANCE.md`

- [ ] **Step 1: Add criterion dev-dep**

Edit `crates/agentdb-core/Cargo.toml`. Under `[dev-dependencies]` add (if not already present):

```toml
criterion = { workspace = true }
```

Add at the bottom of the file:

```toml
[[bench]]
name = "mvcc_bench"
harness = false
```

Check `Cargo.toml` at the workspace root for a `criterion` workspace dep. If absent, add `criterion = "0.5"` under `[workspace.dependencies]`.

- [ ] **Step 2: Write the bench**

Create `crates/agentdb-core/benches/mvcc_bench.rs`:

```rust
use std::sync::Arc;

use agentdb_core::{
    append_and_fetch, scan_session_events, BoxRegistry, ReadCtx, SessionRegistry, TenantRegistry,
};
use agentdb_storage::{KvStore, RocksKvStore};
use criterion::{criterion_group, criterion_main, Criterion};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn bench_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (_k, _o, _kv, reg, tenants, sessions) = rt.block_on(async {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        for i in 1..=1000u64 {
            append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1",
                json!({"i": i}), 0, i).await.unwrap();
        }
        (kv_dir, obj_dir, kv, reg, tenants, sessions)
    });
    let box_ = reg.route_tenant("t-1");
    let tenant = rt.block_on(async { tenants.get_or_mint(box_.id, "t-1", 0).unwrap() });
    let session = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap().unwrap();

    c.bench_function("scan_1000_events_via_ctx", |b| {
        b.iter(|| {
            let ctx = ReadCtx::open(&box_, reg.kv()).unwrap();
            let v = scan_session_events(&ctx, &session).unwrap();
            assert_eq!(v.len(), 1000);
        });
    });
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
```

- [ ] **Step 3: Run the bench**

Run: `cargo bench -p agentdb-core --bench mvcc_bench 2>&1 | tee /tmp/mvcc_bench.txt`

Expected: PASS. Record the per-iteration time reported by criterion.

- [ ] **Step 4: Update `docs/PERFORMANCE.md`**

Append a new section to `docs/PERFORMANCE.md`:

```markdown
## Snapshot read overhead (M9)

All read primitives now go through a `ReadCtx` that wraps a RocksDB
snapshot. Cost per request is one `DB::snapshot()` call + a few
atomic increments on `BoxMetrics`.

Measured on the criterion bench `crates/agentdb-core/benches/mvcc_bench.rs`
against a session with 1000 events:

| operation                         | time       |
|-----------------------------------|------------|
| `ReadCtx::open` + scan 1000 events | <PASTE FROM /tmp/mvcc_bench.txt> |

Interpretation: the snapshot-open cost is dominated by the `scan_prefix`
itself; the snapshot setup is a single-digit microseconds overhead.
At prototype scale this is well within the "agent workload tail-latency
tolerance" budget described in the design doc §1.1.
```

Replace `<PASTE FROM /tmp/mvcc_bench.txt>` with the actual timing from Step 3.

- [ ] **Step 5: Commit**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add crates/agentdb-core/benches/ crates/agentdb-core/Cargo.toml docs/PERFORMANCE.md
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "docs(performance): snapshot read overhead bench"
```

---

### Task 20: Final verification — whole workspace clippy + test

**Why:** Verify no regressions across the workspace.

- [ ] **Step 1: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: clean. Fix any lint the changes introduced (most often: unused imports left over from signature changes).

- [ ] **Step 2: Full test suite**

Run: `cargo test --workspace`

Expected: all green.

- [ ] **Step 3: Smoke test the server end-to-end**

Run the repo's existing smoke script:

```bash
bash scripts/smoke.sh
```

Expected: all checks pass; server starts, accepts appends, returns consistent fetches, dashboard renders, tier transitions happen.

- [ ] **Step 4: Commit any lint fixes**

```bash
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc status
# If anything was fixed:
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc add -u
git -C /Users/lishen/work/agentdb/.worktrees/m9-mvcc commit -m "chore(m9): clippy fixes post-MVCC"
```

---

## Self-review checklist

- [x] Spec coverage: every spec section maps to a task.
  - `ReadCtx` → Task 5
  - `KvSnapshot` trait (owned variant) → Task 1
  - `SegmentPinRegistry` → Task 2
  - `scan_session_events` / `fetch_last_n_merged` → Tasks 6–8
  - `HybridPlanner` visibility filter → Task 9
  - `reactivate` inline delete removed → Task 10
  - `SegmentGc` → Task 11, wired in Task 12, reporter in Task 15
  - REST/wire threading → Tasks 13–14
  - Metrics + SSE → Tasks 3, 15, 16
  - ARCHITECTURE.md + PERFORMANCE.md → Tasks 18–19
  - Integration tests (tier transition, reactivate, fork, concurrent) → Task 17

- [x] No placeholders like "TBD" / "add error handling" / "similar to Task N" remain.

- [x] Type consistency: `ReadCtx::open(&Box_, &Arc<dyn KvStore>)` is used identically in Tasks 5, 6, 7, 9, 11, 13, 14, 17. `SegmentGc::start(store, tenants, sessions, box_, config, reporter)` is the shape used in Tasks 11, 12, 15. `HybridPlanner` fields are `ctx, store, indexes, sessions, box_id, tenant_internal_id` in Tasks 9 and all test updates.
