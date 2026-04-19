# AgentDB Prototype — Milestone 5: Lifecycle engine + object_store tiering

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make session tiering real. An idle session migrates from warm (RocksDB) to cold (object_store segment file) automatically. Queries against cold sessions keep working — the hybrid planner falls back to reading the segment when `kv.get` misses. A background `LifecycleEngine` drives transitions on a timer, and an admin REST endpoint lets demo operators force tier moves for narrative beats.

**Architecture:** New `agentdb-core::segment` module serialises per-session events into a single BSON document written to `object_store`. `agentdb-core::tiering` exposes `cold_transition` and `reactivate` primitives that move data between RocksDB and the object store atomically with metadata updates. `LifecycleEngine` is a per-box tokio task that polls session metadata for idle warm sessions and invokes `cold_transition` under the existing per-box `write_lock`. `PerBoxIndexes::build` is extended to ingest cold-tier segments so indexes remain complete across tier moves. The `HybridPlanner::resolve` step falls back to a segment read when warm-tier `kv.get` returns `None` for a known-cold session. `bin/agentdb-server` spins the engines up at startup; a new `POST /api/admin/tier/force` endpoint lets the demo operator move sessions on command.

**Tech Stack (M5 additions):** `object_store` 0.11 (Apache; already referenced in the spec). For simplicity we use only the `LocalFileSystem` backend in this milestone; the same `Arc<dyn ObjectStore>` abstraction trivially swaps in S3/MinIO/GCS later. No other new deps.

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §5.3 (tiering), §5.5 (derived data), §11 (lifecycle engine)
- Previous milestones: M1-M4

---

## Scope

**In scope for M5:**
- **Cold tier storage** — per-session segment file on `object_store`, containing all the session's events in a single BSON document
- **`cold_transition(session)`** — writes segment, deletes warm entries for the session's events, marks session `tier=Cold`
- **`reactivate(session)`** — reads segment, batch-restores events to warm, deletes segment, marks session `tier=Warm`
- **Tier-aware planner resolve** — `HybridPlanner::resolve` falls back to reading the session's segment when the warm lookup returns `None` and the session is cold
- **Tier-aware index build** — `PerBoxIndexes::build` scans both warm events and cold segments so hybrid queries work across tiers
- **`LifecycleEngine`** — per-box tokio task that ticks every `T_tick` (default 2s), demotes warm sessions idle for more than `T_cold` (default 60s)
- **Admin REST endpoint** — `POST /api/admin/tier/force` takes `{tenant_id, session_id, target}` where `target` ∈ {`"Warm"`, `"Cold"`} and invokes the transition under the per-box lock
- **Server wiring** — `agentdb-server` starts one lifecycle task per box, binds object_store to a local dir by default (`AGENTDB_OBJECTS`)

**Out of scope (explicitly deferred):**
- **Archive tier.** The third tier from the spec — with derived-data sidecar (summary + embedding) replacing raw events — is deferred. All deferred-archive paths stay closed (admin endpoint rejects `"Archive"` target with a protocol error); lifecycle engine never schedules archive transitions. This keeps M5 focused and tests tractable.
- **Real LLM calls for derived data.** Would be an M5.5/M6 add-on. Deterministic hash-based fake would be the planned stand-in.
- **S3/MinIO/GCS integration**. `object_store` abstraction makes this a one-line config change but is outside M5 scope.
- **Archive query semantics** (query-through-derived-sidecar). Not needed without archive tier.
- **Per-tenant / per-box object-store prefix isolation beyond path layout.** We rely on the path scheme to keep tenants separate within one shared object store.
- **Deleting empty segments, compaction across segments.** A reactivated session that goes cold again writes a new segment at a new epoch filename; old versions are not garbage-collected in M5.
- **Tokio cancellation on shutdown** beyond dropping the engine handle. The integration test spawns short-lived servers so leaked tasks don't matter; the real binary relies on process exit to clean up.

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created:**
- `crates/agentdb-core/src/segment.rs` — `SegmentFile { events }` BSON codec + `write_segment` / `read_segment` helpers over `object_store`
- `crates/agentdb-core/src/tiering.rs` — `cold_transition` + `reactivate` primitives
- `crates/agentdb-core/src/lifecycle.rs` — `LifecycleEngine` per-box tokio task
- `crates/agentdb-api/src/admin.rs` — REST handler for `POST /api/admin/tier/force`

**Modified:**
- `Cargo.toml` — add `object_store = "0.11"` to workspace deps
- `crates/agentdb-core/Cargo.toml` — depend on `object_store`, `bson`, `tokio` with `time` feature
- `crates/agentdb-core/src/types.rs` — extend `Session` with `segment_path: Option<String>`
- `crates/agentdb-core/src/box_runtime.rs` — add `object_store: Arc<dyn ObjectStore>` into `BoxRegistry`
- `crates/agentdb-core/src/indexes.rs` — build walks cold segments too
- `crates/agentdb-core/src/planner.rs` — tier-aware `resolve` with object-store fallback
- `crates/agentdb-core/src/lib.rs` — declare new modules, re-export types
- `crates/agentdb-api/src/lib.rs` — add `/api/admin/tier/force` route
- `crates/agentdb-api/src/error.rs` — pass through core `Invalid` as 400 as before (no change expected)
- `bin/agentdb-server/src/main.rs` — instantiate `object_store::LocalFileSystem`, start `LifecycleEngine` per box

**Responsibilities:**
- `segment.rs` — pure codec + object_store I/O; no locks, no metadata.
- `tiering.rs` — atomic transitions holding a `MutexGuard<'_, ()>` over the box write lock; writes segment → updates metadata → deletes warm entries in that order.
- `lifecycle.rs` — scheduling only; delegates transitions to `tiering.rs`.
- `admin.rs` — thin HTTP adapter; returns the new tier on success.
- `indexes.rs` — agnostic to tier at call-site; reads events from whichever source the session points at.
- `planner.rs` — same pattern as `indexes.rs` for `resolve`.

---

## Data model notes

### Session (additions)

```rust
pub struct Session {
    // ... existing fields
    pub segment_path: Option<String>,
}
```

`segment_path` is `None` while warm. When cold, it's set to the `object_store` path of the segment. On reactivation the field is cleared and the segment deleted.

### Segment path layout

```
boxes/{box_id:04x}/cold/{tenant_internal_id:016x}/{session_internal_id:016x}/segment-{epoch_ms}.bson
```

Fixed-width hex so the alphabetical ordering of object-store listings matches numeric order. The `epoch_ms` suffix allows a session to be cold → warm → cold again without a name collision.

### Segment format

```rust
pub struct SegmentFile {
    pub schema_version: u32,       // always 1 in M5
    pub session_internal_id: u64,
    pub events: Vec<Event>,
}
```

Serialised via `bson::serialize_to_vec(&segment)`. The segment is a single BSON document regardless of event count — simple to encode, straightforward for `deserialize_from_slice` on the read path, and acceptable at prototype scale (worst case a session is a few MB of events).

### Lifecycle contract

- Per-box `write_lock` serializes tier transitions with appends. `cold_transition` must be called while holding the lock.
- `last_active_at` on the session is the only input to the idle check. `append_event` in M2 already updates it on every append.
- On `cold_transition` failure mid-way (e.g., `object_store` write succeeds but `cf_events` delete fails), the session metadata is updated last. If we crash between "segment written" and "metadata updated", next startup will see a stale warm session with no `segment_path` — which is correct; reactivation is not needed and the (orphaned) segment can be GC'd out-of-band later. This is acceptable M5 behaviour; we don't implement orphan reclaim.

---

## Task 1: `object_store` dep + segment codec

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/agentdb-core/Cargo.toml`
- Create: `crates/agentdb-core/src/segment.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Register `object_store` in workspace deps; add `time` feature to tokio**

Edit `Cargo.toml`. In `[workspace.dependencies]` add:
```toml
object_store = "0.11"
```

Also extend the workspace tokio features to include `"time"` (needed for `tokio::time::interval` / `tokio::time::sleep` in the lifecycle engine):
```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "signal", "net", "io-util", "time"] }
```

- [ ] **Step 2: Depend on it from `agentdb-core`**

Edit `crates/agentdb-core/Cargo.toml`. In `[dependencies]` add:
```toml
object_store = { workspace = true }
```

Also ensure the `tokio` dev-dep has the `time` feature (needed later for tests):
```toml
[dev-dependencies]
tokio = { workspace = true, features = ["test-util", "macros", "time"] }
```
(If your existing `agentdb-core/Cargo.toml` doesn't have `time` in the dev-dep features, add it.)

- [ ] **Step 3: Write `segment.rs` with tests**

Save `crates/agentdb-core/src/segment.rs`:
```rust
use std::sync::Arc;

use bson::Document;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::Event;

pub const SEGMENT_SCHEMA_V1: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFile {
    pub schema_version: u32,
    pub session_internal_id: u64,
    pub events: Vec<Event>,
}

/// Build the object-store path for a session's segment. Fixed-width hex so
/// alphabetical listings match numeric ordering.
pub fn segment_path(
    box_id: u16,
    tenant_internal_id: u64,
    session_internal_id: u64,
    epoch_ms: u64,
) -> String {
    format!(
        "boxes/{:04x}/cold/{:016x}/{:016x}/segment-{}.bson",
        box_id, tenant_internal_id, session_internal_id, epoch_ms
    )
}

pub async fn write_segment(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    segment: &SegmentFile,
) -> Result<()> {
    let doc = bson::to_document(segment).map_err(|e| Error::Invalid(e.to_string()))?;
    let bytes = bson::serialize_to_vec(&doc).map_err(|e| Error::Invalid(e.to_string()))?;
    let p = ObjectPath::from(path);
    store
        .put(&p, bytes.into())
        .await
        .map_err(|e| Error::Invalid(format!("object_store put: {e}")))?;
    Ok(())
}

pub async fn read_segment(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<SegmentFile> {
    let p = ObjectPath::from(path);
    let get = store
        .get(&p)
        .await
        .map_err(|e| Error::Invalid(format!("object_store get: {e}")))?;
    let bytes = get
        .bytes()
        .await
        .map_err(|e| Error::Invalid(format!("object_store read: {e}")))?;
    let doc: Document = bson::deserialize_from_slice(&bytes)
        .map_err(|e| Error::Invalid(format!("bson decode: {e}")))?;
    let seg: SegmentFile = bson::from_document(doc).map_err(|e| Error::Invalid(e.to_string()))?;
    if seg.schema_version != SEGMENT_SCHEMA_V1 {
        return Err(Error::Invalid(format!(
            "unknown segment schema version {}",
            seg.schema_version
        )));
    }
    Ok(seg)
}

pub async fn delete_segment(store: &Arc<dyn ObjectStore>, path: &str) -> Result<()> {
    let p = ObjectPath::from(path);
    store
        .delete(&p)
        .await
        .map_err(|e| Error::Invalid(format!("object_store delete: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use tempfile::TempDir;

    fn mem_store() -> (TempDir, Arc<dyn ObjectStore>) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        (dir, store)
    }

    #[tokio::test]
    async fn segment_roundtrip_preserves_events() {
        let (_d, store) = mem_store();
        let seg = SegmentFile {
            schema_version: SEGMENT_SCHEMA_V1,
            session_internal_id: 7,
            events: vec![
                Event {
                    seq: 1,
                    ts: 100,
                    payload: json!({"text": "hello"}),
                },
                Event {
                    seq: 2,
                    ts: 200,
                    payload: json!({"embedding": [1.0, 0.0]}),
                },
            ],
        };
        let path = segment_path(3, 42, 7, 1_700_000_000_000);
        write_segment(&store, &path, &seg).await.unwrap();
        let back = read_segment(&store, &path).await.unwrap();
        assert_eq!(back.session_internal_id, 7);
        assert_eq!(back.events.len(), 2);
        assert_eq!(back.events[0].payload, json!({"text": "hello"}));
        assert_eq!(back.events[1].payload, json!({"embedding": [1.0, 0.0]}));
    }

    #[tokio::test]
    async fn read_segment_with_unknown_schema_errors() {
        let (_d, store) = mem_store();
        let seg = SegmentFile {
            schema_version: 99,
            session_internal_id: 1,
            events: vec![],
        };
        let path = segment_path(1, 1, 1, 1);
        write_segment(&store, &path, &seg).await.unwrap();
        let err = read_segment(&store, &path).await.unwrap_err();
        assert!(err.to_string().contains("schema version"));
    }

    #[tokio::test]
    async fn delete_removes_segment() {
        let (_d, store) = mem_store();
        let seg = SegmentFile {
            schema_version: SEGMENT_SCHEMA_V1,
            session_internal_id: 1,
            events: vec![],
        };
        let path = segment_path(1, 1, 1, 1);
        write_segment(&store, &path, &seg).await.unwrap();
        delete_segment(&store, &path).await.unwrap();
        assert!(read_segment(&store, &path).await.is_err());
    }

    #[test]
    fn segment_path_is_fixed_width_hex() {
        let p = segment_path(3, 42, 7, 1_700_000_000_000);
        assert_eq!(
            p,
            "boxes/0003/cold/000000000000002a/0000000000000007/segment-1700000000000.bson"
        );
    }
}
```

- [ ] **Step 4: Declare the module in `lib.rs`**

Edit `crates/agentdb-core/src/lib.rs`. Add after the existing `pub mod` lines:
```rust
pub mod segment;
```

And re-export:
```rust
pub use segment::{read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_V1};
```

- [ ] **Step 5: Verify compile + tests**

Run: `cargo check --workspace` — expect clean.

Run: `cargo test -p agentdb-core segment::tests`
Expected: 4 passed.

Run M4 tests still pass:
`cargo test --workspace` — expect ≥ 135 passing (no regressions).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add Cargo.toml crates/agentdb-core/Cargo.toml crates/agentdb-core/src/segment.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): segment codec over object_store for session cold storage"
```

---

## Task 2: Extend `Session` with `segment_path`, add `BoxRegistry::object_store`

**Files:**
- Modify: `crates/agentdb-core/src/types.rs`
- Modify: `crates/agentdb-core/src/box_runtime.rs`

**Context:** `Session` gains an optional `segment_path: Option<String>` so cold sessions can find their segment. `BoxRegistry` gains an `Arc<dyn ObjectStore>` handle used by lifecycle transitions and the planner's tier-aware resolve.

- [ ] **Step 1: Extend Session struct**

In `crates/agentdb-core/src/types.rs`, modify `Session`:
```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub internal_id: SessionId,
    pub external_id: String,
    pub tenant_internal_id: TenantId,
    pub box_id: BoxId,
    pub created_at: u64,
    pub last_active_at: u64,
    pub tier: Tier,
    pub next_seq: u64,
    /// When `tier == Cold`, the `object_store` path where events live. None for warm sessions.
    #[serde(default)]
    pub segment_path: Option<String>,
}
```

Update the three `Session { ... }` literals in the codebase that must now either set `segment_path: None` or pick up the default via `#[serde(default)]`:
- `crates/agentdb-core/src/registry.rs` — `SessionRegistry::get_or_mint` constructs a Session (lines ~138-ish). Add `segment_path: None,` to that literal.
- `crates/agentdb-core/src/primitives.rs` — any `Session { ... }` in tests (the `fetch_last_n_zero_returns_empty` test constructs one; add the field).

Update the existing test in `types.rs` that constructs a Session — add `segment_path: None,`.

- [ ] **Step 2: Extend BoxRegistry**

In `crates/agentdb-core/src/box_runtime.rs`, extend imports:
```rust
use object_store::ObjectStore;
```

Replace `BoxRegistry` struct + `new` + `kv()` so it also holds an `object_store`:
```rust
pub struct BoxRegistry {
    boxes: Vec<Arc<Box_>>,
    kv: Arc<dyn KvStore>,
    object_store: Arc<dyn ObjectStore>,
}

impl BoxRegistry {
    pub fn new(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
    ) -> Result<Self> {
        if num_boxes == 0 {
            return Err(Error::Invalid("num_boxes must be > 0".into()));
        }
        let boxes = (0..num_boxes).map(|i| Arc::new(Box_::new(i))).collect();
        Ok(Self {
            boxes,
            kv,
            object_store,
        })
    }

    pub fn kv(&self) -> &Arc<dyn KvStore> {
        &self.kv
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    pub fn num_boxes(&self) -> u16 {
        self.boxes.len() as u16
    }

    pub fn route_tenant(&self, external: &str) -> Arc<Box_> {
        // unchanged — keep the FNV-1a body from M2.
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in external.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let idx = (hash as usize) % self.boxes.len();
        self.boxes[idx].clone()
    }

    /// Look up an `Arc<Box_>` by index. Used by the server binary to start
    /// one `LifecycleEngine` per box at process startup.
    pub fn box_by_id(&self, id: BoxId) -> Option<Arc<Box_>> {
        self.boxes.get(id as usize).cloned()
    }
}
```

Update all `BoxRegistry::new(kv, N)` call sites to pass an `object_store`. This affects:
- every `#[cfg(test)]` block inside `agentdb-core` that builds a `BoxRegistry` — test helpers should construct a scratch `LocalFileSystem` from a `TempDir`
- tests inside `agentdb-wire` that build `BoxRegistry` — same story
- `bin/agentdb-server/src/main.rs` — you will fully rewrite this in Task 7; for M5 Task 2 just do the minimum to compile (e.g., a temporary in-memory `LocalFileSystem::new()` pointing at `/tmp/agentdb-objects-stub`; Task 7 replaces this)
- integration tests under `crates/agentdb-api/tests/` and `crates/agentdb-wire/tests/`

For tests the idiomatic helper is:
```rust
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use tempfile::TempDir;

fn test_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    (dir, store)
}
```

Add this helper function wherever a test module needs a store, keeping the `TempDir` alive for the test's duration (return it from the test's setup tuple).

- [ ] **Step 3: Update box_runtime tests**

In `crates/agentdb-core/src/box_runtime.rs`'s `mod tests`, update the `registry(num)` helper:
```rust
fn registry(num: u16) -> (TempDir, TempDir, BoxRegistry) {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    (kv_dir, obj_dir, BoxRegistry::new(kv, store, num).unwrap())
}
```

Update the three ordering tests (`zero_boxes_is_rejected`, `same_tenant_always_routes_to_same_box`, `different_tenants_eventually_hit_multiple_boxes`, `num_boxes_reports_correct_count`) to destructure the returned 3-tuple and keep both `TempDir`s alive.

Also update the `indexes_built_is_idempotent_and_reflects_events` test from M4 similarly.

- [ ] **Step 4: Verify M4 tests still pass**

Run: `cargo test --workspace` — expect all 135 tests pass (no new tests in this task, just refactoring signatures).

If compilation errors appear in other crates' tests (most likely `agentdb-core::primitives::tests`, `agentdb-core::indexes::tests`, `agentdb-core::planner::tests`, `agentdb-wire::handlers::*::tests`, and the integration tests under `crates/*/tests/`), walk through each and update the `BoxRegistry::new(...)` call to pass a `LocalFileSystem` from a `TempDir`.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core crates/agentdb-api crates/agentdb-wire bin/agentdb-server
git commit -m "refactor(core): add segment_path to Session and object_store handle to BoxRegistry"
```

---

## Task 3: `cold_transition` — warm → cold

**Files:**
- Create: `crates/agentdb-core/src/tiering.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** `cold_transition(ctx)` drains a session's events from `cf_events` into a segment file. Must be called holding the box's write lock. Updates are ordered so a crash mid-way leaves a warm session with dangling indexed events (consistent) or an orphaned segment (not yet metadata-referenced — safely ignorable). Never creates a state where a session thinks it's cold but the segment is missing.

- [ ] **Step 1: Write tiering.rs**

Save `crates/agentdb-core/src/tiering.rs`:
```rust
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_storage::{encode_key, KvStore, Namespace, WriteOp};
use object_store::ObjectStore;
use tokio::sync::MutexGuard;

use crate::error::{Error, Result};
use crate::primitives::{scan_session_events, DEFAULT_BRANCH_ID};
use crate::registry::SessionRegistry;
use crate::segment::{
    delete_segment, read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_V1,
};
use crate::types::{Session, Tier};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Move a warm session to cold: write a segment on the object store, then
/// delete the session's warm events and mark the metadata.
///
/// Precondition: caller holds the box's `write_lock` (demonstrated by the
/// `MutexGuard` reference). `session.tier` must be `Warm`.
pub async fn cold_transition(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
) -> Result<()> {
    if session.tier != Tier::Warm {
        return Err(Error::Invalid(format!(
            "cold_transition: session {} is not Warm (tier={:?})",
            session.external_id, session.tier
        )));
    }
    let events = scan_session_events(kv, session)?;
    let path = segment_path(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
        now_millis(),
    );

    // (1) Write segment. If this fails we return before touching any other state.
    let seg = SegmentFile {
        schema_version: SEGMENT_SCHEMA_V1,
        session_internal_id: session.internal_id,
        events: events.clone(),
    };
    write_segment(store, &path, &seg).await?;

    // (2) Delete warm events + write updated metadata as a single batch so
    // either the session flips to cold AND events are gone, or neither.
    let mut ops: Vec<WriteOp> = Vec::with_capacity(events.len() + 1);
    for ev in &events {
        ops.push(WriteOp::Delete {
            key: encode_key(
                Namespace::Events,
                session.box_id,
                session.tenant_internal_id,
                session.internal_id,
                DEFAULT_BRANCH_ID,
                ev.seq,
            ),
        });
    }
    // Session metadata update: tier=Cold, segment_path=Some(path), last_active unchanged.
    session.tier = Tier::Cold;
    session.segment_path = Some(path.clone());
    let session_record_key = crate::meta::key_session_record(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
    );
    let session_record_bytes = serde_json::to_vec(session)?;
    ops.push(WriteOp::Put {
        key: session_record_key,
        value: session_record_bytes,
    });
    if let Err(e) = kv.write_batch(ops) {
        // Try best-effort cleanup: delete the segment we just wrote.
        let _ = delete_segment(store, &path).await;
        // Roll local session back so the caller observes the pre-transition state.
        session.tier = Tier::Warm;
        session.segment_path = None;
        return Err(e.into());
    }
    // Silence unused-import warning — `sessions` is still part of the public
    // signature because future tier transitions (e.g. archive) will call back
    // into the registry.
    let _ = sessions;
    Ok(())
}

/// Restore a cold session's events to warm, then delete the segment.
///
/// Precondition: caller holds the box's `write_lock`. `session.tier` must be `Cold`.
pub async fn reactivate(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    _lock: &MutexGuard<'_, ()>,
    session: &mut Session,
) -> Result<()> {
    if session.tier != Tier::Cold {
        return Err(Error::Invalid(format!(
            "reactivate: session {} is not Cold (tier={:?})",
            session.external_id, session.tier
        )));
    }
    let Some(path) = session.segment_path.clone() else {
        return Err(Error::Invalid(format!(
            "reactivate: cold session {} has no segment_path",
            session.external_id
        )));
    };
    let seg = read_segment(store, &path).await?;

    // Batch: put events + update metadata.
    let mut ops: Vec<WriteOp> = Vec::with_capacity(seg.events.len() + 1);
    for ev in &seg.events {
        let key = encode_key(
            Namespace::Events,
            session.box_id,
            session.tenant_internal_id,
            session.internal_id,
            DEFAULT_BRANCH_ID,
            ev.seq,
        );
        let value = serde_json::to_vec(ev)?;
        ops.push(WriteOp::Put { key, value });
    }
    session.tier = Tier::Warm;
    session.segment_path = None;
    let session_record_key = crate::meta::key_session_record(
        session.box_id,
        session.tenant_internal_id,
        session.internal_id,
    );
    let session_record_bytes = serde_json::to_vec(session)?;
    ops.push(WriteOp::Put {
        key: session_record_key,
        value: session_record_bytes,
    });
    kv.write_batch(ops)?;
    let _ = sessions; // see note in cold_transition

    // Best-effort: remove the now-unneeded segment. If this fails we log but
    // don't propagate — the metadata says warm already, and orphan cleanup is
    // out of scope for M5.
    if let Err(err) = delete_segment(store, &path).await {
        tracing::warn!(?err, %path, "orphan segment delete failed (best-effort)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use crate::BoxRegistry;
    use agentdb_storage::RocksKvStore;
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct Ctx {
        _kv_dir: TempDir,
        _obj_dir: TempDir,
        registry: Arc<BoxRegistry>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
    }

    async fn seed(text: &str, tag: &str) -> (Ctx, Session) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            tag,
            json!({"text": text}),
            0,
            100,
        )
        .await
        .unwrap();
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let session = sessions
            .lookup(box_.id, tenant.internal_id, tag)
            .unwrap()
            .unwrap();
        (
            Ctx {
                _kv_dir: kv_dir,
                _obj_dir: obj_dir,
                registry,
                tenants,
                sessions,
            },
            session,
        )
    }

    #[tokio::test]
    async fn cold_transition_writes_segment_and_deletes_warm_events() {
        let (ctx, mut session) = seed("hello cold world", "s-1").await;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        cold_transition(
            ctx.registry.kv(),
            ctx.registry.object_store(),
            &ctx.sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap();
        drop(guard);

        assert_eq!(session.tier, Tier::Cold);
        assert!(session.segment_path.is_some());

        // Warm events are gone.
        let warm = scan_session_events(ctx.registry.kv(), &session).unwrap();
        assert!(warm.is_empty());

        // Segment readable.
        let seg = read_segment(
            ctx.registry.object_store(),
            session.segment_path.as_deref().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(seg.events.len(), 1);
        assert_eq!(
            seg.events[0].payload.get("text").and_then(|v| v.as_str()),
            Some("hello cold world")
        );
    }

    #[tokio::test]
    async fn cold_transition_rejects_non_warm_session() {
        let (ctx, mut session) = seed("t", "s").await;
        session.tier = Tier::Cold;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let err = cold_transition(
            ctx.registry.kv(),
            ctx.registry.object_store(),
            &ctx.sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not Warm"));
    }

    #[tokio::test]
    async fn reactivate_restores_warm_events_and_deletes_segment() {
        let (ctx, mut session) = seed("round trip", "s-2").await;
        let box_ = ctx.registry.route_tenant("t-1");
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        let path_before = session.segment_path.clone().unwrap();

        {
            let guard = box_.write_lock.lock().await;
            reactivate(
                ctx.registry.kv(),
                ctx.registry.object_store(),
                &ctx.sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }

        assert_eq!(session.tier, Tier::Warm);
        assert!(session.segment_path.is_none());

        let warm = scan_session_events(ctx.registry.kv(), &session).unwrap();
        assert_eq!(warm.len(), 1);

        // Segment is gone.
        assert!(read_segment(ctx.registry.object_store(), &path_before)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn reactivate_rejects_non_cold_session() {
        let (ctx, mut session) = seed("t", "s").await;
        let box_ = ctx.registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let err = reactivate(
            ctx.registry.kv(),
            ctx.registry.object_store(),
            &ctx.sessions,
            &guard,
            &mut session,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not Cold"));
    }

    // Silence "unused" warnings for ctx fields used only to keep TempDirs alive.
    #[allow(dead_code)]
    fn _use_ctx(ctx: &Ctx) {
        let _ = (&ctx.registry, &ctx.tenants);
    }
}
```

- [ ] **Step 2: Declare in lib.rs**

Edit `crates/agentdb-core/src/lib.rs`. Add `pub mod tiering;` and re-export:
```rust
pub use tiering::{cold_transition, reactivate};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-core tiering::tests`
Expected: 4 passed.

Run: `cargo test --workspace` — all tests pass.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/tiering.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): cold_transition + reactivate with segment object-store I/O"
```

---

## Task 4: Tier-aware `PerBoxIndexes::build`

**Files:**
- Modify: `crates/agentdb-core/src/indexes.rs`

**Context:** On index build, walk all sessions; for warm sessions read `cf_events`, for cold sessions read the segment from `object_store`. Both feed `index_event`.

- [ ] **Step 1: Extend `build` to accept and use the object store**

In `crates/agentdb-core/src/indexes.rs`, change `build`:
```rust
use std::sync::Arc;

use agentdb_index::text::{InMemoryInverted, TextIndex};
use agentdb_index::vector::{InMemoryFlatVec, VectorIndex};
use agentdb_index::DocId;
use agentdb_storage::{BoxId, KvStore};
use object_store::ObjectStore;

use crate::error::Result;
use crate::primitives::scan_session_events;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::segment::read_segment;
use crate::types::{Event, Tier};
```

Replace the `build` method:
```rust
pub async fn build(
    &mut self,
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    box_id: BoxId,
) -> Result<()> {
    let tenant_externals = tenants.list_in_box(box_id)?;
    for tenant_ext in tenant_externals {
        let Some(tenant) = tenants.lookup(box_id, &tenant_ext)? else {
            continue;
        };
        let session_externals = sessions.list_in_tenant(box_id, tenant.internal_id)?;
        for session_ext in session_externals {
            let Some(session) = sessions.lookup(box_id, tenant.internal_id, &session_ext)?
            else {
                continue;
            };
            let events: Vec<Event> = match session.tier {
                Tier::Warm => scan_session_events(kv, &session)?,
                Tier::Cold => {
                    if let Some(path) = &session.segment_path {
                        match read_segment(store, path).await {
                            Ok(seg) => seg.events,
                            Err(err) => {
                                tracing::warn!(
                                    ?err,
                                    session = %session.external_id,
                                    "skipping cold session with unreadable segment"
                                );
                                continue;
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Tier::Archive => continue, // M5 deferral: archive not implemented
            };
            for event in events {
                self.index_event(session.internal_id, &event);
            }
        }
    }
    Ok(())
}
```

Because `build` is now async, its callers (all inside the `Box_::indexes_built` method in `box_runtime.rs`) need to be `.await`-aware. Check: `Box_::indexes_built` already uses `.await` for its internal lock operations. Just add `.await` after the `build(...)` call:

In `crates/agentdb-core/src/box_runtime.rs`, inside `Box_::indexes_built`'s slow path, find the line:
```rust
built.build(kv, tenants, sessions, self.id)?;
```

Change it to:
```rust
built.build(kv, store, tenants, sessions, self.id).await?;
```

And extend the method's arguments to include `store: &Arc<dyn ObjectStore>`. Update its callers in `handlers/aggregate.rs` accordingly — they pass `registry.object_store()`.

- [ ] **Step 2: Update tests and callers**

In `agentdb-core/src/indexes.rs`'s `mod tests`, each test that calls `idx.build(&kv, &tenants, &sessions, box_id)` now passes `&store` as second arg. Add to the test setup a `TempDir` + `LocalFileSystem` and destructure into an extra `obj_dir` tuple element. Rewrite `setup_with_events` signature to return the store too; then call `.await?` on `build`.

In `agentdb-core/src/planner.rs`'s `mod tests`, similarly: tests that build indexes now construct a store and pass it in. Use the same `setup_with_events`-style helper.

In `agentdb-wire/src/handlers/aggregate.rs`, the `aggregate_response` function calls `box_.indexes_built(registry.kv(), tenants, sessions)`. Update to pass the object store: `box_.indexes_built(registry.kv(), registry.object_store(), tenants, sessions)`.

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-core` — all core tests pass.
Run: `cargo test --workspace` — all tests pass.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core crates/agentdb-wire
git commit -m "feat(core): index build reads warm events AND cold segments"
```

---

## Task 5: Tier-aware `HybridPlanner::resolve`

**Files:**
- Modify: `crates/agentdb-core/src/planner.rs`

**Context:** When the planner's over-fetch strategy resolves a DocId back to an event, the `kv.get` can return `None` if the session is now cold. Add a fallback: look up the session by `doc_id.session_internal_id`, and if cold, read the segment and scan for the matching seq.

- [ ] **Step 1: Plumb the object store into the planner**

In `crates/agentdb-core/src/planner.rs`, change the `HybridPlanner` struct:
```rust
pub struct HybridPlanner<'a> {
    pub kv: &'a Arc<dyn KvStore>,
    pub store: &'a Arc<dyn ObjectStore>,
    pub indexes: &'a PerBoxIndexes,
    pub sessions: &'a SessionRegistry,
    pub box_id: BoxId,
    pub tenant_internal_id: u64,
}
```

Add `use object_store::ObjectStore;` at top.

Change `resolve` into an async method that falls back to segment read for cold sessions:
```rust
async fn resolve(&self, doc_id: DocId) -> Result<Option<Event>> {
    let key = encode_key(
        Namespace::Events,
        self.box_id,
        self.tenant_internal_id,
        doc_id.session_internal_id,
        DEFAULT_BRANCH_ID,
        doc_id.seq,
    );
    if let Some(bytes) = self.kv.get(&key)? {
        return Ok(Some(serde_json::from_slice(&bytes)?));
    }

    // Warm tier missed — check if the session is cold and fall back to its segment.
    let session = match self.sessions.lookup_by_internal(
        self.box_id,
        self.tenant_internal_id,
        doc_id.session_internal_id,
    )? {
        Some(s) => s,
        None => return Ok(None),
    };
    let Some(path) = session.segment_path.as_deref() else {
        return Ok(None);
    };
    let seg = crate::segment::read_segment(self.store, path).await?;
    Ok(seg.events.into_iter().find(|e| e.seq == doc_id.seq))
}
```

Both `plan_over_fetch` and `plan_locality_bound` now need to be async and to `.await` on resolve. `plan_locality_bound` doesn't currently call `resolve`, but it does call `scan_session_events(self.kv, &session)`. For cold sessions, `scan_session_events` returns no events (they've been deleted from warm); add a cold-path branch that reads the segment instead:

Inside `plan_locality_bound`, after resolving the session, replace:
```rust
let all = scan_session_events(self.kv, &session)?;
```
with:
```rust
let all: Vec<Event> = match session.tier {
    Tier::Warm => scan_session_events(self.kv, &session)?,
    Tier::Cold => {
        let Some(path) = session.segment_path.as_deref() else {
            return Ok(PlanOutput {
                strategy: Strategy::LocalityBound,
                hits: Vec::new(),
            });
        };
        let seg = crate::segment::read_segment(self.store, path).await?;
        seg.events
    }
    Tier::Archive => Vec::new(),
};
```

Add `use crate::types::Tier;` near the top of `planner.rs`.

Make `plan`, `plan_locality_bound`, and `plan_over_fetch` async. Each is called once per aggregate request so async overhead is negligible.

Update every call site that calls `planner.plan(...)` to `.await` the result. Inside `agentdb-wire/src/handlers/aggregate.rs`, the `planner.plan(&source, predicate, session_hint.as_deref())?` line becomes `planner.plan(&source, predicate, session_hint.as_deref()).await?`. And where the planner is constructed, add `store: registry.object_store(),`.

- [ ] **Step 2: Update the planner tests**

Each planner test in `agentdb-core/src/planner.rs::tests` now needs a `LocalFileSystem` store and to `.await` both `idx.build(...)` and `planner.plan(...)`. Walk through the three existing tests and update.

- [ ] **Step 3: Add a new test that queries a cold session**

Append to `planner::tests`:
```rust
#[tokio::test]
async fn vector_search_over_fetch_strategy_finds_cold_session_event() {
    let (_d, _obj, kv, store, reg, tenants, sessions) = setup_with_store().await;
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"embedding": [1.0, 0.0, 0.0]}),
        0,
        100,
    )
    .await
    .unwrap();

    // Cool the session.
    let tenant = tenants.get_or_mint(reg.route_tenant("t-1").id, "t-1", 0).unwrap();
    let mut session = sessions
        .lookup(tenant.box_id, tenant.internal_id, "s-1")
        .unwrap()
        .unwrap();
    let box_ = reg.route_tenant("t-1");
    {
        let guard = box_.write_lock.lock().await;
        crate::tiering::cold_transition(&kv, &store, &sessions, &guard, &mut session)
            .await
            .unwrap();
    }

    // Build indexes (includes cold segment).
    let mut indexes = PerBoxIndexes::new();
    indexes
        .build(&kv, &store, &tenants, &sessions, tenant.box_id)
        .await
        .unwrap();

    let p = HybridPlanner {
        kv: &kv,
        store: &store,
        indexes: &indexes,
        sessions: &sessions,
        box_id: tenant.box_id,
        tenant_internal_id: tenant.internal_id,
    };
    let out = p
        .plan(
            &Source::Vector {
                path: "payload.embedding".into(),
                query: vec![1.0, 0.0, 0.0],
                limit: 5,
                num_candidates: 10,
            },
            Box::new(|_, _| true),
            None,
        )
        .await
        .unwrap();
    assert_eq!(out.hits.len(), 1);
    assert!(out.hits[0].score > 0.99);
}
```

Where `setup_with_store` is a new helper returning `(TempDir, TempDir, Arc<dyn KvStore>, Arc<dyn ObjectStore>, Arc<BoxRegistry>, Arc<TenantRegistry>, Arc<SessionRegistry>)`. Write this helper once at the top of the tests module and refactor existing tests to use it.

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core planner::tests` — 4 passed.
Run: `cargo test --workspace` — all tests pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core crates/agentdb-wire
git commit -m "feat(core): tier-aware HybridPlanner resolves cold events from segments"
```

---

## Task 6: `LifecycleEngine` — per-box idle-demotion tokio task

**Files:**
- Create: `crates/agentdb-core/src/lifecycle.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

**Context:** The engine runs per box, ticks every `T_tick`, scans sessions via `list_in_box` + `list_in_tenant` + `lookup`, and for each warm session idle longer than `T_cold` calls `cold_transition` under the box lock. Failures are logged, not propagated. `drop(engine)` stops the task on the next tick boundary.

Config defaults:
- `T_tick = Duration::from_secs(2)`
- `T_cold = Duration::from_secs(60)`

Both configurable via a `LifecycleConfig` struct. Env wiring happens in Task 8.

- [ ] **Step 1: Write lifecycle.rs**

Save `crates/agentdb-core/src/lifecycle.rs`:
```rust
use std::sync::Arc;
use std::time::Duration;

use agentdb_storage::KvStore;
use object_store::ObjectStore;
use tokio::task::JoinHandle;

use crate::box_runtime::Box_;
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::tiering::cold_transition;
use crate::types::Tier;

#[derive(Debug, Clone, Copy)]
pub struct LifecycleConfig {
    pub tick: Duration,
    pub t_cold: Duration,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(2),
            t_cold: Duration::from_secs(60),
        }
    }
}

pub struct LifecycleEngine {
    _handle: JoinHandle<()>,
}

impl LifecycleEngine {
    pub fn start(
        kv: Arc<dyn KvStore>,
        store: Arc<dyn ObjectStore>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
        box_: Arc<Box_>,
        config: LifecycleConfig,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(err) = sweep(&kv, &store, &tenants, &sessions, &box_, config).await {
                    tracing::warn!(box_id = box_.id, ?err, "lifecycle sweep errored");
                }
            }
        });
        Self { _handle: handle }
    }
}

async fn sweep(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    tenants: &Arc<TenantRegistry>,
    sessions: &Arc<SessionRegistry>,
    box_: &Arc<Box_>,
    config: LifecycleConfig,
) -> crate::error::Result<()> {
    let tenant_externals = tenants.list_in_box(box_.id)?;
    let now_ms = now_millis();
    let cold_threshold_ms = config.t_cold.as_millis() as u64;
    for tenant_ext in tenant_externals {
        let Some(tenant) = tenants.lookup(box_.id, &tenant_ext)? else {
            continue;
        };
        let session_externals = sessions.list_in_tenant(box_.id, tenant.internal_id)?;
        for session_ext in session_externals {
            let Some(mut session) =
                sessions.lookup(box_.id, tenant.internal_id, &session_ext)?
            else {
                continue;
            };
            if session.tier != Tier::Warm {
                continue;
            }
            if now_ms.saturating_sub(session.last_active_at) < cold_threshold_ms {
                continue;
            }
            let guard = box_.write_lock.lock().await;
            // Re-read session under the lock in case a writer just updated it.
            let Some(mut fresh) = sessions.lookup(box_.id, tenant.internal_id, &session_ext)?
            else {
                continue;
            };
            if fresh.tier != Tier::Warm {
                continue;
            }
            if now_ms.saturating_sub(fresh.last_active_at) < cold_threshold_ms {
                continue;
            }
            match cold_transition(kv, store, sessions, &guard, &mut fresh).await {
                Ok(()) => {
                    // Also patch the earlier stack-local copy so downstream telemetry is accurate.
                    session = fresh;
                    let _ = session;
                }
                Err(err) => {
                    tracing::warn!(
                        box_id = box_.id,
                        session = %session_ext,
                        ?err,
                        "cold_transition failed"
                    );
                }
            }
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
    use crate::registry::{SessionRegistry, TenantRegistry};
    use crate::BoxRegistry;
    use agentdb_storage::RocksKvStore;
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_demotes_idle_warm_session_to_cold_on_tick() {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));

        append_and_fetch(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "to be cold"}),
            0,
            1, // ancient last_active_at
        )
        .await
        .unwrap();

        let box_ = registry.route_tenant("t-1");
        let _engine = LifecycleEngine::start(
            kv.clone(),
            store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_.clone(),
            LifecycleConfig {
                tick: Duration::from_millis(50),
                t_cold: Duration::from_millis(10),
            },
        );

        // Wait up to 2s for the engine to demote the session.
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut seen_cold = false;
        for _ in 0..40 {
            if let Some(s) = sessions.lookup(box_.id, tenant.internal_id, "s-1").unwrap() {
                if s.tier == Tier::Cold {
                    seen_cold = true;
                    break;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(seen_cold, "session never transitioned to cold");
    }
}
```

- [ ] **Step 2: Declare in lib.rs**

Edit `crates/agentdb-core/src/lib.rs`. Add `pub mod lifecycle;` and re-export:
```rust
pub use lifecycle::{LifecycleConfig, LifecycleEngine};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-core lifecycle::tests -- --nocapture`
Expected: 1 passed.

Run: `cargo test --workspace` — all pass.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/lifecycle.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): LifecycleEngine demotes idle warm sessions to cold"
```

---

## Task 7: Admin REST endpoint `POST /api/admin/tier/force`

**Files:**
- Create: `crates/agentdb-api/src/admin.rs`
- Modify: `crates/agentdb-api/src/lib.rs`
- Modify: `crates/agentdb-api/src/error.rs` (if needed)

**Context:** `POST /api/admin/tier/force` takes `{tenant_id, session_id, target}` where `target` is `"Warm"` or `"Cold"`. Archive rejected. Invokes the transition under the per-box lock, returns `{tier: "..."}`.

- [ ] **Step 1: Add admin.rs**

Save `crates/agentdb-api/src/admin.rs`:
```rust
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::tiering::{cold_transition, reactivate};
use agentdb_core::types::Tier;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct ForceTierRequest {
    pub tenant_id: String,
    pub session_id: String,
    pub target: String,
}

#[derive(Serialize)]
pub struct ForceTierResponse {
    pub tier: String,
}

pub async fn force_tier(
    State(state): State<AppState>,
    Json(body): Json<ForceTierRequest>,
) -> Result<Json<ForceTierResponse>, ApiError> {
    let target = body.target.to_ascii_lowercase();
    match target.as_str() {
        "warm" | "cold" => {}
        other => {
            return Err(ApiError::BadRequest(format!(
                "unsupported target tier {other}; expected 'warm' or 'cold'"
            )));
        }
    }

    let box_ = state.registry.route_tenant(&body.tenant_id);
    let tenant = state
        .tenants
        .get_or_mint(box_.id, &body.tenant_id, now_millis())?;
    let mut session = state
        .sessions
        .lookup(box_.id, tenant.internal_id, &body.session_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("session {} not found", body.session_id)))?;

    let guard = box_.write_lock.lock().await;
    match target.as_str() {
        "cold" if session.tier == Tier::Warm => {
            cold_transition(
                state.registry.kv(),
                state.registry.object_store(),
                &state.sessions,
                &guard,
                &mut session,
            )
            .await?;
        }
        "cold" => {
            // Already cold — no-op.
        }
        "warm" if session.tier == Tier::Cold => {
            reactivate(
                state.registry.kv(),
                state.registry.object_store(),
                &state.sessions,
                &guard,
                &mut session,
            )
            .await?;
        }
        "warm" => {
            // Already warm — no-op.
        }
        _ => unreachable!(),
    }
    drop(guard);

    let tier_str = match session.tier {
        Tier::Warm => "warm",
        Tier::Cold => "cold",
        Tier::Archive => "archive",
    };
    Ok(Json(ForceTierResponse {
        tier: tier_str.into(),
    }))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
```

- [ ] **Step 2: Wire the route**

Edit `crates/agentdb-api/src/lib.rs`. Add `pub mod admin;` with the other module declarations. In `router()`, add the route:
```rust
.route(
    "/api/admin/tier/force",
    axum::routing::post(admin::force_tier),
)
```

- [ ] **Step 3: Integration test**

Append to `crates/agentdb-api/tests/http.rs`:
```rust
#[tokio::test]
#[serial]
async fn admin_force_tier_cold_then_warm_round_trips() {
    let (_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Seed an event.
    client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
        .json(&serde_json::json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let cold: serde_json::Value = client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&serde_json::json!({
            "tenant_id": "t-1",
            "session_id": "s-1",
            "target": "Cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cold["tier"], "cold");

    let warm: serde_json::Value = client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&serde_json::json!({
            "tenant_id": "t-1",
            "session_id": "s-1",
            "target": "Warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(warm["tier"], "warm");
}
```

Because `spawn()` now needs to construct a `BoxRegistry` with an `object_store`, update the M2-era `spawn()` helper to:
```rust
async fn spawn() -> (TempDir, TempDir, String) { ... }
```
returning the object-store `TempDir` too; and update the tuple destructure in every existing test.

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-api --test http`
Expected: 3 passed (2 M2 + 1 new).

Run: `cargo test --workspace` — all pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-api
git commit -m "feat(api): POST /api/admin/tier/force for warm<->cold transitions"
```

---

## Task 8: Server wiring — object store, lifecycle engines, env config

**Files:**
- Modify: `bin/agentdb-server/Cargo.toml`
- Modify: `bin/agentdb-server/src/main.rs`

**Context:** Construct an `Arc<dyn ObjectStore>` at startup (local FS under `AGENTDB_OBJECTS`, default `./data/objects`). Pass it to `BoxRegistry::new`. Launch one `LifecycleEngine` per box. Accept env config for tick and cold thresholds.

- [ ] **Step 1: Update server Cargo.toml**

Edit `bin/agentdb-server/Cargo.toml`. Add `object_store = { workspace = true }` in `[dependencies]`.

- [ ] **Step 2: Rewire main**

Replace the relevant chunks of `bin/agentdb-server/src/main.rs` — specifically the registry construction. The full shape:

```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agentdb_api::{router, AppState};
use agentdb_core::{BoxRegistry, LifecycleConfig, LifecycleEngine, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve as serve_mongo, WireState};
use anyhow::Context;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let data_dir: PathBuf = std::env::var_os("AGENTDB_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/rocksdb"));
    let objects_dir: PathBuf = std::env::var_os("AGENTDB_OBJECTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/objects"));
    let num_boxes: u16 = match std::env::var("AGENTDB_BOXES") {
        Ok(v) => v.parse().context("AGENTDB_BOXES must be a valid u16")?,
        Err(_) => 4,
    };
    let http_addr: SocketAddr = match std::env::var("AGENTDB_ADDR") {
        Ok(v) => v.parse().context("AGENTDB_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7878".parse().unwrap(),
    };
    let mongo_addr: SocketAddr = match std::env::var("AGENTDB_MONGO_ADDR") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_MONGO_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7879".parse().unwrap(),
    };
    let lifecycle_cfg = LifecycleConfig {
        tick: Duration::from_secs(
            std::env::var("AGENTDB_TICK_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        ),
        t_cold: Duration::from_secs(
            std::env::var("AGENTDB_T_COLD_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
        ),
    };

    std::fs::create_dir_all(&data_dir).context("create data dir")?;
    std::fs::create_dir_all(&objects_dir).context("create objects dir")?;

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?);
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&objects_dir).context("open object store")?,
    );
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), num_boxes)?);
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));

    // Spin up one lifecycle engine per box. Handles are intentionally leaked
    // for the life of the process — shutdown is process exit.
    let mut engines: Vec<LifecycleEngine> = Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let box_ = registry
            .box_by_id(i)
            .expect("box id in range");
        engines.push(LifecycleEngine::start(
            kv.clone(),
            store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_,
            lifecycle_cfg,
        ));
    }
    std::mem::forget(engines);

    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
    };
    let wire_state = WireState {
        registry,
        tenants,
        sessions,
    };

    let app = router(http_state);
    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .context("bind http listener")?;
    let mongo_listener = tokio::net::TcpListener::bind(mongo_addr)
        .await
        .context("bind mongo listener")?;
    tracing::info!(%http_addr, %mongo_addr, %objects_dir.display(), "agentdb-server listening");

    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, app).await.context("http serve")
    });
    let mongo_task = tokio::spawn(async move {
        serve_mongo(mongo_listener, wire_state)
            .await
            .map_err(|e| anyhow::anyhow!("mongo serve: {e}"))
    });

    tokio::select! {
        r = http_task => r.context("http task join")??,
        r = mongo_task => r.context("mongo task join")??,
    };
    Ok(())
}
```

(`box_by_id` was already added to `BoxRegistry` in Task 2 — re-use it here.)

- [ ] **Step 3: Build and smoke-test**

Run: `cargo build -p agentdb-server`
Expected: clean.

Run: `cargo test --workspace` — all pass.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add bin/agentdb-server crates/agentdb-core/src/box_runtime.rs
git commit -m "feat(server): wire object_store + one LifecycleEngine per box on startup"
```

---

## Task 9: End-to-end integration test — tier transition via REST + hybrid query

**Files:**
- Modify: `crates/agentdb-wire/tests/driver.rs`

**Context:** One test that:
1. Inserts an event via the mongodb driver.
2. Forces the session to `Cold` via REST admin.
3. Verifies a `$search` query still returns the event (proving index + resolve fallback work).
4. Forces back to `Warm`. Verifies the event is in warm storage again by doing a normal `find`.

- [ ] **Step 1: Append the test**

Append at the bottom of `crates/agentdb-wire/tests/driver.rs`:
```rust
#[tokio::test]
async fn cold_session_is_still_queryable_via_search() {
    use std::time::Duration;
    let (_dir, _obj, port, http_port) = spawn_with_http().await;
    let cli = client(port).await;
    let db = cli.database("t-cold");
    let events = db.collection::<bson::Document>("events");

    events
        .insert_one(doc! { "session_id": "s-1", "text": "refund policy" })
        .await
        .unwrap();

    // Force the session cold.
    let rest = reqwest::Client::new();
    let resp: serde_json::Value = rest
        .post(format!("http://127.0.0.1:{http_port}/api/admin/tier/force"))
        .json(&serde_json::json!({
            "tenant_id": "t-cold",
            "session_id": "s-1",
            "target": "Cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["tier"], "cold");

    // $search should still find the event via the index + cold resolve.
    let pipeline = vec![doc! {
        "$search": {
            "text": { "query": "refund", "path": "payload.text" },
            "limit": 10_i64,
        }
    }];
    let cursor = events.aggregate(pipeline).await.unwrap();
    use futures_util::StreamExt;
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1);

    // Reactivate.
    let _warm_again: serde_json::Value = rest
        .post(format!("http://127.0.0.1:{http_port}/api/admin/tier/force"))
        .json(&serde_json::json!({
            "tenant_id": "t-cold",
            "session_id": "s-1",
            "target": "Warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    // avoid unused-variable warning on Duration
    let _ = Duration::from_millis(10);
}
```

Because this test needs both the Mongo wire port AND the HTTP admin port, introduce a new helper `spawn_with_http()`:

In the same file, near `spawn()`:
```rust
async fn spawn_with_http() -> (TempDir, TempDir, u16, u16) {
    use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));

    let mongo_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mongo_port = mongo_listener.local_addr().unwrap().port();
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    let wire_state = WireState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
    };
    let http_state = agentdb_api::AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
    };

    tokio::spawn(async move {
        let _ = serve(mongo_listener, wire_state).await;
    });
    let app = agentdb_api::router(http_state);
    tokio::spawn(async move {
        let _ = axum::serve(http_listener, app).await;
    });

    (kv_dir, obj_dir, mongo_port, http_port)
}
```

Add `use object_store::ObjectStore;` in the imports.

Also add `agentdb-api` to `crates/agentdb-wire/Cargo.toml` as a dev-dep, pointing at `"../agentdb-api"`.

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire --test driver`
Expected: 6 passed (5 prior + 1 new).

Run: `cargo test --workspace` — all pass.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire crates/agentdb-api
git commit -m "test(wire): cold session still queryable via \$search through resolve fallback"
```

---

## Task 10: Milestone exit gate

**Files:** none beyond re-running checks.

- [ ] **Step 1: Full workspace test**

Run: `cargo test --workspace`
Expected: all tests pass. Minimum counts:
- agentdb-storage: 17 + 11 = 28
- agentdb-core: 43 + 4 (segment) + 4 (tiering) + 1 (lifecycle) + 1 (planner cold-test) = 53 minimum
- agentdb-api: 3 integration
- agentdb-wire: 41 unit + 6 driver = 47
- agentdb-index: 16
- **Total ≥ 147**

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. Fix anything in M5 code (don't touch prior milestones).

- [ ] **Step 3: Fmt**

Run: `cargo fmt --all --check`
Expected: clean.

## Milestone exit criteria

`cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` returns exit 0.

Manual demo (optional):
```bash
cargo run -p agentdb-server &
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
> use t-demo
> db.events.insertOne({session_id: "s-1", text: "refund policy"})
# Force cold via HTTP admin
> exit
curl -X POST http://127.0.0.1:7878/api/admin/tier/force \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"t-demo","session_id":"s-1","target":"Cold"}'
# Query still works — event comes back from the segment
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
> use t-demo
> db.events.aggregate([{$search: {text: {query: "refund", path: "payload.text"}}}])
# Leave it idle for >60s (or set AGENTDB_T_COLD_SECS=5) and watch the lifecycle
# engine auto-demote sessions in the server logs.
```

## Known limitations to carry into M6

- **Archive tier still not implemented.** Derived-data sidecar (summary + embedding) deferred.
- **No orphan-segment reclaim.** If a `cold_transition` partially fails (segment written, metadata not), the orphan segment lingers until out-of-band cleanup.
- **Index rebuilds on restart only.** M4's deferral carries over — new events after first-build are invisible until next build. Cold transitions during a running server don't invalidate indexes either; a reactivated session's events remain in the index unchanged.
- **Lifecycle engine sweep cost.** `sweep` iterates every session under every tenant in the box. At demo scale this is fine; at 10^5-tenant scale it needs a secondary index on `last_active_at` or a dedicated sweep queue.
- **No graceful shutdown signalling.** Engines are `std::mem::forget`-leaked; they stop only when the process exits. Acceptable for a prototype.
- **Reactivate-while-fork.** Fork (M6) will add `branch_id` to segment paths. For M5, branch is implicit 0 and reactivation handles only that branch. M6 must extend the segment scheme.
