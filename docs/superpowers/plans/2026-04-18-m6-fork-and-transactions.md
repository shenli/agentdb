# AgentDB Prototype — Milestone 6: Session fork + stubbed Mongo transaction commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support the "spawn a subagent with a context snapshot" demo moment via `POST /api/sessions/{id}/fork`. Forked sessions share the parent's history up to a capture point (copy-on-write); new events in the child don't touch the parent. The hot-path `append_and_fetch` primitive returns a merged view (parent tail + child events) so the subagent sees the context it inherited. Also stub the three Mongo transaction commands so drivers that auto-wrap multi-write operations don't fail.

**Architecture:** Fork is represented as a **cross-session parent pointer**. A child session is a first-class new session (fresh `internal_id`, fresh `next_seq`, fresh key-prefix) with a `Session.forked_from: Option<ForkRef>` field recording `(parent_session_internal_id, parent_seq_at_fork)`. The in-session branch design from spec §9 is deferred — `branch_id` remains hard-coded to `DEFAULT_BRANCH_ID=0` so keys, indexes, and tiering from M1-M5 are untouched. Only `fetch_last_n` (the read primitive that feeds context back to agents) merges parent+child at query time; `scan_session_events`, index build, and the planner remain session-local. Transaction support is wire-protocol-only: `startTransaction` / `commitTransaction` / `abortTransaction` return `{ok: 1}` with no actual batching (single-doc atomicity covers the existing use cases).

**Tech Stack (M6 additions):** None. All changes are internal Rust code plus REST + Mongo wire handlers.

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §2.8 (fork), §9 (fork implementation), §8 (transactions), §7.3 (REST primitives)
- Previous milestones: M1-M5

---

## Scope

**In scope for M6:**
- `Session.forked_from: Option<ForkRef>` field (new) carrying `{parent_session_internal_id, parent_seq_at_fork}`
- `fork_session` primitive: look up parent by external name, capture its `next_seq`, mint a new child session under a new external name (same box and tenant as parent), persist the parent pointer
- `POST /api/sessions/{id}/fork` REST endpoint taking `{tenant_id, parent_session_id, new_session_id}` returning `{new_session_id, parent_session_id, parent_seq_at_fork}`
- `fetch_last_n` fork-aware — if the session is forked, merge parent events (up to `parent_seq_at_fork`, tier-aware) with child events, sort by timestamp descending, take the last N
- `append_and_fetch` unchanged in signature; its `recent` field now contains the merged view when called on a forked child
- Mongo transaction wire commands stubbed: `startTransaction`, `commitTransaction`, `abortTransaction` return `{ok: 1}` without tracking state
- Integration tests: fork via REST, child writes, child fetch_last_n includes parent tail; Mongo transaction wire commands accepted with ok:1 by the real driver

**Out of scope (explicitly deferred):**
- In-session branch chains (spec §9's `branch_id` model) — we keep `DEFAULT_BRANCH_ID=0` hard-coded. Future milestones may refactor.
- Indexing parent events from the child's perspective — `$search` / `$vectorSearch` on a forked child see only the child's own events. Parent events remain queryable via the parent session's external name.
- Real transaction semantics — the stubbed commands guarantee no atomicity beyond what's already given by single-doc writes. A driver that sends `insert` twice inside a transaction gets two independent atomic writes.
- Deep fork chains (fork of a fork). `forked_from` is a single-level pointer; reading grandparent events requires walking only one level. Deep chains would add recursion + cycle-safety and are not needed for the demo.
- Fork of a cold session — M6 requires the parent to be warm OR cold-with-readable-segment. The fork primitive uses `scan_session_events` (via `fetch_last_n`), which is tier-aware via M5's planner. Confirmed workable.
- Concurrent fork/append race — fork reads `parent.next_seq` without the parent's write lock. If an append lands concurrently, the new child may or may not inherit that event depending on the commit ordering; in the worst case a single event is missed. Acceptable for M6; documented.

**Concurrency contract:** `fork_session` acquires the box's write lock (forks go through the same serialization discipline as appends). It reads parent metadata, writes child metadata + lookup, all under the lock. `fetch_last_n` fork-merge is read-only; no lock needed.

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created:**
- `crates/agentdb-core/src/fork.rs` — `ForkRef` struct + `fork_session` primitive
- `crates/agentdb-api/src/fork.rs` — REST handler for `POST /api/sessions/{id}/fork`
- `crates/agentdb-wire/src/handlers/txn.rs` — stubbed transaction command handlers

**Modified:**
- `crates/agentdb-core/src/types.rs` — add `forked_from: Option<ForkRef>` to `Session`
- `crates/agentdb-core/src/registry.rs` — `SessionRegistry::get_or_mint_with_fork` (mint with `forked_from` set)
- `crates/agentdb-core/src/primitives.rs` — `fetch_last_n` merges parent events
- `crates/agentdb-core/src/lib.rs` — declare `fork` module, re-export types
- `crates/agentdb-api/src/lib.rs` — add `/api/sessions/:session_id/fork` route
- `crates/agentdb-wire/src/handlers/mod.rs` — `pub mod txn;`
- `crates/agentdb-wire/src/command.rs` — dispatch transaction commands to `txn.rs`

**Responsibilities:**
- `fork.rs` (core) — pure primitive; no HTTP.
- `fork.rs` (api) — thin HTTP adapter; routes to `fork_session`.
- `txn.rs` — trivial stubs; no real transaction state.
- `primitives.rs::fetch_last_n` — gains an optional parent-merge branch guarded by `session.forked_from.is_some()`.

---

## Data model notes

### ForkRef

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkRef {
    /// Parent session's *internal* id. Same tenant, same box as the child.
    pub parent_session_internal_id: u64,
    /// Parent's `next_seq` at the moment of fork. The child sees parent events
    /// with `seq < parent_seq_at_fork`.
    pub parent_seq_at_fork: u64,
}
```

`parent_session_internal_id` is stable across process restarts (minted once by the parent's get_or_mint). The child references it by value, not by name, so renaming the parent's external id doesn't break the fork.

### Session additions

```rust
#[serde(default)]
pub forked_from: Option<ForkRef>,
```

Backward-compatible via `serde(default)` — existing Session records on disk deserialize with `forked_from = None`.

### fetch_last_n merge semantics

For a non-forked session: unchanged (return the last N events of this session, tier-aware via M5).

For a forked session with `forked_from = Some(ref)`:
1. Read all child events (via existing `scan_session_events` — tier-aware).
2. Read parent events with `seq < ref.parent_seq_at_fork` (via `scan_session_events` against a parent Session record we look up). This includes parent events that existed when the fork happened and excludes anything written to the parent AFTER the fork.
3. Concatenate, sort by `ts` (timestamp) descending, take the last N (that is, the N most recent by ts after sort).
4. Return in ascending-by-ts order for caller ergonomics (the existing contract of fetch_last_n).

No recursive walk up a fork chain. If the parent is itself forked, we do **not** walk further — parent events inherited into child-of-parent are not inherited into grandchild. The spec's deep-chain model is explicit out-of-scope for M6; a future milestone can swap in a recursive walk.

### Edge cases

- Parent doesn't exist (typo in `parent_session_id`) → fork returns 400 (do NOT mint).
- Child external_id already exists as a non-forked session → fork returns 400 (would conflict).
- Child external_id already exists AS a fork → fork returns 400 (idempotent re-fork not supported in M6).
- Parent is cold or archive → fork still works for the metadata; `fetch_last_n` reads through the tier-aware pipeline.

---

## Task 1: `ForkRef` struct + `Session.forked_from` field

**Files:**
- Modify: `crates/agentdb-core/src/types.rs`

- [ ] **Step 1: Write failing test**

Append inside the existing `#[cfg(test)] mod tests` in `crates/agentdb-core/src/types.rs`:
```rust
    #[test]
    fn session_with_forked_from_roundtrips_through_json() {
        let s = Session {
            internal_id: 10,
            external_id: "s-child".into(),
            tenant_internal_id: 3,
            box_id: 1,
            created_at: 100,
            last_active_at: 150,
            tier: Tier::Warm,
            next_seq: 1,
            segment_path: None,
            forked_from: Some(ForkRef {
                parent_session_internal_id: 9,
                parent_seq_at_fork: 42,
            }),
        };
        let text = serde_json::to_string(&s).unwrap();
        let back: Session = serde_json::from_str(&text).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn session_without_forked_from_deserializes_from_old_record() {
        // Old record from M5: no forked_from field.
        let text = r#"{
            "internal_id": 5,
            "external_id": "s-1",
            "tenant_internal_id": 1,
            "box_id": 1,
            "created_at": 0,
            "last_active_at": 0,
            "tier": "warm",
            "next_seq": 1,
            "segment_path": null
        }"#;
        let s: Session = serde_json::from_str(text).unwrap();
        assert!(s.forked_from.is_none());
    }
```

- [ ] **Step 2: Run (expect compile failure — `ForkRef` missing)**

Run: `cargo test -p agentdb-core types::tests -- --nocapture`
Expected: FAIL.

- [ ] **Step 3: Add `ForkRef` and field**

In `crates/agentdb-core/src/types.rs`, add the `ForkRef` struct (placement: near the other serde types, before `Session`):
```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkRef {
    /// Parent session's *internal* id (same tenant, same box as the child).
    pub parent_session_internal_id: u64,
    /// Parent's `next_seq` at the moment of fork. The child sees parent events
    /// with `seq < parent_seq_at_fork`.
    pub parent_seq_at_fork: u64,
}
```

Extend `Session`:
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
    #[serde(default)]
    pub segment_path: Option<String>,
    #[serde(default)]
    pub forked_from: Option<ForkRef>,
}
```

Update **every** `Session { ... }` literal in the workspace to add `forked_from: None`:
- `crates/agentdb-core/src/registry.rs` — `SessionRegistry::get_or_mint` (add `forked_from: None`)
- `crates/agentdb-core/src/primitives.rs::tests` — any inline `Session` construction
- `crates/agentdb-core/src/types.rs::tests` — existing `Session` literal in `session_roundtrips_through_json` (leave `forked_from: None` explicit)

Use Grep to find them all.

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core types::tests`
Expected: 7 passed (5 from before + 2 new).

Run: `cargo test --workspace` — 147 + 2 = 149 tests pass (no regressions).

- [ ] **Step 5: Re-export `ForkRef` from lib.rs**

Edit `crates/agentdb-core/src/lib.rs`. Update the types re-export:
```rust
pub use types::{Document, Event, ForkRef, Session, Tenant, Tier};
```

Run: `cargo check --workspace` — clean.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core
git commit -m "feat(core): ForkRef type and Session.forked_from field (backward-compat via serde default)"
```

---

## Task 2: `SessionRegistry::get_or_mint_with_fork`

**Files:**
- Modify: `crates/agentdb-core/src/registry.rs`

**Context:** The existing `get_or_mint` always sets `forked_from: None`. The fork primitive needs a variant that mints with a supplied `ForkRef`. Keep `get_or_mint` as-is (forwards to the new function with `None`) to avoid breaking call sites.

- [ ] **Step 1: Write failing test**

Append inside `#[cfg(test)] mod tests` in `crates/agentdb-core/src/registry.rs`:
```rust
    #[test]
    fn get_or_mint_with_fork_stores_parent_pointer() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: 7,
            parent_seq_at_fork: 42,
        };
        let s = reg
            .get_or_mint_with_fork(1, 10, "child", 100, Some(fork_ref.clone()))
            .unwrap();
        assert_eq!(s.forked_from, Some(fork_ref));
    }

    #[test]
    fn get_or_mint_is_unchanged_for_non_forked_session() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let s = reg.get_or_mint(1, 10, "plain", 100).unwrap();
        assert!(s.forked_from.is_none());
    }
```

- [ ] **Step 2: Run (expect compile failure)**

Run: `cargo test -p agentdb-core registry::tests -- --nocapture`
Expected: FAIL — `get_or_mint_with_fork` undefined.

- [ ] **Step 3: Implement**

In `crates/agentdb-core/src/registry.rs`, inside `impl SessionRegistry`, add `get_or_mint_with_fork` and refactor `get_or_mint` to forward:

```rust
pub fn get_or_mint(
    &self,
    box_id: BoxId,
    tenant_id: TenantId,
    external: &str,
    now_millis: u64,
) -> Result<Session> {
    self.get_or_mint_with_fork(box_id, tenant_id, external, now_millis, None)
}

pub fn get_or_mint_with_fork(
    &self,
    box_id: BoxId,
    tenant_id: TenantId,
    external: &str,
    now_millis: u64,
    forked_from: Option<crate::types::ForkRef>,
) -> Result<Session> {
    if let Some(s) = self.lookup(box_id, tenant_id, external)? {
        return Ok(s);
    }
    let next_key = key_next_session_id(box_id, tenant_id);
    let current: SessionId = match self.kv.get(&next_key)? {
        Some(b) => serde_json::from_slice(&b)?,
        None => 1,
    };
    let session = Session {
        internal_id: current,
        external_id: external.to_string(),
        tenant_internal_id: tenant_id,
        box_id,
        created_at: now_millis,
        last_active_at: now_millis,
        tier: Tier::Warm,
        next_seq: 1,
        segment_path: None,
        forked_from,
    };
    let session_bytes = serde_json::to_vec(&session)?;
    let lookup_bytes = serde_json::to_vec(&session.internal_id)?;
    let next_bytes = serde_json::to_vec(&(current + 1))?;
    self.kv.write_batch(vec![
        WriteOp::Put {
            key: key_session_by_external(box_id, tenant_id, external),
            value: lookup_bytes,
        },
        WriteOp::Put {
            key: key_session_record(box_id, tenant_id, current),
            value: session_bytes,
        },
        WriteOp::Put {
            key: next_key,
            value: next_bytes,
        },
    ])?;
    Ok(session)
}
```

The existing `get_or_mint` body is replaced by the one-line delegation above. Make sure you actually delete the old body and don't leave both definitions.

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-core registry::tests`
Expected: all registry tests pass (previous + 2 new = more than before).

Run: `cargo test --workspace` — all tests pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/registry.rs
git commit -m "feat(core): SessionRegistry::get_or_mint_with_fork"
```

---

## Task 3: `fork_session` primitive

**Files:**
- Create: `crates/agentdb-core/src/fork.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Write fork.rs**

Save `crates/agentdb-core/src/fork.rs`:
```rust
use std::sync::Arc;

use tokio::sync::MutexGuard;

use crate::box_runtime::BoxRegistry;
use crate::error::{Error, Result};
use crate::registry::{SessionRegistry, TenantRegistry};
use crate::types::{ForkRef, Session};

/// Create a new session whose history begins as a copy-on-write snapshot of
/// the parent's events at `parent.next_seq - 1` (inclusive).
///
/// - Must be called while holding the box write lock (demonstrated by the
///   `MutexGuard` reference).
/// - Parent must exist. Child's external id must be free.
/// - Both sessions live under the same tenant/box as the parent; fork never
///   crosses box boundaries.
#[allow(clippy::too_many_arguments)]
pub fn fork_session(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    _lock: &MutexGuard<'_, ()>,
    tenant_external: &str,
    parent_session_external: &str,
    new_session_external: &str,
    now_millis: u64,
) -> Result<Session> {
    let box_ = registry.route_tenant(tenant_external);
    let tenant = tenants
        .lookup(box_.id, tenant_external)?
        .ok_or_else(|| Error::NotFound(format!("tenant {tenant_external}")))?;
    let parent = sessions
        .lookup(box_.id, tenant.internal_id, parent_session_external)?
        .ok_or_else(|| {
            Error::NotFound(format!("parent session {parent_session_external}"))
        })?;

    if sessions
        .lookup(box_.id, tenant.internal_id, new_session_external)?
        .is_some()
    {
        return Err(Error::Invalid(format!(
            "target session {new_session_external} already exists"
        )));
    }

    let fork_ref = ForkRef {
        parent_session_internal_id: parent.internal_id,
        parent_seq_at_fork: parent.next_seq,
    };
    let child = sessions.get_or_mint_with_fork(
        box_.id,
        tenant.internal_id,
        new_session_external,
        now_millis,
        Some(fork_ref),
    )?;
    // kv isn't directly used here — borrow-silencer for when the fn signature
    // later grows (e.g., to persist a fork-lineage record).
    let _ = registry.kv();
    Ok(child)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde_json::json;
    use tempfile::TempDir;

    async fn ctx() -> (
        TempDir,
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (kv_dir, obj_dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn fork_creates_child_with_parent_pointer() {
        let (_k, _o, registry, tenants, sessions) = ctx().await;
        for i in 1..=3u64 {
            append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "parent",
                json!({"i": i}),
                0,
                i * 10,
            )
            .await
            .unwrap();
        }
        let box_ = registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let child = fork_session(
            &registry,
            &tenants,
            &sessions,
            &guard,
            "t-1",
            "parent",
            "child",
            1000,
        )
        .unwrap();
        drop(guard);

        assert!(child.forked_from.is_some());
        let fork = child.forked_from.as_ref().unwrap();
        // Parent had 3 events, so next_seq at fork time was 4.
        assert_eq!(fork.parent_seq_at_fork, 4);
        assert_eq!(child.next_seq, 1);
    }

    #[tokio::test]
    async fn fork_with_missing_parent_errors() {
        let (_k, _o, registry, tenants, sessions) = ctx().await;
        // Pre-mint the tenant so the tenant-not-found case doesn't fire first.
        tenants
            .get_or_mint(registry.route_tenant("t-1").id, "t-1", 0)
            .unwrap();
        let box_ = registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let err = fork_session(
            &registry,
            &tenants,
            &sessions,
            &guard,
            "t-1",
            "nope",
            "child",
            0,
        )
        .unwrap_err();
        assert!(err.to_string().contains("parent session"));
    }

    #[tokio::test]
    async fn fork_with_existing_child_errors() {
        let (_k, _o, registry, tenants, sessions) = ctx().await;
        append_and_fetch(
            &registry, &tenants, &sessions, "t-1", "parent",
            json!({}), 0, 1,
        )
        .await
        .unwrap();
        append_and_fetch(
            &registry, &tenants, &sessions, "t-1", "taken",
            json!({}), 0, 1,
        )
        .await
        .unwrap();
        let box_ = registry.route_tenant("t-1");
        let guard = box_.write_lock.lock().await;
        let err = fork_session(
            &registry, &tenants, &sessions, &guard,
            "t-1", "parent", "taken", 0,
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
```

- [ ] **Step 2: Declare in lib.rs**

Edit `crates/agentdb-core/src/lib.rs`. Add:
```rust
pub mod fork;
```

And re-export:
```rust
pub use fork::fork_session;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p agentdb-core fork::tests`
Expected: 3 passed.

Run: `cargo test --workspace` — all pass.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/fork.rs crates/agentdb-core/src/lib.rs
git commit -m "feat(core): fork_session primitive with cross-session parent pointer"
```

---

## Task 4: fork-aware `fetch_last_n`

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`

**Context:** When a session has `forked_from = Some(ref)`, `fetch_last_n` merges its own events with parent events (up to the fork seq), sorts by `ts` descending, and returns the last N in ascending-ts order. For non-forked sessions the behavior is unchanged.

Parent events are read via `scan_session_events` (M5 made this tier-aware, so cold/warm both work).

- [ ] **Step 1: Write failing test**

Append inside `#[cfg(test)] mod tests` in `crates/agentdb-core/src/primitives.rs`:
```rust
    #[tokio::test]
    async fn fetch_last_n_merged_on_forked_session_pulls_parent_tail() {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;

        let (_dir, kv, tenants, sessions) = setup();
        let obj_dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let lock = tokio::sync::Mutex::new(());
        let t = tenants.get_or_mint(0, "t", 0).unwrap();

        // Parent: 3 events with ts 10, 20, 30.
        for i in 1..=3u64 {
            open_and_append(
                kv.clone(),
                &sessions,
                &lock,
                0,
                t.internal_id,
                "parent",
                json!({"i": i}),
                i * 10,
            )
            .await
            .unwrap();
        }

        // Fork: child with parent_seq_at_fork = 4.
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: sessions
                .lookup(0, t.internal_id, "parent")
                .unwrap()
                .unwrap()
                .internal_id,
            parent_seq_at_fork: 4,
        };
        let _child = sessions
            .get_or_mint_with_fork(0, t.internal_id, "child", 1000, Some(fork_ref))
            .unwrap();

        // Child: 2 events with ts 100, 110.
        open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "child",
            json!({"c": 1}),
            100,
        )
        .await
        .unwrap();
        open_and_append(
            kv.clone(),
            &sessions,
            &lock,
            0,
            t.internal_id,
            "child",
            json!({"c": 2}),
            110,
        )
        .await
        .unwrap();

        // Events by ts: parent [10, 20, 30] + child [100, 110]. Last 4 → [20, 30, 100, 110].
        let reloaded = sessions.lookup(0, t.internal_id, "child").unwrap().unwrap();
        let got = fetch_last_n_merged(&kv, &store, &sessions, &reloaded, 4)
            .await
            .unwrap();
        let tss: Vec<u64> = got.iter().map(|e| e.ts).collect();
        assert_eq!(tss, vec![20, 30, 100, 110]);

        let _ = obj_dir; // keep alive for the duration of the test
    }
```

- [ ] **Step 2: Run test (expect compile failure — `fetch_last_n_merged` undefined)**

Run: `cargo test -p agentdb-core primitives::tests::fetch_last_n_merged_on_forked_session_pulls_parent_tail`
Expected: FAIL.

- [ ] **Step 3: Keep `fetch_last_n` as-is, add a new async fork+tier-aware helper**

Leave the existing `fetch_last_n(&Arc<dyn KvStore>, &Session, usize) -> Result<Vec<Event>>` unchanged. Add a new async helper `fetch_last_n_merged` alongside it in `crates/agentdb-core/src/primitives.rs`. The new helper:
- Takes an `&Arc<dyn ObjectStore>` so it can read cold parent segments via M5's `read_segment`.
- Takes `&SessionRegistry` so it can `lookup_by_internal` the parent.
- Reads own events via the existing `fetch_last_n` (fine regardless of tier: if this session is cold, we've either reactivated or are mid-transition; the regular `fetch_last_n` is still session-local).

Append to `crates/agentdb-core/src/primitives.rs`:
```rust
use object_store::ObjectStore;

/// Fork+tier-aware version of `fetch_last_n`.
///
/// Non-forked session: identical to `fetch_last_n`.
/// Forked session: merges parent events (with `seq < parent_seq_at_fork`)
/// read from the parent's tier-appropriate location (warm cf_events, cold
/// segment, archive → empty), sorts by `ts` ascending, truncates to `n`.
pub async fn fetch_last_n_merged(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    sessions: &SessionRegistry,
    session: &Session,
    n: usize,
) -> Result<Vec<Event>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let own = fetch_last_n(kv, session, n)?;
    let Some(fork) = session.forked_from.as_ref() else {
        return Ok(own);
    };
    let Some(parent) = sessions.lookup_by_internal(
        session.box_id,
        session.tenant_internal_id,
        fork.parent_session_internal_id,
    )?
    else {
        return Ok(own); // parent deleted — degrade gracefully to child-only
    };
    let parent_events: Vec<Event> = match parent.tier {
        crate::types::Tier::Warm => scan_session_events(kv, &parent)?,
        crate::types::Tier::Cold => {
            let Some(path) = parent.segment_path.as_deref() else {
                Vec::new()
            };
            match path {
                _ if parent.segment_path.is_none() => Vec::new(),
                path => match crate::segment::read_segment(store, path).await {
                    Ok(seg) => seg.events,
                    Err(_) => Vec::new(),
                },
            }
        }
        crate::types::Tier::Archive => Vec::new(),
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

(Note: the `match path` construct above is a bit awkward because of how Rust's pattern matching interacts with `let-else`. If the compiler complains, fall back to the straightforward:
```rust
let parent_events: Vec<Event> = match parent.tier {
    crate::types::Tier::Warm => scan_session_events(kv, &parent)?,
    crate::types::Tier::Cold => {
        if let Some(path) = parent.segment_path.as_deref() {
            match crate::segment::read_segment(store, path).await {
                Ok(seg) => seg.events,
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        }
    }
    crate::types::Tier::Archive => Vec::new(),
};
```
)

Re-export the new helper from `lib.rs`:
```rust
pub use primitives::{
    append_and_fetch, append_event, fetch_last_n, fetch_last_n_merged, scan_session_events,
    AppendFetchOutput, DEFAULT_BRANCH_ID,
};
```

- [ ] **Step 4: Update `append_and_fetch` to use `fetch_last_n_merged`**

In `crates/agentdb-core/src/primitives.rs::append_and_fetch`, replace:
```rust
let recent = fetch_last_n(&kv, &session, fetch_last_n_)?;
```
with:
```rust
let recent = fetch_last_n_merged(
    &kv,
    registry.object_store(),
    sessions,
    &session,
    fetch_last_n_,
)
.await?;
```

(The existing `append_and_fetch` already has `&registry` and `sessions` in scope.)

- [ ] **Step 5: Adjust the new fork test to use `fetch_last_n_merged`**

Update the fork test from Step 1 to call `fetch_last_n_merged(&kv, &store, &sessions, &reloaded, 4).await.unwrap()` (the test's `setup_with_store` helper already constructs a store; adjust if needed).

The existing `fetch_last_n_*` tests in `primitives::tests` are unchanged — they continue to call the synchronous `fetch_last_n(&kv, &session, n)?` and exercise the non-forked path.

No external callers of `fetch_last_n` exist outside agentdb-core (verified via Grep). Only the `append_and_fetch` call site switches to `fetch_last_n_merged`.

- [ ] **Step 6: Run tests**

Run: `cargo test -p agentdb-core primitives::tests`
Expected: 9 passed (8 from prior milestones + 1 new fork test).

Run: `cargo test --workspace` — all pass.

- [ ] **Step 7: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core/src/primitives.rs
git commit -m "feat(core): fetch_last_n merges parent tail for forked sessions"
```

---

## Task 5: `POST /api/sessions/{session_id}/fork` REST endpoint

**Files:**
- Create: `crates/agentdb-api/src/fork.rs`
- Modify: `crates/agentdb-api/src/lib.rs`

**Context:** Route takes a path-scoped `session_id` (the parent) and a body:
```json
{"tenant_id": "t-1", "new_session_id": "s-child"}
```
Returns:
```json
{"new_session_id": "s-child", "parent_session_id": "s-parent", "parent_seq_at_fork": 42}
```

(The REST shape in the spec §7.3 shows the parent's id in the URL; we match that.)

- [ ] **Step 1: Write handler**

Save `crates/agentdb-api/src/fork.rs`:
```rust
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::fork::fork_session;
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct ForkRequest {
    pub tenant_id: String,
    pub new_session_id: String,
}

#[derive(Serialize)]
pub struct ForkResponse {
    pub new_session_id: String,
    pub parent_session_id: String,
    pub parent_seq_at_fork: u64,
}

pub async fn fork_handler(
    State(state): State<AppState>,
    Path(parent_session_id): Path<String>,
    Json(body): Json<ForkRequest>,
) -> Result<Json<ForkResponse>, ApiError> {
    let box_ = state.registry.route_tenant(&body.tenant_id);
    let guard = box_.write_lock.lock().await;
    let child = fork_session(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &guard,
        &body.tenant_id,
        &parent_session_id,
        &body.new_session_id,
        now_millis(),
    )?;
    drop(guard);
    let fork = child.forked_from.expect("fork_session always sets forked_from");
    Ok(Json(ForkResponse {
        new_session_id: child.external_id,
        parent_session_id,
        parent_seq_at_fork: fork.parent_seq_at_fork,
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

Edit `crates/agentdb-api/src/lib.rs`. Add `pub mod fork;` alongside other module declarations. In `router()`, add:
```rust
.route(
    "/api/sessions/:session_id/fork",
    axum::routing::post(fork::fork_handler),
)
```

- [ ] **Step 3: Integration test**

Append to `crates/agentdb-api/tests/http.rs`:
```rust
#[tokio::test]
#[serial]
async fn fork_endpoint_creates_child_and_fetch_last_n_merges_parent_tail() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Seed parent with 2 events.
    for i in 1..=2u64 {
        client
            .post(format!(
                "{base}/api/tenants/t-1/sessions/parent/append_and_fetch"
            ))
            .json(&serde_json::json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // Fork.
    let fork: serde_json::Value = client
        .post(format!("{base}/api/sessions/parent/fork"))
        .json(&serde_json::json!({
            "tenant_id": "t-1",
            "new_session_id": "child"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fork["new_session_id"], "child");
    assert_eq!(fork["parent_session_id"], "parent");
    assert_eq!(fork["parent_seq_at_fork"], 3);

    // Append to child, request fetch_last_n=5 — should include parent tail + child event.
    let resp: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/child/append_and_fetch"
        ))
        .json(&serde_json::json!({"event": {"c": 1}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let recent = resp["recent"].as_array().unwrap();
    // Expected: 2 parent events + 1 new child event = 3 total.
    assert_eq!(recent.len(), 3);
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-api --test http`
Expected: 4 passed (3 prior + 1 new).

Run: `cargo test --workspace` — all pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-api
git commit -m "feat(api): POST /api/sessions/:session_id/fork"
```

---

## Task 6: Stub Mongo transaction commands

**Files:**
- Create: `crates/agentdb-wire/src/handlers/txn.rs`
- Modify: `crates/agentdb-wire/src/handlers/mod.rs`
- Modify: `crates/agentdb-wire/src/command.rs`

**Context:** The Mongo driver's session/transaction helpers send `startTransaction`, `commitTransaction`, `abortTransaction` commands. We accept them with `{ok: 1}` but implement no real transaction state. Single-doc writes are already atomic; multi-doc wrap-per-transaction is lost, which is acceptable for the prototype.

- [ ] **Step 1: Write txn.rs**

Save `crates/agentdb-wire/src/handlers/txn.rs`:
```rust
use bson::{doc, Document};

/// `startTransaction` and `commitTransaction` both return `{ok: 1}`. No
/// transaction state is tracked — the caller gets the same atomicity as
/// single-doc writes would provide on their own.
pub fn start_transaction_response() -> Document {
    doc! { "ok": 1.0 }
}

pub fn commit_transaction_response() -> Document {
    doc! { "ok": 1.0 }
}

pub fn abort_transaction_response() -> Document {
    doc! { "ok": 1.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_returns_ok_one() {
        assert_eq!(start_transaction_response().get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn commit_returns_ok_one() {
        assert_eq!(commit_transaction_response().get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn abort_returns_ok_one() {
        assert_eq!(abort_transaction_response().get_f64("ok").unwrap(), 1.0);
    }
}
```

- [ ] **Step 2: Declare in mod.rs**

Edit `crates/agentdb-wire/src/handlers/mod.rs`. Add `pub mod txn;`.

- [ ] **Step 3: Route the commands in `dispatch`**

Edit `crates/agentdb-wire/src/command.rs`. In the `dispatch` function's match, add these arms (place them after `"insert"` and before the `other =>` catch-all, or in alphabetical order):
```rust
"startTransaction" => crate::handlers::txn::start_transaction_response(),
"commitTransaction" => crate::handlers::txn::commit_transaction_response(),
"abortTransaction" => crate::handlers::txn::abort_transaction_response(),
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p agentdb-wire handlers::txn::tests`
Expected: 3 passed.

Run: `cargo test --workspace` — all pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire
git commit -m "feat(wire): stub startTransaction/commitTransaction/abortTransaction as no-op"
```

---

## Task 7: End-to-end fork test via the `mongodb` Rust driver + transaction wire smoke

**Files:**
- Modify: `crates/agentdb-wire/tests/driver.rs`

**Context:** Two tests:
1. Fork flow: seed parent via the mongodb driver, fork via REST, insert on child via driver, verify fetch_last_n merges (query child through mongo and check events come back).
2. Transaction wire smoke: run `startTransaction` + `commitTransaction` via `run_command` and assert ok:1.

- [ ] **Step 1: Append the tests**

Append at the bottom of `crates/agentdb-wire/tests/driver.rs`:
```rust
#[tokio::test]
async fn fork_child_merges_parent_tail_via_http_append_and_fetch() {
    let (_kv, _obj, mongo_port, http_port) = spawn_with_http().await;
    let cli = client(mongo_port).await;
    let db = cli.database("t-fork");
    let events = db.collection::<bson::Document>("events");

    // Seed the parent with 2 events via the driver.
    events
        .insert_one(doc! { "session_id": "parent", "i": 1 })
        .await
        .unwrap();
    events
        .insert_one(doc! { "session_id": "parent", "i": 2 })
        .await
        .unwrap();

    // Fork via REST.
    let rest = reqwest::Client::new();
    let fork: serde_json::Value = rest
        .post(format!("http://127.0.0.1:{http_port}/api/sessions/parent/fork"))
        .json(&serde_json::json!({
            "tenant_id": "t-fork",
            "new_session_id": "child"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fork["parent_seq_at_fork"], 3);

    // Child appends, requests fetch_last_n=5 via HTTP REST (the mongodb driver
    // doesn't invoke append_and_fetch; we use the REST endpoint directly).
    let resp: serde_json::Value = rest
        .post(format!(
            "http://127.0.0.1:{http_port}/api/tenants/t-fork/sessions/child/append_and_fetch"
        ))
        .json(&serde_json::json!({"event": {"c": 1}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["recent"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn transaction_wire_commands_return_ok_one() {
    let (_dir, port) = spawn().await;
    let cli = client(port).await;
    let admin = cli.database("admin");
    let started = admin
        .run_command(doc! {
            "startTransaction": 1,
            "lsid": { "id": bson::oid::ObjectId::new() },
            "txnNumber": 1_i64,
            "autocommit": false,
        })
        .await
        .unwrap();
    assert_eq!(started.get_f64("ok").unwrap(), 1.0);

    let committed = admin
        .run_command(doc! {
            "commitTransaction": 1,
            "lsid": { "id": bson::oid::ObjectId::new() },
            "txnNumber": 1_i64,
        })
        .await
        .unwrap();
    assert_eq!(committed.get_f64("ok").unwrap(), 1.0);
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p agentdb-wire --test driver`
Expected: 8 passed (6 prior + 2 new).

Run: `cargo test --workspace` — all pass.

- [ ] **Step 3: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-wire/tests/driver.rs
git commit -m "test(wire): end-to-end fork via REST + mongo transaction wire stubs"
```

---

## Task 8: Milestone exit gate

**Files:** none beyond re-running checks.

- [ ] **Step 1: Full workspace test**

Run: `cargo test --workspace`
Expected: all tests pass. Minimum counts:
- agentdb-storage: 17 + 11 = 28
- agentdb-core: 53 + 2 (types fork) + 2 (registry fork) + 3 (fork.rs) + 1 (primitives fork) = 61
- agentdb-api: 3 + 1 (fork http) = 4
- agentdb-wire: 47 unit + 3 (txn) = 50 unit, 6 driver + 2 new = 8 driver
- agentdb-index: 16
- **Total ≥ 167**

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

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
> db.events.insertOne({session_id: "agent", text: "user asks for a refund"})
> db.events.insertOne({session_id: "agent", text: "agent replies: checking account"})
> exit

# Fork the agent's session to spawn a subagent:
curl -X POST http://127.0.0.1:7878/api/sessions/agent/fork \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"t-demo","new_session_id":"subagent"}'

# Subagent appends, gets parent context back via fetch_last_n:
curl -X POST http://127.0.0.1:7878/api/tenants/t-demo/sessions/subagent/append_and_fetch \
  -H 'Content-Type: application/json' \
  -d '{"event":{"text":"subagent: looking up account"}, "fetch_last_n": 10}'
# The `recent` array will contain parent's 2 events AND the subagent's new one.
```

## Known limitations (tracked for later)

- **Single-level fork only.** Fork-of-fork inherits only the immediate parent's events, not the grandparent's.
- **Fork + cold tier:** if the parent is cold, `fetch_last_n` will read its segment from object_store each time a forked child fetches. No caching in M6.
- **No "unfork" or "merge back"** — the child's events never propagate back to the parent.
- **Concurrent fork/append race:** fork captures `parent.next_seq` without forcing a lock on the parent's writes. A racing append may or may not be included in the child's inherited tail.
- **`$search` and `$vectorSearch` on a forked child return child-only results.** Parent events remain in the parent's index scope.
- **Real Mongo transactions not implemented.** The three wire commands return `ok: 1` without any cross-operation atomicity; single-doc writes are still atomic as before.
- **`branch_id` remains hard-coded to 0 in the key layout.** The in-session-branches model from spec §9 is deferred.
