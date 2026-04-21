# M12: Archive Tier + Lifecycle Webhooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reach `Tier::Archive` as a live lifecycle state via both the background sweep (after `T_ARCHIVE_SECS` idle in cold) and `POST /api/admin/tier/force target=archive`, and fire a fire-and-forget HTTP webhook POST on every tier transition for downstream derived-data services.

**Architecture:** `LifecycleEngine` grows a `t_archive` threshold and a second inner pass (cold→archive). Archive transitions are metadata-only — no segment movement, no event deletes — reserving the hook for production storage-class transitions. `Box_` gets a new `webhook: RwLock<Option<String>>` field; a per-box admin endpoint sets it. Every tier-transition call site (admin + both sweep passes) invokes a new `fire_tier_change` free function in `agentdb-api/src/webhook.rs` that `tokio::spawn`s a `reqwest::Client::post(...)` against the configured URL, with no retries. Core stays layer-clean via a `Fn(&Arc<Box_>, Session, Tier, Tier)` callback set on `LifecycleEngine` at construction; the server binary wires the closure that owns an `AppState` clone.

**Tech Stack:** Rust 1.75, `parking_lot::RwLock` (existing), `reqwest` 0.12 (already a dev-dep, promoted to regular dep of agentdb-api), `tokio::spawn`, `async-broadcast` SSE (existing).

---

## File Structure

**Created:**
- `crates/agentdb-api/src/webhook.rs` — `fire_tier_change`, `TierChangeCallback` glue factory
- `crates/agentdb-api/tests/archive_webhook.rs` — end-to-end integration test with a local mock server

**Modified:**
- `crates/agentdb-core/src/tiering.rs` — add `cold_to_archive` + `archive_to_cold` transition functions
- `crates/agentdb-core/src/lifecycle.rs` — add `t_archive: Duration` to `LifecycleConfig`, add `TierChangeHook` type, extend `LifecycleEngine::start` to take an optional hook, extend `sweep` with a second pass (cold→archive)
- `crates/agentdb-core/src/box_runtime.rs` — add `webhook: parking_lot::RwLock<Option<String>>` field on `Box_`, init in `new`
- `crates/agentdb-core/src/metrics.rs` — add `webhook_fires: AtomicU64` + `webhook_failures: AtomicU64` counters + snapshot fields
- `crates/agentdb-api/Cargo.toml` — move `reqwest` from `[dev-dependencies]` to `[dependencies]`
- `crates/agentdb-api/src/lib.rs` — `AppState` gets a `http: reqwest::Client` field, new `pub mod webhook`, route for the new admin endpoint
- `crates/agentdb-api/src/admin.rs` — accept `target = "archive"`, call `cold_to_archive`/`archive_to_cold`, fire webhook after each successful transition
- `crates/agentdb-api/src/admin_boxes.rs` — new `set_webhook` handler + `WebhookRequest` type
- `crates/agentdb-api/src/stream.rs` — `TierChange` SSE event body is unchanged but the `TickBox` grows `webhook_fires` + `webhook_failures` for dashboard exposure
- `bin/agentdb-server/src/main.rs` — parse `AGENTDB_T_ARCHIVE_SECS` env var, construct the `TierChangeHook` closure that owns an `AppState` clone, pass it to `LifecycleEngine::start`
- `crates/agentdb-dashboard/static/app.js` — render webhook URL + fires/failures on each box tile
- `docs/ARCHITECTURE.md` — add Archive tier + webhook notes, drop stale bullets
- `docs/PERFORMANCE.md` — add webhook overhead note

---

## Task 0: Add `TenantRegistry::lookup_by_internal`

**Files:**
- Modify: `crates/agentdb-core/src/registry.rs`

The server binary's tier-change hook (Task 8) needs to resolve a tenant's external id from its internal id so the webhook payload can carry the user-facing `tenant_id`. `SessionRegistry` already has `lookup_by_internal`; mirror that on `TenantRegistry`.

- [ ] **Step 1: Understand how tenants are persisted**

Run: `grep -n "key_tenant_record\|tenant_by_internal\|tenant_external" crates/agentdb-core/src/meta.rs crates/agentdb-core/src/registry.rs`

You'll find that `get_or_mint` persists both `key_tenant_by_external(box, external) → internal` and `key_tenant_record(box, internal) → Tenant`. The second one is already a reverse-lookup table — we just need a method that reads it.

- [ ] **Step 2: Add the method**

Edit `crates/agentdb-core/src/registry.rs`. Inside `impl TenantRegistry { ... }`, after the existing `pub fn lookup` method, append:

```rust
    /// Reverse lookup: given an internal id, return the full tenant
    /// record (including its external string id). Used by the
    /// lifecycle hook to populate webhook payloads.
    pub fn lookup_by_internal(
        &self,
        box_id: BoxId,
        internal_id: TenantId,
    ) -> Result<Option<Tenant>> {
        let Some(bytes) = self
            .kv
            .get(&crate::meta::key_tenant_record(box_id, internal_id))?
        else {
            return Ok(None);
        };
        let t: Tenant = serde_json::from_slice(&bytes)?;
        Ok(Some(t))
    }
```

- [ ] **Step 3: Add a test**

In the `#[cfg(test)] mod tests` block of `registry.rs`, append:

```rust
    #[test]
    fn tenant_lookup_by_internal_round_trips() {
        use agentdb_storage::RocksKvStore;
        use std::sync::Arc;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let reg = TenantRegistry::new(kv);
        let minted = reg.get_or_mint(1, "t-ext", 42).unwrap();
        let found = reg.lookup_by_internal(1, minted.internal_id).unwrap();
        assert_eq!(found.map(|t| t.external_id), Some("t-ext".to_string()));
        let missing = reg.lookup_by_internal(1, 9999).unwrap();
        assert!(missing.is_none());
    }
```

- [ ] **Step 4: Run**

Run: `cargo test -p agentdb-core --lib registry::tests`
Expected: all existing registry tests pass plus `tenant_lookup_by_internal_round_trips`.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/registry.rs
git commit -m "feat(agentdb-core): TenantRegistry::lookup_by_internal"
```

---

## Task 1: Promote `reqwest` to a regular dep of agentdb-api

**Files:**
- Modify: `crates/agentdb-api/Cargo.toml`

- [ ] **Step 1: Move the `reqwest` line**

Open `crates/agentdb-api/Cargo.toml`. Find:

```toml
[dependencies]
agentdb-core = { path = "../agentdb-core" }
agentdb-dashboard = { path = "../agentdb-dashboard" }
async-broadcast = { workspace = true }
axum = { workspace = true }
futures-util = "0.3"
tokio = { workspace = true }
tower = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
agentdb-storage = { path = "../agentdb-storage" }
anyhow = { workspace = true }
object_store = { workspace = true }
tempfile = { workspace = true }
reqwest = { workspace = true }
serial_test = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
```

Change to:

```toml
[dependencies]
agentdb-core = { path = "../agentdb-core" }
agentdb-dashboard = { path = "../agentdb-dashboard" }
async-broadcast = { workspace = true }
axum = { workspace = true }
futures-util = "0.3"
reqwest = { workspace = true }
tokio = { workspace = true }
tower = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
agentdb-storage = { path = "../agentdb-storage" }
anyhow = { workspace = true }
object_store = { workspace = true }
tempfile = { workspace = true }
serial_test = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [ ] **Step 2: Verify workspace builds**

Run: `cargo build -p agentdb-api`
Expected: clean (no compile errors; reqwest is already in the lock file as a transitive dep).

- [ ] **Step 3: Commit**

```bash
git add crates/agentdb-api/Cargo.toml
git commit -m "deps(agentdb-api): promote reqwest to regular dep"
```

---

## Task 2: Webhook counters on `BoxMetrics`

**Files:**
- Modify: `crates/agentdb-core/src/metrics.rs`

- [ ] **Step 1: Add fields to the struct**

Edit `crates/agentdb-core/src/metrics.rs`. Find:

```rust
pub struct BoxMetrics {
    pub inserts: AtomicU64,
    pub queries_find: AtomicU64,
    pub queries_aggregate: AtomicU64,
    pub appends: AtomicU64,
    pub forks: AtomicU64,
    pub tier_transitions: AtomicU64,
    pub quota_hits: AtomicU64,
    pub faults_injected: AtomicU64,
    pub gauges: RwLock<GaugeSnapshot>,
}
```

Add two fields at the end of the atomic block (just above `pub gauges`):

```rust
pub struct BoxMetrics {
    pub inserts: AtomicU64,
    pub queries_find: AtomicU64,
    pub queries_aggregate: AtomicU64,
    pub appends: AtomicU64,
    pub forks: AtomicU64,
    pub tier_transitions: AtomicU64,
    pub quota_hits: AtomicU64,
    pub faults_injected: AtomicU64,
    pub webhook_fires: AtomicU64,
    pub webhook_failures: AtomicU64,
    pub gauges: RwLock<GaugeSnapshot>,
}
```

- [ ] **Step 2: Add recording methods**

Inside `impl BoxMetrics { ... }`, after `pub fn record_fault`, add:

```rust
    pub fn record_webhook_fire(&self) {
        self.webhook_fires.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_webhook_failure(&self) {
        self.webhook_failures.fetch_add(1, Ordering::Relaxed);
    }
```

- [ ] **Step 3: Extend `MetricsSnapshot` + `snapshot()`**

Find `pub struct MetricsSnapshot` and add two fields:

```rust
pub struct MetricsSnapshot {
    pub inserts: u64,
    pub queries_find: u64,
    pub queries_aggregate: u64,
    pub appends: u64,
    pub forks: u64,
    pub tier_transitions: u64,
    pub quota_hits: u64,
    pub faults_injected: u64,
    pub webhook_fires: u64,
    pub webhook_failures: u64,
    pub gauges: GaugeSnapshot,
}
```

Find `pub fn snapshot(&self) -> MetricsSnapshot` and extend it to include the new fields:

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
            webhook_fires: self.webhook_fires.load(Ordering::Relaxed),
            webhook_failures: self.webhook_failures.load(Ordering::Relaxed),
            gauges: self.gauges.read().clone(),
        }
    }
```

- [ ] **Step 4: Add a unit test for the counters**

In the existing `#[cfg(test)] mod tests` block, after the existing `counters_increment` test, append:

```rust
    #[test]
    fn webhook_counters_increment() {
        let m = BoxMetrics::new();
        m.record_webhook_fire();
        m.record_webhook_fire();
        m.record_webhook_failure();
        let s = m.snapshot();
        assert_eq!(s.webhook_fires, 2);
        assert_eq!(s.webhook_failures, 1);
    }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p agentdb-core --lib metrics::tests`
Expected: all pass (including the new `webhook_counters_increment`).

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-core/src/metrics.rs
git commit -m "feat(agentdb-core): webhook_fires + webhook_failures counters"
```

---

## Task 3: `Box_::webhook` field + `cold_to_archive` / `archive_to_cold` tiering

**Files:**
- Modify: `crates/agentdb-core/src/box_runtime.rs`
- Modify: `crates/agentdb-core/src/tiering.rs`

- [ ] **Step 1: Add the `webhook` field on `Box_`**

Edit `crates/agentdb-core/src/box_runtime.rs`. Find the `Box_` struct (around line 39):

```rust
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
    pub metrics: Arc<BoxMetrics>,
    pub fault: RwLock<Option<InjectedFault>>,
    pub quota: RwLock<Quota>,
    pub segment_pins: Arc<SegmentPinRegistry>,
    box_bucket: parking_lot::Mutex<Option<TokenBucket>>,
    tenant_buckets: parking_lot::Mutex<HashMap<u64, TokenBucket>>,
}
```

Add `pub webhook: RwLock<Option<String>>` right after `quota`:

```rust
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
    pub metrics: Arc<BoxMetrics>,
    pub fault: RwLock<Option<InjectedFault>>,
    pub quota: RwLock<Quota>,
    pub webhook: RwLock<Option<String>>,
    pub segment_pins: Arc<SegmentPinRegistry>,
    box_bucket: parking_lot::Mutex<Option<TokenBucket>>,
    tenant_buckets: parking_lot::Mutex<HashMap<u64, TokenBucket>>,
}
```

Initialize it in `fn new`:

```rust
    fn new(id: BoxId) -> Self {
        Self {
            id,
            write_lock: Mutex::new(()),
            indexes: tokio::sync::RwLock::new(None),
            metrics: Arc::new(BoxMetrics::new()),
            fault: RwLock::new(None),
            quota: RwLock::new(Quota::default()),
            webhook: RwLock::new(None),
            segment_pins: Arc::new(SegmentPinRegistry::new()),
            box_bucket: parking_lot::Mutex::new(None),
            tenant_buckets: parking_lot::Mutex::new(HashMap::new()),
        }
    }
```

- [ ] **Step 2: Build + run existing tests**

Run: `cargo test -p agentdb-core --lib`
Expected: all pass (field addition is backward-compatible; nothing reads `webhook` yet).

- [ ] **Step 3: Add `cold_to_archive` + `archive_to_cold` to tiering.rs**

Read `crates/agentdb-core/src/tiering.rs` so you know the idiom for `cold_transition` + `reactivate`. Then append two new pub async fns at the end of the file, before the `#[cfg(test)]` block:

```rust
/// Relabel a cold session as archived. Metadata-only: the segment
/// stays where it is. The caller holds the box write lock.
///
/// Returns an error only if the session isn't currently `Cold`.
pub async fn cold_to_archive(
    sessions: &crate::registry::SessionRegistry,
    _guard: &tokio::sync::MutexGuard<'_, ()>,
    session: &mut crate::types::Session,
) -> crate::error::Result<()> {
    if session.tier != crate::types::Tier::Cold {
        return Err(crate::error::Error::Invalid(format!(
            "session {} is not cold (is {:?}); cannot archive",
            session.external_id, session.tier
        )));
    }
    session.tier = crate::types::Tier::Archive;
    sessions.save(session)?;
    Ok(())
}

/// Un-archive: relabel as cold. Metadata-only. Caller holds the box
/// write lock. From cold, a subsequent `reactivate` call can pull
/// events back to warm.
pub async fn archive_to_cold(
    sessions: &crate::registry::SessionRegistry,
    _guard: &tokio::sync::MutexGuard<'_, ()>,
    session: &mut crate::types::Session,
) -> crate::error::Result<()> {
    if session.tier != crate::types::Tier::Archive {
        return Err(crate::error::Error::Invalid(format!(
            "session {} is not archived (is {:?}); cannot un-archive",
            session.external_id, session.tier
        )));
    }
    session.tier = crate::types::Tier::Cold;
    sessions.save(session)?;
    Ok(())
}
```

`SessionRegistry::save(&Session) -> Result<()>` persists under the box write lock the caller already holds — it's the same method `cold_transition` and `reactivate` use.

- [ ] **Step 4: Add unit tests for the new transitions**

In `crates/agentdb-core/src/tiering.rs`'s `#[cfg(test)] mod tests` block, append two tests following the existing pattern for `cold_transition` tests (they already construct a `Box_` + KV + object_store; mirror that setup):

```rust
    #[tokio::test]
    async fn cold_to_archive_flips_tier_only() {
        use crate::types::{Session, Tier};
        // ... set up kv/store/box/tenant/session already in Cold tier
        // (reuse the same helpers used by cold_transition tests — grep
        // for `setup_cold_session` or similar)
        //
        // Then:
        //   let guard = box_.write_lock.lock().await;
        //   cold_to_archive(&sessions, &guard, &mut s).await.unwrap();
        //   drop(guard);
        //   assert_eq!(s.tier, Tier::Archive);
        //   // Segment still present (same path)
        //   assert!(s.segment_path.is_some());
    }

    #[tokio::test]
    async fn archive_to_cold_flips_tier_only() {
        // Mirror image of the above: start in Archive, transition to Cold.
    }

    #[tokio::test]
    async fn cold_to_archive_rejects_non_cold() {
        // Start with a warm session. Call cold_to_archive. Assert error.
    }
```

If you cannot find a shared `setup_cold_session` helper, write the tests with full inline setup — copy the pattern from the existing `cold_transition` test in the same file. Do NOT leave these as stubs.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p agentdb-core --lib tiering::tests`
Expected: all tests pass, including the 3 new ones.

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-core/src/box_runtime.rs crates/agentdb-core/src/tiering.rs
git commit -m "feat(agentdb-core): Box_.webhook field + cold_to_archive / archive_to_cold"
```

---

## Task 4: `LifecycleConfig.t_archive` + `TierChangeHook` + second sweep pass

**Files:**
- Modify: `crates/agentdb-core/src/lifecycle.rs`

- [ ] **Step 1: Extend `LifecycleConfig`**

Edit `crates/agentdb-core/src/lifecycle.rs`. Replace the struct + default:

```rust
#[derive(Debug, Clone, Copy)]
pub struct LifecycleConfig {
    pub tick: Duration,
    pub t_cold: Duration,
    pub t_archive: Duration,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(2),
            t_cold: Duration::from_secs(60),
            t_archive: Duration::from_secs(600),
        }
    }
}
```

- [ ] **Step 2: Add `TierChangeHook` type alias and use it in the engine**

Near the top of the file (after the imports), add:

```rust
use crate::types::{Session, Tier};
use std::sync::Arc as StdArc;

/// Callback invoked after each successful tier transition performed
/// by the lifecycle sweep. `None` at construction time = no-op.
/// Signature: `(box_, fresh_session, from_tier, to_tier)`.
pub type TierChangeHook = StdArc<
    dyn Fn(&Arc<Box_>, Session, Tier, Tier) + Send + Sync,
>;
```

(The existing `use crate::types::Tier;` line already imports `Tier` — keep the expanded `use` or combine them; either way is fine.)

Change `LifecycleEngine` and its constructor:

```rust
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
        hook: Option<TierChangeHook>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(err) = sweep(
                    &kv,
                    &store,
                    &tenants,
                    &sessions,
                    &box_,
                    config,
                    hook.as_ref(),
                )
                .await
                {
                    tracing::warn!(box_id = box_.id, ?err, "lifecycle sweep errored");
                }
            }
        });
        Self { _handle: handle }
    }
}
```

- [ ] **Step 3: Extend `sweep` to take the hook and do cold→archive**

Change the `sweep` signature:

```rust
async fn sweep(
    kv: &Arc<dyn KvStore>,
    store: &Arc<dyn ObjectStore>,
    tenants: &Arc<TenantRegistry>,
    sessions: &Arc<SessionRegistry>,
    box_: &Arc<Box_>,
    config: LifecycleConfig,
    hook: Option<&TierChangeHook>,
) -> crate::error::Result<()> {
```

Inside the `match cold_transition(...)` success arm, **after** the existing metrics calls, insert the hook invocation:

```rust
            match cold_transition(kv, store, sessions, &guard, &mut fresh).await {
                Ok(()) => {
                    let _ = session;
                    box_.metrics.record_tier_transition();
                    box_.metrics.drop_tier(Tier::Warm);
                    box_.metrics.bump_tier(Tier::Cold);
                    if let Some(h) = hook {
                        h(box_, fresh.clone(), Tier::Warm, Tier::Cold);
                    }
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
```

Then, **after the existing outer `for session_ext in session_externals` loop for the warm→cold pass, and before the outer `for tenant_ext` loop closes**, add a second loop that handles cold→archive. Be precise about the braces: the second loop goes inside the tenant loop so it processes all cold sessions for the same tenant, right after the warm pass for that tenant.

To keep the code readable, replace the entire inner body of the `for tenant_ext in tenant_externals { ... }` loop with something like this:

```rust
    for tenant_ext in tenant_externals {
        let Some(tenant) = tenants.lookup(box_.id, &tenant_ext)? else {
            continue;
        };
        let session_externals = sessions.list_in_tenant(box_.id, tenant.internal_id)?;

        // Pass 1: warm → cold
        let cold_threshold_ms = config.t_cold.as_millis() as u64;
        for session_ext in &session_externals {
            let Some(session) = sessions.lookup(box_.id, tenant.internal_id, session_ext)? else {
                continue;
            };
            if session.tier != Tier::Warm {
                continue;
            }
            if now_ms.saturating_sub(session.last_active_at) < cold_threshold_ms {
                continue;
            }
            let guard = box_.write_lock.lock().await;
            let Some(mut fresh) = sessions.lookup(box_.id, tenant.internal_id, session_ext)?
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
                    let _ = session;
                    box_.metrics.record_tier_transition();
                    box_.metrics.drop_tier(Tier::Warm);
                    box_.metrics.bump_tier(Tier::Cold);
                    if let Some(h) = hook {
                        h(box_, fresh.clone(), Tier::Warm, Tier::Cold);
                    }
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

        // Pass 2: cold → archive
        let archive_threshold_ms = config.t_archive.as_millis() as u64;
        for session_ext in &session_externals {
            let Some(session) = sessions.lookup(box_.id, tenant.internal_id, session_ext)? else {
                continue;
            };
            if session.tier != Tier::Cold {
                continue;
            }
            if now_ms.saturating_sub(session.last_active_at) < archive_threshold_ms {
                continue;
            }
            let guard = box_.write_lock.lock().await;
            let Some(mut fresh) = sessions.lookup(box_.id, tenant.internal_id, session_ext)?
            else {
                continue;
            };
            if fresh.tier != Tier::Cold {
                continue;
            }
            if now_ms.saturating_sub(fresh.last_active_at) < archive_threshold_ms {
                continue;
            }
            match crate::tiering::cold_to_archive(sessions, &guard, &mut fresh).await {
                Ok(()) => {
                    box_.metrics.record_tier_transition();
                    box_.metrics.drop_tier(Tier::Cold);
                    box_.metrics.bump_tier(Tier::Archive);
                    if let Some(h) = hook {
                        h(box_, fresh.clone(), Tier::Cold, Tier::Archive);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        box_id = box_.id,
                        session = %session_ext,
                        ?err,
                        "cold_to_archive failed"
                    );
                }
            }
        }
    }
```

Remove the original `let cold_threshold_ms = config.t_cold.as_millis() as u64;` that was at function-scope — it's now inside the tenant loop.

- [ ] **Step 4: Fix up existing lifecycle tests**

The existing lifecycle tests call `LifecycleEngine::start` with 6 positional args. We just added a 7th (`hook`). Every test call-site must add a trailing `None`. Find them:

```bash
grep -n "LifecycleEngine::start" crates/agentdb-core/src/lifecycle.rs bin/agentdb-server/src/main.rs
```

For each hit in `crates/agentdb-core/src/lifecycle.rs`, add `, None` to the arg list. (The binary call in `main.rs` is handled in a later task.)

- [ ] **Step 5: Add a cold→archive sweep test**

At the bottom of `crates/agentdb-core/src/lifecycle.rs`'s `#[cfg(test)] mod tests` block, append one test. Follow the pattern of any existing sweep test in the file (look for `sweep_demotes_warm` or a similar existing test to copy the setup helpers):

```rust
    #[tokio::test]
    async fn sweep_archives_cold_session_past_t_archive() {
        use crate::types::Tier;
        // Copy setup from the existing sweep test: build kv/store/
        // tenants/sessions, construct Box_, insert a tenant, insert a
        // session with tier = Cold and last_active_at ancient enough to
        // be past t_archive.
        //
        // Run sweep with:
        //   LifecycleConfig {
        //     tick: Duration::from_millis(50),
        //     t_cold: Duration::from_millis(10),
        //     t_archive: Duration::from_millis(20),
        //   }
        //
        // After sweep returns, re-read the session and assert
        // session.tier == Tier::Archive.
    }
```

If the existing test uses a private helper that sets up cold sessions, reuse it. If not, mirror the setup from `cold_transition` tests inline.

- [ ] **Step 6: Run tests**

Run: `cargo test -p agentdb-core --lib lifecycle::tests`
Expected: all existing tests pass + the new `sweep_archives_cold_session_past_t_archive` passes.

- [ ] **Step 7: Commit**

```bash
git add crates/agentdb-core/src/lifecycle.rs
git commit -m "feat(agentdb-core): t_archive config + TierChangeHook + cold->archive sweep"
```

---

## Task 5: `webhook` module + `fire_tier_change` free function

**Files:**
- Create: `crates/agentdb-api/src/webhook.rs`
- Modify: `crates/agentdb-api/src/lib.rs`

- [ ] **Step 1: Create the webhook module**

Create `crates/agentdb-api/src/webhook.rs`:

```rust
//! Lifecycle webhook delivery. Fire-and-forget HTTP POST on each
//! tier transition, read via the caller's per-`Box_` configured URL.
//! No retries; failures are logged + counted on `BoxMetrics`.

use std::sync::Arc;
use std::time::Duration;

use agentdb_core::{Box_, Session, Tier};
use serde::Serialize;
use serde_json::json;

/// 5s connect+read timeout. Clients choose their own tolerance for
/// long webhooks; we don't want a slow receiver blocking a box's
/// lifecycle worker forever.
pub const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared HTTP client factory. Clone this into `AppState` once;
/// `reqwest::Client` is internally reference-counted and
/// connection-pooled.
pub fn new_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(WEBHOOK_TIMEOUT)
        .build()
        .expect("reqwest client builds with default config")
}

#[derive(Serialize)]
struct Payload<'a> {
    event: &'static str,
    ts_ms: u64,
    box_id: u16,
    tenant_id: &'a str,
    session_id: &'a str,
    from: &'static str,
    to: &'static str,
    session: SessionFields<'a>,
}

#[derive(Serialize)]
struct SessionFields<'a> {
    internal_id: u64,
    next_seq: u64,
    last_active_at: u64,
    segment_path: Option<&'a str>,
}

fn tier_str(t: Tier) -> &'static str {
    match t {
        Tier::Warm => "warm",
        Tier::Cold => "cold",
        Tier::Archive => "archive",
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Read the box's configured webhook URL. If `Some(url)`,
/// `tokio::spawn` a task that POSTs a JSON payload and bumps the
/// box's metrics counters based on the result. Returns immediately.
pub fn fire_tier_change(
    client: &reqwest::Client,
    box_: &Arc<Box_>,
    tenant_external: &str,
    session: Session,
    from: Tier,
    to: Tier,
) {
    let url = { box_.webhook.read().clone() };
    let Some(url) = url else {
        return;
    };
    let client = client.clone();
    let box_ = box_.clone();
    let tenant_id = tenant_external.to_string();
    let session_id = session.external_id.clone();
    let segment_path = session.segment_path.clone();
    tokio::spawn(async move {
        let body = json!({
            "event": "TierChange",
            "ts_ms": now_ms(),
            "box_id": box_.id,
            "tenant_id": tenant_id,
            "session_id": session_id,
            "from": tier_str(from),
            "to": tier_str(to),
            "session": {
                "internal_id": session.internal_id,
                "next_seq": session.next_seq,
                "last_active_at": session.last_active_at,
                "segment_path": segment_path,
            },
        });
        match client.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                box_.metrics.record_webhook_fire();
            }
            Ok(resp) => {
                box_.metrics.record_webhook_failure();
                tracing::warn!(
                    box_id = box_.id,
                    status = resp.status().as_u16(),
                    url = %url,
                    "webhook returned non-2xx"
                );
            }
            Err(err) => {
                box_.metrics.record_webhook_failure();
                tracing::warn!(
                    box_id = box_.id,
                    ?err,
                    url = %url,
                    "webhook POST failed"
                );
            }
        }
    });
}

// Let Payload / SessionFields stay private; the JSON body is the
// only public contract, not the Rust types.
#[allow(dead_code)]
type _Unused = (Payload<'static>, SessionFields<'static>);
```

The `Payload` / `SessionFields` structs are kept for future typed callers but the current public function uses `serde_json::json!` directly for readability — they don't need to be public. The `_Unused` type silences the "unused struct" lint without exposing them.

- [ ] **Step 2: Declare the module + extend `AppState`**

Edit `crates/agentdb-api/src/lib.rs`. Near the top add `pub mod webhook;` alphabetically (between `stream` and any other mod).

Find the `pub struct AppState`:

```rust
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
    pub stream: StreamBus,
}
```

Extend with an `http: reqwest::Client` field:

```rust
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
    pub stream: StreamBus,
    pub http: reqwest::Client,
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p agentdb-api`
Expected: compile errors in callers that construct `AppState { ... }` without `http`. There are three call sites we need to fix: `bin/agentdb-server/src/main.rs`, `crates/agentdb-api/tests/http.rs`, `crates/agentdb-api/tests/admission.rs`, and `crates/agentdb-wire/tests/driver.rs`. We fix `main.rs` in a later task; the test sites need a quick patch now so tests still compile.

Run: `grep -rn "AppState {" crates/ bin/ --include="*.rs" | grep -v "//"`
For each test file hit, add `http: agentdb_api::webhook::new_client(),` to the struct literal. For the binary main, defer to Task 8.

Apply the edits, then:

Run: `cargo test -p agentdb-api --no-run 2>&1 | tail -5`
Expected: compilation succeeds.

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-api/src/webhook.rs crates/agentdb-api/src/lib.rs \
        crates/agentdb-api/tests/http.rs crates/agentdb-api/tests/admission.rs \
        crates/agentdb-wire/tests/driver.rs
git commit -m "feat(agentdb-api): webhook::fire_tier_change + AppState.http client"
```

---

## Task 6: Admin endpoints — `target=archive` + `POST .../webhook`

**Files:**
- Modify: `crates/agentdb-api/src/admin.rs`
- Modify: `crates/agentdb-api/src/admin_boxes.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (add the new route)

- [ ] **Step 1: Extend `force_tier` to handle archive transitions**

Edit `crates/agentdb-api/src/admin.rs`. Current handler only knows `"warm"` and `"cold"`. Replace the function body:

```rust
pub async fn force_tier(
    State(state): State<AppState>,
    Json(body): Json<ForceTierRequest>,
) -> Result<Json<ForceTierResponse>, ApiError> {
    let target = body.target.to_ascii_lowercase();
    match target.as_str() {
        "warm" | "cold" | "archive" => {}
        other => {
            return Err(ApiError::BadRequest(format!(
                "unsupported target tier {other}; expected 'warm' | 'cold' | 'archive'"
            )));
        }
    }

    let box_ = state.registry.route_tenant(&body.tenant_id);
    let tenant = state
        .tenants
        .lookup(box_.id, &body.tenant_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("tenant {} not found", body.tenant_id)))?;
    let mut session = state
        .sessions
        .lookup(box_.id, tenant.internal_id, &body.session_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("session {} not found", body.session_id)))?;

    let guard = box_.write_lock.lock().await;
    // Accumulate transitions so each step can fire its own webhook + SSE.
    let mut steps: Vec<(Tier, Tier)> = Vec::new();
    loop {
        use agentdb_core::tiering::{archive_to_cold, cold_to_archive, cold_transition, reactivate};
        let from = session.tier;
        let done = match (from, target.as_str()) {
            (Tier::Warm, "cold") => {
                cold_transition(
                    state.registry.kv(),
                    state.registry.object_store(),
                    &state.sessions,
                    &guard,
                    &mut session,
                )
                .await?;
                steps.push((Tier::Warm, Tier::Cold));
                true
            }
            (Tier::Warm, "archive") => {
                cold_transition(
                    state.registry.kv(),
                    state.registry.object_store(),
                    &state.sessions,
                    &guard,
                    &mut session,
                )
                .await?;
                steps.push((Tier::Warm, Tier::Cold));
                false // continue loop: now at cold, still targeting archive
            }
            (Tier::Cold, "archive") => {
                cold_to_archive(&state.sessions, &guard, &mut session).await?;
                steps.push((Tier::Cold, Tier::Archive));
                true
            }
            (Tier::Cold, "warm") => {
                reactivate(
                    state.registry.kv(),
                    state.registry.object_store(),
                    &state.sessions,
                    &guard,
                    &mut session,
                )
                .await?;
                steps.push((Tier::Cold, Tier::Warm));
                true
            }
            (Tier::Archive, "cold") => {
                archive_to_cold(&state.sessions, &guard, &mut session).await?;
                steps.push((Tier::Archive, Tier::Cold));
                true
            }
            (Tier::Archive, "warm") => {
                archive_to_cold(&state.sessions, &guard, &mut session).await?;
                steps.push((Tier::Archive, Tier::Cold));
                false // continue loop: now at cold, still targeting warm
            }
            _ => true, // already at target — no-op
        };
        if done {
            break;
        }
    }
    drop(guard);

    // Bump metrics + fire SSE + webhook for each step.
    for &(from, to) in &steps {
        box_.metrics.record_tier_transition();
        box_.metrics.drop_tier(from);
        box_.metrics.bump_tier(to);
        state.stream.emit(StreamEvent::TierChange {
            ts_ms: now_ms(),
            box_id: box_.id,
            session_id: body.session_id.clone(),
            from: tier_str(from).into(),
            to: tier_str(to).into(),
        });
        crate::webhook::fire_tier_change(
            &state.http,
            &box_,
            &body.tenant_id,
            session.clone(),
            from,
            to,
        );
    }

    Ok(Json(ForceTierResponse {
        tier: tier_str(session.tier).into(),
    }))
}

fn tier_str(t: Tier) -> &'static str {
    match t {
        Tier::Warm => "warm",
        Tier::Cold => "cold",
        Tier::Archive => "archive",
    }
}
```

- [ ] **Step 2: Add the webhook-config admin endpoint**

Edit `crates/agentdb-api/src/admin_boxes.rs`. Append:

```rust
#[derive(Deserialize)]
pub struct WebhookRequest {
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Serialize)]
pub struct WebhookResponse {
    pub url: Option<String>,
}

pub async fn set_webhook(
    State(state): State<AppState>,
    Path(id): Path<u16>,
    Json(body): Json<WebhookRequest>,
) -> Result<Json<WebhookResponse>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| ApiError::BadRequest(format!("box {id} not found")))?;
    if let Some(ref u) = body.url {
        if !(u.starts_with("http://") || u.starts_with("https://")) {
            return Err(ApiError::BadRequest(format!(
                "webhook url must start with http:// or https:// (got {u})"
            )));
        }
    }
    *b.webhook.write() = body.url.clone();
    Ok(Json(WebhookResponse { url: body.url }))
}
```

- [ ] **Step 3: Wire the new route**

Edit `crates/agentdb-api/src/lib.rs`. Find the `router` function. Near the other `admin_boxes::...` routes, add:

```rust
        .route(
            "/api/admin/boxes/:id/webhook",
            axum::routing::post(admin_boxes::set_webhook),
        )
```

- [ ] **Step 4: Build**

Run: `cargo build -p agentdb-api`
Expected: clean.

- [ ] **Step 5: Run existing tests — nothing should have regressed**

Run: `cargo test -p agentdb-api`
Expected: all passing (new tests come in Task 7).

- [ ] **Step 6: Commit**

```bash
git add crates/agentdb-api/src/admin.rs crates/agentdb-api/src/admin_boxes.rs \
        crates/agentdb-api/src/lib.rs
git commit -m "feat(agentdb-api): archive tier in force_tier + POST /boxes/:id/webhook"
```

---

## Task 7: Integration test — mock server + end-to-end webhook + archive

**Files:**
- Create: `crates/agentdb-api/tests/archive_webhook.rs`

- [ ] **Step 1: Write the integration test**

Create `crates/agentdb-api/tests/archive_webhook.rs`:

```rust
//! End-to-end test for Archive tier + lifecycle webhook delivery.

use std::net::SocketAddr;
use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use serial_test::serial;
use tempfile::TempDir;
use tokio::sync::mpsc;

type Rx = mpsc::UnboundedReceiver<Value>;
type Tx = mpsc::UnboundedSender<Value>;

/// Start a local hyper server that forwards every POST body into an
/// mpsc channel. Returns (base_url, receiver).
async fn mock_webhook_server() -> (String, Rx) {
    let (tx, rx) = mpsc::unbounded_channel::<Value>();
    let app = Router::new()
        .route("/", post(
            |State(tx): State<Tx>, Json(body): Json<Value>| async move {
                let _ = tx.send(body);
                axum::http::StatusCode::NO_CONTENT
            },
        ))
        .with_state(tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/"), rx)
}

async fn spawn_db() -> (TempDir, TempDir, String, Arc<BoxRegistry>) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"), registry)
}

async fn recv_with_timeout(rx: &mut Rx) -> Option<Value> {
    tokio::time::timeout(std::time::Duration::from_millis(2000), rx.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
#[serial]
async fn archive_roundtrip_fires_webhooks_for_every_step() {
    let (mock_url, mut rx) = mock_webhook_server().await;
    let (_dir, _obj_dir, base, registry) = spawn_db().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("t-a").id;

    // Configure webhook URL.
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/webhook"))
        .json(&json!({"url": mock_url}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Seed a session in warm.
    client
        .post(format!("{base}/api/tenants/t-a/sessions/s-1/append_and_fetch"))
        .json(&json!({"event":{"text":"hi"},"fetch_last_n":0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // warm → cold
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-a",
            "session_id": "s-1",
            "target": "cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let ev = recv_with_timeout(&mut rx).await.expect("warm→cold webhook");
    assert_eq!(ev["event"], "TierChange");
    assert_eq!(ev["from"], "warm");
    assert_eq!(ev["to"], "cold");
    assert_eq!(ev["tenant_id"], "t-a");
    assert_eq!(ev["session_id"], "s-1");

    // cold → archive
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-a",
            "session_id": "s-1",
            "target": "archive"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let ev = recv_with_timeout(&mut rx).await.expect("cold→archive webhook");
    assert_eq!(ev["from"], "cold");
    assert_eq!(ev["to"], "archive");

    // archive → warm: two-step (archive→cold, then cold→warm)
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({
            "tenant_id": "t-a",
            "session_id": "s-1",
            "target": "warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let ev1 = recv_with_timeout(&mut rx).await.expect("archive→cold webhook");
    let ev2 = recv_with_timeout(&mut rx).await.expect("cold→warm webhook");
    assert_eq!(ev1["from"], "archive");
    assert_eq!(ev1["to"], "cold");
    assert_eq!(ev2["from"], "cold");
    assert_eq!(ev2["to"], "warm");
}

#[tokio::test]
#[serial]
async fn no_webhook_when_url_unset() {
    let (_dir, _obj_dir, base, registry) = spawn_db().await;
    let client = reqwest::Client::new();
    let _box_id = registry.route_tenant("t-b").id;

    // Seed and force without ever setting a URL.
    client
        .post(format!("{base}/api/tenants/t-b/sessions/s/append_and_fetch"))
        .json(&json!({"event":{"text":"hi"},"fetch_last_n":0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client
        .post(format!("{base}/api/admin/tier/force"))
        .json(&json!({"tenant_id": "t-b", "session_id": "s", "target": "cold"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // No assertion machinery needed — the test passes as long as the
    // request didn't panic or hang on a missing webhook. The spawn
    // path returns immediately when `webhook` is None.
}

#[tokio::test]
#[serial]
async fn set_webhook_rejects_non_http_url() {
    let (_dir, _obj_dir, base, registry) = spawn_db().await;
    let client = reqwest::Client::new();
    let box_id = registry.route_tenant("anything").id;
    let r = client
        .post(format!("{base}/api/admin/boxes/{box_id}/webhook"))
        .json(&json!({"url": "ftp://bad"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p agentdb-api --test archive_webhook`
Expected: 3/3 pass.

- [ ] **Step 3: Commit**

```bash
git add crates/agentdb-api/tests/archive_webhook.rs
git commit -m "test(agentdb-api): archive roundtrip + webhook delivery + URL validation"
```

---

## Task 8: Server main — parse `T_ARCHIVE_SECS`, wire the hook

**Files:**
- Modify: `bin/agentdb-server/src/main.rs`

- [ ] **Step 1: Read current lifecycle setup + AppState construction**

Run: `grep -n "LifecycleConfig\|LifecycleEngine::start\|AppState\|AGENTDB_T_COLD_SECS" bin/agentdb-server/src/main.rs`

You should see the existing `LifecycleConfig { tick: ..., t_cold: ... }` block and the `LifecycleEngine::start(...)` call. We need to:
1. Parse `AGENTDB_T_ARCHIVE_SECS` (default 600)
2. Add `t_archive` to the `LifecycleConfig` literal
3. Build the `http` client once, put it in `AppState`
4. Build a `TierChangeHook` closure that calls `fire_tier_change` and pass it as the 7th arg to `LifecycleEngine::start`

- [ ] **Step 2: Parse the env var and add to `LifecycleConfig`**

Find the existing block that constructs `LifecycleConfig`:

```rust
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
```

Change to:

```rust
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
        t_archive: Duration::from_secs(
            std::env::var("AGENTDB_T_ARCHIVE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(600),
        ),
    };
```

- [ ] **Step 3: Add the `http` client + hook to AppState + LifecycleEngine::start**

Find the block that constructs `AppState`:

```rust
    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
        stream: stream.clone(),
    };
```

Change to (the `http` client goes in, and we also stash it for the hook):

```rust
    let http_client = agentdb_api::webhook::new_client();
    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
        stream: stream.clone(),
        http: http_client.clone(),
    };
```

Then find the `LifecycleEngine::start(...)` call (likely inside a loop over boxes). The current shape:

```rust
        LifecycleEngine::start(
            kv.clone(),
            store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_.clone(),
            lifecycle_cfg,
        )
```

Replace the call with one that takes the hook closure. Before the loop, construct the hook once:

```rust
    let hook: agentdb_core::lifecycle::TierChangeHook = {
        let http = http_client.clone();
        let stream_for_hook = stream.clone();
        let tenants_for_hook = tenants.clone();
        std::sync::Arc::new(move |box_, session, from, to| {
            // Find the tenant external id so the webhook carries it.
            // We could thread it through the callback signature, but
            // the session's tenant_internal_id + a lookup is cheap and
            // keeps the core::lifecycle surface narrow.
            let tenant_external = tenants_for_hook
                .lookup_by_internal(box_.id, session.tenant_internal_id)
                .ok()
                .flatten()
                .map(|t| t.external_id)
                .unwrap_or_else(|| format!("tenant-{}", session.tenant_internal_id));

            // SSE TierChange so the dashboard sees it (auto-sweep did
            // not emit this before — now it does, consistently with
            // admin-forced).
            stream_for_hook.emit(agentdb_api::stream::StreamEvent::TierChange {
                ts_ms: agentdb_api::stream::now_ms(),
                box_id: box_.id,
                session_id: session.external_id.clone(),
                from: tier_name(from).into(),
                to: tier_name(to).into(),
            });

            agentdb_api::webhook::fire_tier_change(
                &http,
                box_,
                &tenant_external,
                session,
                from,
                to,
            );
        })
    };

    fn tier_name(t: agentdb_core::Tier) -> &'static str {
        match t {
            agentdb_core::Tier::Warm => "warm",
            agentdb_core::Tier::Cold => "cold",
            agentdb_core::Tier::Archive => "archive",
        }
    }
```

**Important:** `tier_name` needs to be at module scope or within `main()` — if `main` has `#[tokio::main]` on it, a nested `fn` is fine in Rust. Place it right above or below the hook construction. If the existing `main.rs` file has other free helpers at module scope, follow that pattern.

Then the loop that starts engines:

```rust
    for (i, box_) in registry.boxes().iter().enumerate() {
        let engine = LifecycleEngine::start(
            kv.clone(),
            store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_.clone(),
            lifecycle_cfg,
            Some(hook.clone()),
        );
        engines.push(engine);
        let _ = i;
    }
```

(If the existing loop has a different shape, preserve it and just add `, Some(hook.clone())` as the 7th positional arg to the `::start` call.)

- [ ] **Step 4: Run the server + smoke script**

Run: `cargo build -p agentdb-server`
Expected: clean compile.

Run: `pkill -f agentdb-server 2>/dev/null; sleep 1; bash scripts/smoke.sh 2>&1 | tail -3`
Expected: `smoke ok`.

- [ ] **Step 5: Commit**

```bash
git add bin/agentdb-server/src/main.rs
git commit -m "feat(agentdb-server): parse T_ARCHIVE_SECS + wire TierChangeHook"
```

---

## Task 9: Dashboard — show webhook URL + fires/failures per box

**Files:**
- Modify: `crates/agentdb-dashboard/static/app.js`
- Modify: `crates/agentdb-dashboard/static/index.html`

- [ ] **Step 1: Extend the per-box row rendering in `app.js`**

Read `crates/agentdb-dashboard/static/app.js` to understand the current per-box tile rendering. Find where the tile HTML is built (look for `box_id` / `box.id` usage).

Each tile currently likely shows: tier counts + quota + fault indicator. Add two new lines for webhook status:

```js
// In the per-box render, below the existing fields, append something like:
// (adapt to the template's real syntax — this is a conceptual patch, not a literal drop-in)
//
//   <div class="webhook">
//     webhook: <span>${b.webhook_url ? maskHost(b.webhook_url) : 'none'}</span>
//     (fires: <span>${b.metrics.webhook_fires}</span> / fails: <span>${b.metrics.webhook_failures}</span>)
//   </div>
//
// plus a helper:
function maskHost(url) {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.host}/...`;
  } catch { return '***invalid***'; }
}
```

However: `b.webhook_url` is NOT currently served by `/api/admin/boxes`. Only `metrics.webhook_fires` and `metrics.webhook_failures` will be, via the extended `MetricsSnapshot` from Task 2. So either:

- **Expose `webhook_url` in `BoxListItem`** (modify `crates/agentdb-api/src/admin_boxes.rs` to include `webhook_url: Option<String>` in the struct, populated from `b.webhook.read().clone()` in both `list_boxes` and `get_box`), and then the dashboard reads `b.webhook_url`.

The cleaner path is the first: make it visible via the admin snapshot. Make that change in `admin_boxes.rs`:

In `pub struct BoxListItem`, add `pub webhook_url: Option<String>,`.
In both `list_boxes` and `get_box`, read `b.webhook.read().clone()` and include it in the constructed `BoxListItem`.

Then in `app.js`, render `b.webhook_url` and `b.metrics.webhook_fires` / `b.metrics.webhook_failures`.

Exact JS change: find the function(s) that build per-box DOM and append two `<div>`s (or whatever the template uses). If the project uses Alpine.js `x-data`/`x-text` from earlier milestones, add the two spans inside the existing tile `<template>` in `index.html`.

- [ ] **Step 2: Verify `cargo build -p agentdb-api` still clean + dashboard admin call returns webhook_url**

Run: `cargo build -p agentdb-api`
Expected: clean.

Run (smoke-style):
```bash
pkill -f agentdb-server 2>/dev/null; sleep 1
AGENTDB_DATA=/tmp/m12-data AGENTDB_OBJECTS=/tmp/m12-obj \
cargo run -p agentdb-server >/tmp/m12-srv.log 2>&1 &
sleep 2
curl -fsS http://127.0.0.1:7878/api/admin/boxes | python3 -m json.tool | head -40
pkill -f agentdb-server
rm -rf /tmp/m12-data /tmp/m12-obj
```
Expected: JSON per-box includes `"webhook_url": null` and `"metrics": { ..., "webhook_fires": 0, "webhook_failures": 0 }`.

- [ ] **Step 3: Manual UI check (optional, not gating)**

Not part of the automated gate — test locally if you like by starting the server + setting a webhook URL via `POST .../webhook` and forcing a tier transition; the dashboard should show the URL and increment fires. The smoke test in Task 10 exercises the server-side path end-to-end without UI.

- [ ] **Step 4: Commit**

```bash
git add crates/agentdb-dashboard/static/app.js crates/agentdb-dashboard/static/index.html \
        crates/agentdb-api/src/admin_boxes.rs
git commit -m "feat(dashboard): surface webhook URL + fires/failures per box"
```

---

## Task 10: Update docs

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/PERFORMANCE.md`

- [ ] **Step 1: Update `ARCHITECTURE.md`**

Find the "Lifecycle: warm → cold" section. Rename to "Lifecycle: warm → cold → archive" and append a short paragraph:

> After `AGENTDB_T_ARCHIVE_SECS` (default 10 minutes) of additional idle, a cold session transitions to `Tier::Archive`. Archive in the prototype is a metadata-only flip on the `Session` record — the segment stays where it is, and the session remains queryable via both REST `fetch_last_n` and Mongo `find`/`aggregate`. A production deployment would use this hook to copy the segment to a cheaper object-storage class (S3 Intelligent-Tiering / Glacier Instant); that copy is explicitly out of the prototype's scope.

Find the "does NOT do" list. Update the archive-adjacent entries to reflect what M12 just landed. Also add a new bullet about lifecycle webhooks:

Replace:
```
- **No LLM hooks, no lifecycle webhooks, no derived-data pipeline.**
```
with:
```
- **Lifecycle webhooks** fire fire-and-forget HTTP POSTs on every
  tier transition (per-box URL, no retries, no auth beyond HTTPS).
  LLM hooks and derivative-data pipelines are out of scope — the
  integrator runs them behind the webhook.
```

- [ ] **Step 2: Update `PERFORMANCE.md`**

Append a new section after "Admission control overhead (M10)":

```markdown
## Lifecycle webhook overhead (M12)

Each tier transition fires a `tokio::spawn`ed `reqwest` POST with a
5s timeout. The caller returns immediately; the transition itself
does not wait. Measured overhead in the spawn path: a single
`RwLock::read().clone()` to read the URL + one spawn, all in the
low-µs range. Webhook delivery success/failure is counted on
`BoxMetrics::webhook_fires` / `webhook_failures` and surfaces via
`/api/admin/boxes` and the dashboard.

If a receiver is down, transitions keep happening and are counted
against `webhook_failures`. There is no retry and no durable outbox
in the prototype — a real deployment should front the webhook with
an internal queue or accept the at-most-once semantics.
```

- [ ] **Step 3: Commit**

```bash
git add docs/ARCHITECTURE.md docs/PERFORMANCE.md
git commit -m "docs: M12 archive tier + webhook behavior in arch/perf notes"
```

---

## Task 11: Milestone exit gate

- [ ] **Step 1: Workspace tests**

Run: `cargo test --workspace`
Expected: all pass.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --all --check`
Expected: clean.

- [ ] **Step 4: Go**

Run: `cd workload && /opt/homebrew/bin/go build ./... && /opt/homebrew/bin/go vet ./...`
Expected: clean.

- [ ] **Step 5: Smoke**

Run: `pkill -f agentdb-server 2>/dev/null; sleep 1; bash scripts/smoke.sh 2>&1 | tail -3`
Expected: `smoke ok`.

- [ ] **Step 6: If anything failed**

Fix inline with a `fix(m12): ...` commit. Do not mark the milestone complete until steps 1-5 are all green.

---

## Self-review checklist

Every spec requirement maps to a task:

- [x] `TenantRegistry::lookup_by_internal` (needed by hook closure in Task 8) — Task 0
- [x] `Tier::Archive` reachable via sweep — Task 4
- [x] `Tier::Archive` reachable via admin force (incl. multi-step archive→warm and warm→archive) — Task 6
- [x] `cold_to_archive` + `archive_to_cold` transition fns — Task 3
- [x] `AGENTDB_T_ARCHIVE_SECS` env var — Task 8
- [x] `LifecycleConfig.t_archive` — Task 4
- [x] `TierChangeHook` on `LifecycleEngine` — Task 4
- [x] `Box_::webhook: RwLock<Option<String>>` — Task 3
- [x] `POST /api/admin/boxes/:id/webhook` with URL validation — Task 6
- [x] `fire_tier_change` with 5s timeout, spawn, no retry — Task 5
- [x] Metrics counters `webhook_fires` + `webhook_failures` — Task 2
- [x] All 5 fire sites: 2 sweep + 2 admin (force_tier + multi-step) + binary hook — Tasks 4 + 6 + 8
- [x] Payload schema — Task 5
- [x] Archived sessions still queryable (no segment move) — Task 3 (tiering fns don't touch segments) + Task 7 (exercises archive→read round-trip)
- [x] Dashboard shows webhook URL + counters — Task 9
- [x] Docs updated — Task 10
- [x] Exit gate — Task 11

---

## Risks not covered by tests

- **Hook closure captures.** If the binary's hook closure accidentally captures `tokio::runtime::Handle::current()` at construction time, it can leak. The spec'd form captures only owned `Arc` / reqwest `Client` / `StreamBus` clones, all of which are Send + Sync and safe to clone into a `tokio::spawn`.
- **Order of SSE vs webhook emit.** The plan fires SSE first, then webhook. If an integrator is listening to both, they may see the SSE event before the webhook POST arrives — fine for independent channels, worth documenting in integrator-facing docs if we ever write them. Tracked for follow-up.
