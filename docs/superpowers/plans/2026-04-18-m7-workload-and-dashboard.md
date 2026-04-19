# AgentDB Prototype — Milestone 7: Metrics + dashboard + workload generator

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the six milestones of database work into a running showcase. A Go workload generator simulates a customer-support chatbot platform driving realistic traffic. A live HTML dashboard visualizes many boxes, many tenants, session tiers, events per second, and fork lineages in one frame. Admin endpoints let a presenter trigger faults, cold-tier moves, and forks on command so the visuals tell the architectural story.

**Architecture:** Per-box metrics (counters, gauges) live on each `Box_` and are aggregated into a GET `/api/admin/boxes` listing. Fault injection and quota knobs are REST endpoints; the per-request fault check runs in the REST + Mongo wire layers. Server-Sent Events (SSE) on `/api/stream` deliver a live event feed (tier changes, forks, quota hits, fault injections, query plans, per-second metrics tick) — a `tracing_subscriber` layer forwards tagged events to an `async_broadcast` channel, which the SSE endpoint consumes. A static HTML page at `/dashboard` (vanilla JS + Alpine.js via CDN, no build step) reads from both the snapshot endpoints and the SSE stream. A separate Go module under `workload/` drives traffic via the `mongodb` Go driver + `net/http` REST client; a small control server on the workload (port 9090) accepts `/demo/*` shortcuts the dashboard can trigger.

**Tech Stack (M7 additions):**
- Rust: `async-broadcast` 0.7 (fan-out for SSE), `axum` already on 0.7 (native SSE support), `metrics` + `metrics-util` 0.18 (in-process metrics registry; we use Counter/Gauge only — no exporter).
- Go: `go 1.22`, `go.mongodb.org/mongo-driver` v1.17 (official driver; v2 requires the full replica-set handshake), `gopkg.in/yaml.v3`, stdlib `net/http`.

**References:**
- Spec: `docs/superpowers/specs/2026-04-17-agentdb-prototype-design.md` §10 (box architecture), §12 (workload), §13 (dashboard), §14 (observability)
- Previous milestones: M1-M6

---

## Scope

**In scope for M7:**
- Per-box `BoxMetrics` on each `Box_`: counters for `inserts`, `queries_by_type` (find/aggregate/append/fork), `tier_transitions`, `fork_created`, `quota_hits`, `fault_injected`; gauges for `sessions_by_tier`, `bytes_by_tier` (approximate — exact byte accounting is out of scope).
- Admin REST endpoints:
  - GET `/api/admin/boxes` (list) — each box: id, num_tenants, num_sessions (per tier), counters snapshot.
  - GET `/api/admin/boxes/:id` — single box detail.
  - GET `/api/admin/sessions/:session_id?tenant_id=X` — session details (branch lineage if forked, tier, last_active_at, recent ops).
  - POST `/api/admin/boxes/:id/inject_fault` body `{kind: "latency"|"error", duration_s}` — stored on `Box_`; the REST + wire handlers check it and honor.
  - POST `/api/admin/boxes/:id/quota` body `{max_warm_sessions?, max_qps?}` — soft caps stored on `Box_`; enforcement on writes.
  - (Admin tier/force and fork from M5/M6 unchanged.)
- SSE endpoint GET `/api/stream` emitting a typed event enum; streamed via `axum::response::Sse`.
- Static dashboard served at `/dashboard`: index.html + styles.css + app.js, all loaded by axum's `ServeDir` from `crates/agentdb-dashboard/static/`. Uses Alpine.js from a CDN. Renders a top bar, box grid (tenants as colored dots by tier), session detail drawer, event feed; demo shortcut buttons at the top.
- Go workload generator under `workload/`:
  - Module: `go.mod` with `mongo-driver` + `yaml`
  - `cmd/agentdb-workload/main.go` — CLI + main loop
  - `internal/persona/` — persona state machine, YAML config loader
  - `internal/control/` — HTTP server on `:9090` for demo shortcuts
  - `internal/client/` — REST + Mongo client wrappers
  - Configurable via `workload/config.yaml` (committed: a 20-tenant demo config).
- Smoke script `scripts/smoke.sh` — starts server + workload with low-volume config, pings endpoints, tears down. CI-friendly.
- Rust integration test exercising the metrics + fault + quota endpoints end-to-end (no Go dependency).

**Out of scope (explicitly deferred):**
- **Performance benchmarks** — no wrk/vegeta runs as part of the milestone exit.
- **Per-operation tracing detail** (OpenTelemetry traces) — we stream a subset of `tracing::info!` events only.
- **Dashboard drill-in charts** (time-series lines) — the dashboard shows current counts, not historical graphs.
- **Auth/TLS for the admin endpoints or dashboard** — both are open and assume a local trusted environment.
- **Synthetic cold tenants for long-tail visual** (spec §10.5) — the workload generator plants real sessions only; long-tail is earned through tenant count.
- **`mongosh` tab completion for admin commands** — out of scope; HTTP admin is the control surface.
- **Go CI** — the smoke script optionally runs the Go binary if present but Rust CI does not build Go.
- **Real tracing subscriber config** — we only register our broadcast layer; log-level + env filter stays as-is.

**Concurrency contract:** `BoxMetrics` uses `AtomicU64` + `parking_lot::Mutex<Gauges>` (or `Mutex<HashMap>` from std — parking_lot not required). Writes are non-blocking (`fetch_add`). Reads (snapshot) take a read guard. The SSE broadcast channel is `async_broadcast::broadcast(1024)` — lossy on slow subscribers (old events dropped) but never blocks producers.

---

## File Structure

All paths relative to repo root `/Users/lishen/work/agentdb`.

**Created (Rust):**
- `crates/agentdb-core/src/metrics.rs` — `BoxMetrics` struct + accessors
- `crates/agentdb-api/src/admin_boxes.rs` — box-level admin endpoints (list, detail, fault, quota)
- `crates/agentdb-api/src/admin_sessions.rs` — session detail endpoint
- `crates/agentdb-api/src/stream.rs` — SSE handler + the tracing Layer + broadcast channel setup
- `crates/agentdb-dashboard/` — new crate holding static assets (index.html, styles.css, app.js) exposed via a single function returning a `tower_http::services::ServeDir`
- `crates/agentdb-dashboard/Cargo.toml`
- `crates/agentdb-dashboard/src/lib.rs`
- `crates/agentdb-dashboard/static/index.html`
- `crates/agentdb-dashboard/static/styles.css`
- `crates/agentdb-dashboard/static/app.js`

**Created (Go):**
- `workload/go.mod`
- `workload/go.sum` (committed after `go mod tidy`)
- `workload/cmd/agentdb-workload/main.go`
- `workload/internal/persona/persona.go`
- `workload/internal/persona/state_machine.go`
- `workload/internal/control/server.go`
- `workload/internal/client/rest.go`
- `workload/internal/client/mongo.go`
- `workload/config.yaml`
- `workload/README.md`

**Created (scripts):**
- `scripts/smoke.sh`

**Modified:**
- `Cargo.toml` (workspace members + new deps: `async-broadcast`, `metrics` 0.23, `tower-http` 0.6 with `fs` feature)
- `crates/agentdb-core/Cargo.toml` (new deps for metrics storage)
- `crates/agentdb-core/src/box_runtime.rs` (`Box_` gets `metrics: Arc<BoxMetrics>`, `quota: Arc<parking_lot::RwLock<Quota>>`, `fault: Arc<parking_lot::RwLock<Option<InjectedFault>>>`)
- `crates/agentdb-core/src/lib.rs` (declare metrics, re-export types)
- `crates/agentdb-core/src/primitives.rs` (bump metric counters on append and fork)
- `crates/agentdb-core/src/tiering.rs` (bump `tier_transitions` counter)
- `crates/agentdb-core/src/fork.rs` (bump `fork_created` counter)
- `crates/agentdb-api/Cargo.toml` (axum features already include SSE; add `async-broadcast`, `tower-http` with `fs`)
- `crates/agentdb-api/src/lib.rs` (declare new modules, add routes, `AppState` gets a broadcast channel handle)
- `crates/agentdb-wire/Cargo.toml` (no change expected)
- `bin/agentdb-server/src/main.rs` (wire broadcast channel + dashboard ServeDir)
- `.gitignore` (ignore `workload/agentdb-workload` build artifact)

**Responsibilities:**
- `metrics.rs` — pure data; atomic counters, one `Mutex<Gauges>` for tier counts.
- `stream.rs` — tracing Layer + SSE handler. No business logic.
- `admin_boxes.rs` / `admin_sessions.rs` — thin HTTP adapters over `BoxRegistry` + `SessionRegistry`.
- `agentdb-dashboard/lib.rs` — single public function returning a `ServeDir` rooted at `static/`.
- `static/*` — vanilla JS + Alpine.js, no build step.
- `workload/...` — pure client code. No shared types with the DB.

---

## Data model notes

### BoxMetrics

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
    pub gauges: parking_lot::RwLock<GaugeSnapshot>,
}

pub struct GaugeSnapshot {
    pub warm_sessions: u64,
    pub cold_sessions: u64,
    pub archive_sessions: u64,
    pub bytes_warm: u64,   // approximate — skipped in M7 if expensive
    pub bytes_cold: u64,
    pub total_tenants: u64,
}
```

Gauges are updated on tier-transition completion + session creation. Counters are incremented on every relevant operation.

### InjectedFault

```rust
pub struct InjectedFault {
    pub kind: FaultKind,
    pub expires_at: Instant,
}

pub enum FaultKind {
    Latency { ms: u64 },
    Error,
}
```

Request handlers check `box_.fault.read().as_ref()` at entry and apply the fault:
- `Latency`: `tokio::time::sleep(ms)` before continuing
- `Error`: return a 503 to REST, or `{ok:0, errmsg:"fault injected"}` to Mongo wire

The fault auto-expires (`expires_at` — `Instant::now() > expires_at` → fault is cleared on next check).

### Quota

```rust
pub struct Quota {
    pub max_warm_sessions: Option<u64>,
    pub max_qps: Option<u64>,
}
```

Enforcement in M7:
- `max_warm_sessions`: checked on `append_event` — if current warm_sessions gauge exceeds, return `QuotaExceeded` error. Does NOT trigger demotion (that's the lifecycle engine).
- `max_qps`: a 1-second rolling window counter; incremented per request, checked against cap. Simple fixed-window rate limit.

### SSE event schema

```rust
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum StreamEvent {
    MetricsTick { ts_ms: u64, boxes: Vec<BoxTickSnapshot> },
    TierChange { ts_ms: u64, box_id: u16, session_id: String, from: String, to: String },
    ForkCreated { ts_ms: u64, box_id: u16, parent: String, child: String },
    QuotaHit { ts_ms: u64, box_id: u16, kind: String },
    FaultInjected { ts_ms: u64, box_id: u16, kind: String, duration_s: u64 },
    QueryPlan { ts_ms: u64, box_id: u16, strategy: String, latency_ms: u64, n_returned: usize },
}
```

Events are serialized to JSON and framed as SSE `data: {...}\n\n`. Clients use `new EventSource('/api/stream')`. The Rust `tracing_subscriber::Layer` inspects each event's span name/fields; known events are mapped to `StreamEvent` and sent on the broadcast channel.

---

## Task 1: `BoxMetrics` + plumbing into `Box_`

**Files:**
- Modify: `Cargo.toml` (workspace deps)
- Modify: `crates/agentdb-core/Cargo.toml`
- Create: `crates/agentdb-core/src/metrics.rs`
- Modify: `crates/agentdb-core/src/box_runtime.rs`
- Modify: `crates/agentdb-core/src/lib.rs`

- [ ] **Step 1: Workspace deps**

Edit `Cargo.toml` workspace deps to add:
```toml
parking_lot = "0.12"
async-broadcast = "0.7"
```

Also extend the existing `reqwest` workspace dep to include the `stream` feature so the SSE integration test can read the response body chunk-by-chunk:
```toml
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls", "stream"] }
```

(We do NOT add the `metrics` crate — keep metrics in-process with `AtomicU64` to avoid the complexity of Prometheus exporters.)

- [ ] **Step 2: agentdb-core deps**

Edit `crates/agentdb-core/Cargo.toml`. In `[dependencies]`:
```toml
parking_lot = { workspace = true }
```

- [ ] **Step 3: Write metrics.rs**

Save `crates/agentdb-core/src/metrics.rs`:
```rust
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Per-box counters and gauges. All writes are non-blocking via atomics; the
/// single `RwLock` protects the tier/byte gauge bundle.
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
    pub gauges: RwLock<GaugeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GaugeSnapshot {
    pub warm_sessions: u64,
    pub cold_sessions: u64,
    pub archive_sessions: u64,
    pub total_tenants: u64,
}

impl BoxMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_query_find(&self) {
        self.queries_find.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_query_aggregate(&self) {
        self.queries_aggregate.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_append(&self) {
        self.appends.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fork(&self) {
        self.forks.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_tier_transition(&self) {
        self.tier_transitions.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_quota_hit(&self) {
        self.quota_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fault(&self) {
        self.faults_injected.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the gauge for a tier (typically on session creation or transition).
    pub fn bump_tier(&self, tier: crate::types::Tier) {
        let mut g = self.gauges.write();
        match tier {
            crate::types::Tier::Warm => g.warm_sessions += 1,
            crate::types::Tier::Cold => g.cold_sessions += 1,
            crate::types::Tier::Archive => g.archive_sessions += 1,
        }
    }
    pub fn drop_tier(&self, tier: crate::types::Tier) {
        let mut g = self.gauges.write();
        match tier {
            crate::types::Tier::Warm => g.warm_sessions = g.warm_sessions.saturating_sub(1),
            crate::types::Tier::Cold => g.cold_sessions = g.cold_sessions.saturating_sub(1),
            crate::types::Tier::Archive => g.archive_sessions = g.archive_sessions.saturating_sub(1),
        }
    }
    pub fn set_tenants(&self, n: u64) {
        self.gauges.write().total_tenants = n;
    }

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
            gauges: self.gauges.read().clone(),
        }
    }
}

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
    pub gauges: GaugeSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Tier;

    #[test]
    fn counters_increment() {
        let m = BoxMetrics::new();
        m.record_append();
        m.record_append();
        m.record_fork();
        let s = m.snapshot();
        assert_eq!(s.appends, 2);
        assert_eq!(s.forks, 1);
    }

    #[test]
    fn tier_gauges_reflect_bump_and_drop() {
        let m = BoxMetrics::new();
        m.bump_tier(Tier::Warm);
        m.bump_tier(Tier::Warm);
        m.bump_tier(Tier::Cold);
        m.drop_tier(Tier::Warm);
        let s = m.snapshot();
        assert_eq!(s.gauges.warm_sessions, 1);
        assert_eq!(s.gauges.cold_sessions, 1);
        assert_eq!(s.gauges.archive_sessions, 0);
    }

    #[test]
    fn drop_tier_saturates_at_zero() {
        let m = BoxMetrics::new();
        m.drop_tier(Tier::Warm);
        assert_eq!(m.snapshot().gauges.warm_sessions, 0);
    }
}
```

- [ ] **Step 4: Extend `Box_`**

In `crates/agentdb-core/src/box_runtime.rs`, extend imports:
```rust
use crate::metrics::BoxMetrics;
use parking_lot::RwLock;
use std::time::Instant;
```

Add new types:
```rust
#[derive(Debug, Clone, Copy)]
pub enum FaultKind {
    Latency { ms: u64 },
    Error,
}

#[derive(Debug, Clone, Copy)]
pub struct InjectedFault {
    pub kind: FaultKind,
    pub expires_at: Instant,
}

impl InjectedFault {
    pub fn is_active(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Quota {
    pub max_warm_sessions: Option<u64>,
    pub max_qps: Option<u64>,
}
```

Extend the `Box_` struct:
```rust
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
    pub metrics: Arc<BoxMetrics>,
    pub fault: RwLock<Option<InjectedFault>>,
    pub quota: RwLock<Quota>,
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
        }
    }

    /// Convenience: if a fault is active, consume its effect (sleep for latency
    /// faults, return true for error faults). Returns true if the caller
    /// should short-circuit with an error.
    pub async fn apply_fault(&self) -> bool {
        let f = { self.fault.read().clone() };
        if let Some(f) = f {
            if f.is_active() {
                match f.kind {
                    FaultKind::Latency { ms } => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
                        false
                    }
                    FaultKind::Error => true,
                }
            } else {
                false
            }
        } else {
            false
        }
    }
    // ... keep existing indexes_built method unchanged
}
```

The existing `indexes_built` method stays as-is.

- [ ] **Step 5: Declare in lib.rs, re-export**

Edit `crates/agentdb-core/src/lib.rs`. Add `pub mod metrics;` and:
```rust
pub use metrics::{BoxMetrics, GaugeSnapshot, MetricsSnapshot};
pub use box_runtime::{Box_, BoxRegistry, FaultKind, InjectedFault, Quota};
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p agentdb-core metrics::tests`
Expected: 3 passed.

Run: `cargo test --workspace` — 158 + 3 = 161 tests pass.

- [ ] **Step 7: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add Cargo.toml crates/agentdb-core
git commit -m "feat(core): BoxMetrics, InjectedFault, Quota on Box_"
```

---

## Task 2: Hook metric counters into existing primitives

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs` (append, append_and_fetch)
- Modify: `crates/agentdb-core/src/fork.rs`
- Modify: `crates/agentdb-core/src/tiering.rs`
- Modify: `crates/agentdb-core/src/registry.rs` (tenant count gauge on get_or_mint)

**Context:** Incrementing counters shouldn't hurt performance (AtomicU64 is fast). Add them to the existing functions at the correct edges.

- [ ] **Step 1: append / append_and_fetch → `record_append`, `record_insert`**

In `primitives.rs::append_and_fetch`, after the append succeeds:
```rust
box_.metrics.record_append();
```
(Insert `box_` as the routed box from `registry.route_tenant(tenant_external)`. The Mongo wire's `insert` handler also calls append, so `record_insert` goes there — to be added in a later task alongside the aggregate counter.)

- [ ] **Step 2: fork → `record_fork`**

In `fork.rs::fork_session`, after `get_or_mint_with_fork` succeeds:
```rust
let box_ = registry.route_tenant(tenant_external);
box_.metrics.record_fork();
```

- [ ] **Step 3: cold_transition / reactivate → `record_tier_transition` + gauge bumps**

In `tiering.rs::cold_transition`, after the write_batch succeeds:
```rust
// metrics: bump transitions + shift gauge
// `box_` must be threaded in or looked up via registry; since cold_transition
// already takes the MutexGuard from the box's lock, either thread Arc<Box_>
// explicitly (preferred) or look up via BoxRegistry:
// registry.box_by_id(session.box_id).map(|b| {
//     b.metrics.record_tier_transition();
//     b.metrics.drop_tier(Tier::Warm);
//     b.metrics.bump_tier(Tier::Cold);
// });
```

Since `cold_transition` doesn't currently take a registry or box handle, the cleanest path is to extend its signature to accept `&Arc<Box_>` (via the caller — admin endpoint, lifecycle engine). Do that plumbing. Every call site already has access to the box.

Same pattern for `reactivate`.

- [ ] **Step 4: tenant count**

In `registry.rs::TenantRegistry::get_or_mint`, after the write_batch in the mint path:
- No box handle in scope for TenantRegistry. Leave it: the dashboard's snapshot endpoint will compute `total_tenants` at query time by calling `tenants.list_in_box(box_id).len()` instead of reading a gauge. Simpler, always accurate.

Remove `total_tenants` from `GaugeSnapshot` and the `set_tenants` method; compute on demand in the admin endpoint.

- [ ] **Step 5: Run tests**

Run: `cargo test --workspace` — all pass (no test count change).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core
git commit -m "feat(core): wire metric counters into primitives, fork, and tier transitions"
```

---

## Task 3: Admin endpoints — list, detail, inject_fault, quota

**Files:**
- Create: `crates/agentdb-api/src/admin_boxes.rs`
- Create: `crates/agentdb-api/src/admin_sessions.rs`
- Modify: `crates/agentdb-api/src/lib.rs`

**Context:** Four endpoints plus two detail views. Each handler is thin — read from BoxRegistry / metrics / session records, build a JSON response.

- [ ] **Step 1: admin_boxes.rs**

Save:
```rust
use std::time::{Duration, Instant};

use agentdb_core::{BoxRegistry, FaultKind, InjectedFault, Quota, TenantRegistry};
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

#[derive(Serialize)]
pub struct BoxListItem {
    pub id: u16,
    pub num_tenants: u64,
    pub metrics: agentdb_core::MetricsSnapshot,
    pub quota: QuotaView,
    pub fault_active: bool,
}

#[derive(Serialize)]
pub struct QuotaView {
    pub max_warm_sessions: Option<u64>,
    pub max_qps: Option<u64>,
}

pub async fn list_boxes(
    State(state): State<AppState>,
) -> Result<Json<Vec<BoxListItem>>, ApiError> {
    let mut out = Vec::new();
    for i in 0..state.registry.num_boxes() {
        let Some(b) = state.registry.box_by_id(i) else { continue };
        let tenants = state.tenants.list_in_box(i)?.len() as u64;
        let metrics = b.metrics.snapshot();
        let q = *b.quota.read();
        let fault_active = b.fault.read().as_ref().map(|f| f.is_active()).unwrap_or(false);
        out.push(BoxListItem {
            id: i,
            num_tenants: tenants,
            metrics,
            quota: QuotaView {
                max_warm_sessions: q.max_warm_sessions,
                max_qps: q.max_qps,
            },
            fault_active,
        });
    }
    Ok(Json(out))
}

pub async fn get_box(
    State(state): State<AppState>,
    Path(id): Path<u16>,
) -> Result<Json<BoxListItem>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| ApiError::BadRequest(format!("box {id} not found")))?;
    let tenants = state.tenants.list_in_box(id)?.len() as u64;
    let metrics = b.metrics.snapshot();
    let q = *b.quota.read();
    let fault_active = b.fault.read().as_ref().map(|f| f.is_active()).unwrap_or(false);
    Ok(Json(BoxListItem {
        id,
        num_tenants: tenants,
        metrics,
        quota: QuotaView {
            max_warm_sessions: q.max_warm_sessions,
            max_qps: q.max_qps,
        },
        fault_active,
    }))
}

#[derive(Deserialize)]
pub struct InjectFaultRequest {
    pub kind: String,       // "latency" | "error"
    pub duration_s: u64,
    #[serde(default)]
    pub latency_ms: Option<u64>,
}

#[derive(Serialize)]
pub struct InjectFaultResponse {
    pub ok: bool,
    pub expires_in_s: u64,
}

pub async fn inject_fault(
    State(state): State<AppState>,
    Path(id): Path<u16>,
    Json(body): Json<InjectFaultRequest>,
) -> Result<Json<InjectFaultResponse>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| ApiError::BadRequest(format!("box {id} not found")))?;
    let kind = match body.kind.to_ascii_lowercase().as_str() {
        "latency" => FaultKind::Latency {
            ms: body.latency_ms.unwrap_or(200),
        },
        "error" => FaultKind::Error,
        other => {
            return Err(ApiError::BadRequest(format!(
                "unknown fault kind {other}; expected 'latency' or 'error'"
            )));
        }
    };
    let expires_at = Instant::now() + Duration::from_secs(body.duration_s);
    *b.fault.write() = Some(InjectedFault { kind, expires_at });
    b.metrics.record_fault();
    Ok(Json(InjectFaultResponse {
        ok: true,
        expires_in_s: body.duration_s,
    }))
}

#[derive(Deserialize)]
pub struct QuotaRequest {
    #[serde(default)]
    pub max_warm_sessions: Option<u64>,
    #[serde(default)]
    pub max_qps: Option<u64>,
}

pub async fn set_quota(
    State(state): State<AppState>,
    Path(id): Path<u16>,
    Json(body): Json<QuotaRequest>,
) -> Result<Json<QuotaView>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| ApiError::BadRequest(format!("box {id} not found")))?;
    let mut q = b.quota.write();
    q.max_warm_sessions = body.max_warm_sessions;
    q.max_qps = body.max_qps;
    Ok(Json(QuotaView {
        max_warm_sessions: q.max_warm_sessions,
        max_qps: q.max_qps,
    }))
}
```

- [ ] **Step 2: admin_sessions.rs**

Save:
```rust
use agentdb_core::types::Session;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct SessionQuery {
    pub tenant_id: String,
}

#[derive(Serialize)]
pub struct SessionDetail {
    pub session: Session,
    pub parent_external_id: Option<String>,
}

pub async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(q): Query<SessionQuery>,
) -> Result<Json<SessionDetail>, ApiError> {
    let box_ = state.registry.route_tenant(&q.tenant_id);
    let tenant = state
        .tenants
        .lookup(box_.id, &q.tenant_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("tenant {} not found", q.tenant_id)))?;
    let session = state
        .sessions
        .lookup(box_.id, tenant.internal_id, &session_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("session {session_id} not found")))?;
    // If forked, resolve the parent's external id for the UI.
    let parent_external_id = match &session.forked_from {
        Some(fork) => state
            .sessions
            .lookup_by_internal(box_.id, tenant.internal_id, fork.parent_session_internal_id)?
            .map(|s| s.external_id),
        None => None,
    };
    Ok(Json(SessionDetail {
        session,
        parent_external_id,
    }))
}
```

- [ ] **Step 3: Wire routes in lib.rs**

Edit `crates/agentdb-api/src/lib.rs`. Add module declarations:
```rust
pub mod admin_boxes;
pub mod admin_sessions;
```

In `router()`, add routes:
```rust
.route("/api/admin/boxes", axum::routing::get(admin_boxes::list_boxes))
.route("/api/admin/boxes/:id", axum::routing::get(admin_boxes::get_box))
.route(
    "/api/admin/boxes/:id/inject_fault",
    axum::routing::post(admin_boxes::inject_fault),
)
.route(
    "/api/admin/boxes/:id/quota",
    axum::routing::post(admin_boxes::set_quota),
)
.route(
    "/api/admin/sessions/:session_id",
    axum::routing::get(admin_sessions::get_session),
)
```

- [ ] **Step 4: Integration tests**

Append to `crates/agentdb-api/tests/http.rs`:
```rust
#[tokio::test]
#[serial]
async fn admin_boxes_list_reports_counters_and_tenant_counts() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();
    for i in 1..=3 {
        client
            .post(format!("{base}/api/tenants/t-{i}/sessions/s/append_and_fetch"))
            .json(&serde_json::json!({"event": {}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let boxes: serde_json::Value = client
        .get(format!("{base}/api/admin/boxes"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = boxes.as_array().unwrap();
    assert_eq!(arr.len(), 4); // AGENTDB_BOXES default = 4
    let total_appends: u64 = arr
        .iter()
        .map(|b| b["metrics"]["appends"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total_appends, 3);
    let total_tenants: u64 = arr
        .iter()
        .map(|b| b["num_tenants"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total_tenants, 3);
}

#[tokio::test]
#[serial]
async fn admin_inject_fault_latency_slows_subsequent_writes() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();
    // Seed so we know which box
    client
        .post(format!("{base}/api/tenants/t-fault/sessions/s/append_and_fetch"))
        .json(&serde_json::json!({"event": {}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // Find the box
    let boxes: serde_json::Value = client
        .get(format!("{base}/api/admin/boxes"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let box_id = boxes
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["num_tenants"].as_u64().unwrap_or(0) >= 1)
        .and_then(|b| b["id"].as_u64())
        .expect("expected at least one active box");
    // Inject 150ms latency for 5s
    client
        .post(format!("{base}/api/admin/boxes/{box_id}/inject_fault"))
        .json(&serde_json::json!({"kind":"latency","duration_s":5,"latency_ms":150}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // Measure the next append — should take at least ~150ms
    let t = std::time::Instant::now();
    client
        .post(format!("{base}/api/tenants/t-fault/sessions/s/append_and_fetch"))
        .json(&serde_json::json!({"event": {}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let elapsed = t.elapsed();
    assert!(elapsed.as_millis() >= 100, "expected latency fault, got {elapsed:?}");
}
```

The latency-fault test requires the append_and_fetch handler to call `box_.apply_fault().await` before writing. That integration happens in Task 5.

For THIS task we just verify the list/detail/inject endpoints persist the fault flag. Replace the latency test with:
```rust
#[tokio::test]
#[serial]
async fn admin_inject_fault_marks_box_fault_active() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/api/admin/boxes/0/inject_fault"))
        .json(&serde_json::json!({"kind":"latency","duration_s":5,"latency_ms":100}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let box0: serde_json::Value = client
        .get(format!("{base}/api/admin/boxes/0"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(box0["fault_active"], true);
}

#[tokio::test]
#[serial]
async fn admin_set_quota_persists() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/api/admin/boxes/0/quota"))
        .json(&serde_json::json!({"max_warm_sessions": 100}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let box0: serde_json::Value = client
        .get(format!("{base}/api/admin/boxes/0"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(box0["quota"]["max_warm_sessions"], 100);
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p agentdb-api --test http`
Expected: 6 passed (4 prior + 2 new for boxes list + fault active + quota).

Actually the plan adds 3 new tests to the snippet; adjust expectation to 4 prior + 3 new = 7.

Run: `cargo test --workspace` — all pass.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-api
git commit -m "feat(api): admin endpoints for boxes listing, fault injection, quota, session detail"
```

---

## Task 4: Fault + quota enforcement in REST + Mongo wire handlers

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs` (`append_and_fetch`: fault + quota check)
- Modify: `crates/agentdb-wire/src/handlers/find.rs`, `aggregate.rs`, `insert.rs` (route query counts + fault check)

**Context:** Before doing the actual work, the handler calls `box_.apply_fault().await`. If it returns `true` (error fault), the handler returns a `QuotaExceeded`-style error. Also checks the soft cap on warm_sessions before a new session mint.

- [ ] **Step 1: `append_and_fetch` fault + quota**

In `crates/agentdb-core/src/primitives.rs::append_and_fetch`, at the top of the function (after routing, before `route_tenant`'s box is available, insert):
```rust
let box_ = registry.route_tenant(tenant_external);
if box_.apply_fault().await {
    box_.metrics.record_fault();
    return Err(Error::Invalid(format!(
        "box {} has an injected error fault",
        box_.id
    )));
}
// Quota check on warm sessions
{
    let q = *box_.quota.read();
    if let Some(max) = q.max_warm_sessions {
        let g = box_.metrics.gauges.read().clone();
        if g.warm_sessions >= max {
            box_.metrics.record_quota_hit();
            return Err(Error::Invalid(format!(
                "box {} warm_sessions quota {max} exceeded",
                box_.id
            )));
        }
    }
}
```

(rely on existing lock + append flow after.)

- [ ] **Step 2: Mongo wire: count query types**

In `crates/agentdb-wire/src/handlers/find.rs::find_response`, at entry:
```rust
let box_ = registry.route_tenant(db);
box_.metrics.record_query_find();
```

In `crates/agentdb-wire/src/handlers/aggregate.rs::aggregate_response`, at entry:
```rust
let box_ = registry.route_tenant(db);
box_.metrics.record_query_aggregate();
```

In `crates/agentdb-wire/src/handlers/insert.rs::insert_response`, at entry:
```rust
let box_ = registry.route_tenant(db);
box_.metrics.record_insert();
```

For aggregate/find, ALSO apply the fault check:
```rust
if box_.apply_fault().await {
    return Err(WireError::Protocol("injected fault".into()));
}
```

- [ ] **Step 3: Add fault test that verifies error fault blocks writes**

Append to `crates/agentdb-api/tests/http.rs`:
```rust
#[tokio::test]
#[serial]
async fn error_fault_blocks_append_and_fetch() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/api/admin/boxes/0/inject_fault"))
        .json(&serde_json::json!({"kind":"error","duration_s":5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    // Find a tenant routed to box 0 — just try multiple until the error hits.
    let mut hit = false;
    for i in 0..32 {
        let resp = client
            .post(format!(
                "{base}/api/tenants/t-{i}/sessions/s/append_and_fetch"
            ))
            .json(&serde_json::json!({"event": {}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap();
        if !resp.status().is_success() {
            hit = true;
            break;
        }
    }
    assert!(hit, "expected at least one tenant to route to box 0 and be blocked");
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --workspace`
Expected: all prior + 1 new (error fault).

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add crates/agentdb-core crates/agentdb-wire crates/agentdb-api
git commit -m "feat: fault + quota enforcement on append_and_fetch and wire handlers"
```

---

## Task 5: SSE `/api/stream` endpoint + broadcast channel

**Files:**
- Modify: `crates/agentdb-api/Cargo.toml` (add `async-broadcast`)
- Create: `crates/agentdb-api/src/stream.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (AppState gets a broadcast sender, register route)
- Modify: `crates/agentdb-core/src/primitives.rs`, `fork.rs`, `tiering.rs` (emit StreamEvent via a passed-in sender OR via `tracing::info!` with known targets)

**Context:** Decouple producers from consumers. Easiest: every event emission calls `tracing::info!` with a target like `"agentdb.stream.fork_created"` and structured fields. A `tracing_subscriber::Layer` intercepts events with those targets and forwards a typed `StreamEvent` on the broadcast channel.

Alternative: pass the sender into `AppState` and call it directly from handlers. Simpler, less clever — go with this.

- [ ] **Step 1: Add dep**

Edit `crates/agentdb-api/Cargo.toml`:
```toml
async-broadcast = { workspace = true }
```

- [ ] **Step 2: Write stream.rs**

Save `crates/agentdb-api/src/stream.rs`:
```rust
use std::convert::Infallible;
use std::time::{SystemTime, UNIX_EPOCH};

use async_broadcast::{Receiver, Sender};
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::Stream;
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum StreamEvent {
    MetricsTick {
        ts_ms: u64,
        boxes: Vec<TickBox>,
    },
    TierChange {
        ts_ms: u64,
        box_id: u16,
        session_id: String,
        from: String,
        to: String,
    },
    ForkCreated {
        ts_ms: u64,
        box_id: u16,
        parent: String,
        child: String,
    },
    QuotaHit {
        ts_ms: u64,
        box_id: u16,
        kind: String,
    },
    FaultInjected {
        ts_ms: u64,
        box_id: u16,
        kind: String,
        duration_s: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickBox {
    pub id: u16,
    pub warm_sessions: u64,
    pub cold_sessions: u64,
    pub appends: u64,
    pub forks: u64,
    pub fault_active: bool,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
pub struct StreamBus {
    pub tx: Sender<StreamEvent>,
}

impl StreamBus {
    pub fn new(capacity: usize) -> Self {
        let (mut tx, _) = async_broadcast::broadcast(capacity);
        tx.set_overflow(true); // overwrite oldest if subscribers are slow
        Self { tx }
    }

    pub fn subscribe(&self) -> Receiver<StreamEvent> {
        self.tx.new_receiver()
    }

    pub fn emit(&self, event: StreamEvent) {
        let _ = self.tx.try_broadcast(event);
    }
}

pub async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    use futures_util::StreamExt;
    let rx = state.stream.subscribe();
    let stream = rx.map(|ev| {
        let data = serde_json::to_string(&ev).unwrap_or_else(|_| "{}".into());
        Ok(Event::default().data(data))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
```

- [ ] **Step 3: Thread StreamBus through AppState**

Edit `crates/agentdb-api/src/lib.rs`. `AppState` gets a new field:
```rust
use crate::stream::StreamBus;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
    pub stream: StreamBus,
}
```

Declare module: `pub mod stream;`.

Add route:
```rust
.route("/api/stream", axum::routing::get(stream::sse_handler))
```

Update every `AppState { ... }` construction site in the workspace to pass a new `StreamBus`. In tests:
```rust
let stream = StreamBus::new(1024);
let http_state = AppState { registry, tenants, sessions, stream };
```

- [ ] **Step 4: Emit events from key endpoints**

`admin_boxes.rs::inject_fault` — after persisting the fault, emit:
```rust
state.stream.emit(StreamEvent::FaultInjected {
    ts_ms: now_ms(),
    box_id: id,
    kind: body.kind.to_ascii_lowercase(),
    duration_s: body.duration_s,
});
```

`fork.rs` (api) — after fork_session succeeds, emit:
```rust
state.stream.emit(StreamEvent::ForkCreated {
    ts_ms: now_ms(),
    box_id: box_.id,
    parent: parent_session_id.clone(),
    child: child.external_id.clone(),
});
```

`admin.rs::force_tier` — after tier transition, emit:
```rust
state.stream.emit(StreamEvent::TierChange {
    ts_ms: now_ms(),
    box_id: box_.id,
    session_id: session.external_id.clone(),
    from: before_tier_str.into(),
    to: tier_str.into(),
});
```

Where `before_tier_str` is captured at the start of the handler (before the transition).

- [ ] **Step 5: Periodic MetricsTick**

In `bin/agentdb-server/src/main.rs`, spawn a task that ticks every 1s and emits a `MetricsTick` summarizing all boxes:
```rust
let bus = state_stream.clone();
let reg = registry.clone();
tokio::spawn(async move {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let mut boxes = Vec::new();
        for i in 0..reg.num_boxes() {
            if let Some(b) = reg.box_by_id(i) {
                let s = b.metrics.snapshot();
                boxes.push(agentdb_api::stream::TickBox {
                    id: i,
                    warm_sessions: s.gauges.warm_sessions,
                    cold_sessions: s.gauges.cold_sessions,
                    appends: s.appends,
                    forks: s.forks,
                    fault_active: b.fault.read().as_ref().map(|f| f.is_active()).unwrap_or(false),
                });
            }
        }
        bus.emit(agentdb_api::stream::StreamEvent::MetricsTick {
            ts_ms: agentdb_api::stream::now_ms(),
            boxes,
        });
    }
});
```

- [ ] **Step 6: Integration test**

Append to `crates/agentdb-api/tests/http.rs`:
```rust
#[tokio::test]
#[serial]
async fn sse_stream_receives_tier_change_event_on_force_cold() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::builder()
        .build()
        .unwrap();

    // Seed a session.
    client
        .post(format!("{base}/api/tenants/t-sse/sessions/s/append_and_fetch"))
        .json(&serde_json::json!({"event":{}, "fetch_last_n":0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Connect to SSE.
    let resp = client.get(format!("{base}/api/stream")).send().await.unwrap();
    assert!(resp.status().is_success());
    let mut body = resp.bytes_stream();

    // Trigger tier change.
    let trigger = client.clone();
    let base2 = base.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        trigger
            .post(format!("{base2}/api/admin/tier/force"))
            .json(&serde_json::json!({
                "tenant_id": "t-sse",
                "session_id": "s",
                "target": "Cold"
            }))
            .send()
            .await
            .unwrap();
    });

    // Read up to 5s of SSE lines; look for a TierChange event.
    use futures_util::StreamExt;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut saw_tier_change = false;
    let mut buf = Vec::new();
    while std::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            body.next(),
        )
        .await;
        let Ok(Some(Ok(bytes))) = chunk else { continue };
        buf.extend_from_slice(&bytes);
        let text = String::from_utf8_lossy(&buf);
        if text.contains("\"kind\":\"TierChange\"") {
            saw_tier_change = true;
            break;
        }
    }
    assert!(saw_tier_change, "never saw TierChange SSE event");
}
```

- [ ] **Step 7: Run tests**

Run: `cargo test -p agentdb-api --test http -- --test-threads=1`
Expected: all pass including the new SSE test.

Run: `cargo test --workspace` — all pass.

- [ ] **Step 8: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add .
git commit -m "feat(api): SSE /api/stream endpoint with TierChange / ForkCreated / FaultInjected events"
```

---

## Task 6: `agentdb-dashboard` crate (static assets + route)

**Files:**
- Create: `crates/agentdb-dashboard/Cargo.toml`
- Create: `crates/agentdb-dashboard/src/lib.rs`
- Create: `crates/agentdb-dashboard/static/index.html`
- Create: `crates/agentdb-dashboard/static/styles.css`
- Create: `crates/agentdb-dashboard/static/app.js`
- Modify: `Cargo.toml` (add to workspace members + `tower-http = { version = "0.6", features = ["fs"] }`)
- Modify: `crates/agentdb-api/Cargo.toml` (depend on `agentdb-dashboard` + `tower-http`)
- Modify: `crates/agentdb-api/src/lib.rs` (nest the dashboard router)

- [ ] **Step 1: Workspace + cargo**

Edit `Cargo.toml` workspace members: add `"crates/agentdb-dashboard"`.

Workspace deps: add `tower-http = { version = "0.6", features = ["fs"] }`.

Create `crates/agentdb-dashboard/Cargo.toml`:
```toml
[package]
name = "agentdb-dashboard"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
axum = { workspace = true }
tower-http = { workspace = true }
```

- [ ] **Step 2: Static assets**

Create `crates/agentdb-dashboard/static/index.html`:
```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>AgentDB Dashboard</title>
    <link rel="stylesheet" href="/dashboard/styles.css" />
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.13.5/dist/cdn.min.js"></script>
    <script defer src="/dashboard/app.js"></script>
  </head>
  <body>
    <header>
      <h1>AgentDB Dashboard</h1>
      <div class="controls">
        <button onclick="demoFork()">Fork demo-parent</button>
        <button onclick="demoFault(0)">Inject fault box 0</button>
        <button onclick="demoCold()">Cool demo session</button>
      </div>
    </header>
    <main x-data="dashboard()" x-init="init()">
      <section class="top-bar">
        <div>Boxes: <span x-text="boxes.length"></span></div>
        <div>Tenants: <span x-text="totalTenants"></span></div>
        <div>Appends: <span x-text="totalAppends"></span></div>
        <div>Forks: <span x-text="totalForks"></span></div>
      </section>
      <section class="box-grid">
        <template x-for="b in boxes" :key="b.id">
          <div class="box-tile" :class="{ fault: b.fault_active }">
            <header>box #<span x-text="b.id"></span></header>
            <div class="counts">
              <span class="warm" x-text="b.warm_sessions || 0"></span> warm
              /
              <span class="cold" x-text="b.cold_sessions || 0"></span> cold
            </div>
            <div class="dots">
              <template x-for="i in (b.warm_sessions || 0)">
                <span class="dot warm"></span>
              </template>
              <template x-for="i in (b.cold_sessions || 0)">
                <span class="dot cold"></span>
              </template>
            </div>
          </div>
        </template>
      </section>
      <section class="event-feed">
        <h2>Events</h2>
        <ul>
          <template x-for="ev in events.slice(-20).reverse()" :key="ev._id">
            <li>
              <span class="ts" x-text="new Date(ev.ts_ms).toLocaleTimeString()"></span>
              <span class="kind" x-text="ev.kind"></span>
              <span class="detail" x-text="describe(ev)"></span>
            </li>
          </template>
        </ul>
      </section>
    </main>
  </body>
</html>
```

Create `crates/agentdb-dashboard/static/styles.css`:
```css
* { box-sizing: border-box; }
body { margin: 0; font-family: ui-monospace, monospace; background: #0b0e14; color: #cdd6f4; }
header { padding: 12px 20px; border-bottom: 1px solid #313244; display: flex; justify-content: space-between; align-items: center; }
h1 { margin: 0; font-size: 1.1rem; font-weight: 600; }
.controls button { background: #89b4fa; color: #11111b; border: none; padding: 6px 10px; margin-left: 6px; border-radius: 4px; cursor: pointer; font-family: inherit; }
main { padding: 20px; }
.top-bar { display: flex; gap: 24px; margin-bottom: 24px; font-size: 0.9rem; }
.box-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }
.box-tile { background: #181825; padding: 10px; border: 1px solid #313244; border-radius: 6px; min-height: 120px; }
.box-tile.fault { background: #5a0d0d; border-color: #f38ba8; }
.box-tile header { padding: 0; border: none; margin-bottom: 8px; font-size: 0.85rem; color: #89b4fa; }
.counts { font-size: 0.8rem; margin-bottom: 8px; color: #a6adc8; }
.warm { color: #a6e3a1; }
.cold { color: #89dceb; }
.dots { display: flex; flex-wrap: wrap; gap: 3px; }
.dot { width: 8px; height: 8px; border-radius: 50%; }
.dot.warm { background: #a6e3a1; }
.dot.cold { background: #89dceb; }
.event-feed ul { list-style: none; padding: 0; font-size: 0.8rem; max-height: 300px; overflow-y: auto; background: #181825; border: 1px solid #313244; border-radius: 6px; }
.event-feed li { padding: 4px 10px; border-bottom: 1px solid #313244; display: flex; gap: 12px; }
.event-feed li:last-child { border-bottom: none; }
.event-feed .ts { color: #6c7086; }
.event-feed .kind { color: #f9e2af; }
.event-feed .detail { color: #cdd6f4; }
```

Create `crates/agentdb-dashboard/static/app.js`:
```js
function dashboard() {
  return {
    boxes: [],
    events: [],
    totalAppends: 0,
    totalForks: 0,
    totalTenants: 0,
    _nextId: 0,

    async init() {
      await this.refreshBoxes();
      this.connectStream();
      setInterval(() => this.refreshBoxes(), 5000);
    },

    async refreshBoxes() {
      try {
        const resp = await fetch('/api/admin/boxes');
        const data = await resp.json();
        this.boxes = data.map(b => ({
          id: b.id,
          warm_sessions: (b.metrics && b.metrics.gauges && b.metrics.gauges.warm_sessions) || 0,
          cold_sessions: (b.metrics && b.metrics.gauges && b.metrics.gauges.cold_sessions) || 0,
          fault_active: b.fault_active,
          num_tenants: b.num_tenants,
          appends: (b.metrics && b.metrics.appends) || 0,
          forks: (b.metrics && b.metrics.forks) || 0,
        }));
        this.totalTenants = this.boxes.reduce((s, b) => s + b.num_tenants, 0);
        this.totalAppends = this.boxes.reduce((s, b) => s + b.appends, 0);
        this.totalForks = this.boxes.reduce((s, b) => s + b.forks, 0);
      } catch (err) {
        console.error('refreshBoxes', err);
      }
    },

    connectStream() {
      const src = new EventSource('/api/stream');
      src.onmessage = (e) => {
        try {
          const ev = JSON.parse(e.data);
          ev._id = this._nextId++;
          this.events.push(ev);
          if (this.events.length > 200) this.events.shift();
          if (ev.kind === 'MetricsTick') {
            for (const tb of ev.boxes) {
              const b = this.boxes.find(x => x.id === tb.id);
              if (b) {
                b.warm_sessions = tb.warm_sessions;
                b.cold_sessions = tb.cold_sessions;
                b.fault_active = tb.fault_active;
                b.appends = tb.appends;
                b.forks = tb.forks;
              }
            }
          }
        } catch (err) {
          console.error('sse parse', err);
        }
      };
      src.onerror = () => console.warn('sse error');
    },

    describe(ev) {
      switch (ev.kind) {
        case 'TierChange': return `${ev.session_id} ${ev.from} → ${ev.to} (box ${ev.box_id})`;
        case 'ForkCreated': return `${ev.parent} → ${ev.child} (box ${ev.box_id})`;
        case 'FaultInjected': return `box ${ev.box_id} ${ev.kind} for ${ev.duration_s}s`;
        case 'QuotaHit': return `box ${ev.box_id} quota ${ev.kind}`;
        case 'MetricsTick': return `${ev.boxes.length} boxes`;
        default: return JSON.stringify(ev);
      }
    },
  };
}

async function demoFork() {
  await fetch('/api/tenants/t-demo/sessions/agent/append_and_fetch', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({event:{text:'hello'}, fetch_last_n:0}),
  });
  await fetch('/api/sessions/agent/fork', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({tenant_id: 't-demo', new_session_id: 'subagent-' + Date.now()}),
  });
}
async function demoFault(boxId) {
  await fetch(`/api/admin/boxes/${boxId}/inject_fault`, {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({kind:'latency', duration_s:10, latency_ms:300}),
  });
}
async function demoCold() {
  await fetch('/api/admin/tier/force', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({tenant_id:'t-demo', session_id:'agent', target:'Cold'}),
  });
}
```

- [ ] **Step 3: lib.rs for the dashboard crate**

Save `crates/agentdb-dashboard/src/lib.rs`:
```rust
use std::path::PathBuf;

use axum::Router;
use tower_http::services::ServeDir;

/// Return a sub-router that serves the dashboard's static assets under the
/// caller-chosen mount prefix.
pub fn router() -> Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    Router::new().fallback_service(ServeDir::new(static_dir))
}
```

- [ ] **Step 4: Wire into agentdb-api**

Edit `crates/agentdb-api/Cargo.toml`:
```toml
agentdb-dashboard = { path = "../agentdb-dashboard" }
```

Edit `crates/agentdb-api/src/lib.rs`. In `router()`, nest the dashboard:
```rust
.nest_service("/dashboard", agentdb_dashboard::router())
```

- [ ] **Step 5: Smoke-test**

Run: `cargo build -p agentdb-server`
Expected: clean.

Run: `cargo test --workspace` — all pass.

The dashboard itself is not unit-tested; it's exercised by the smoke script in Task 10.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all && cargo fmt --all --check
git add .
git commit -m "feat(dashboard): static HTML+CSS+JS + mount at /dashboard"
```

---

## Task 7: Scaffold Go workload module

**Files:**
- Create: `workload/go.mod`, `workload/go.sum`, `workload/README.md`
- Create: `workload/cmd/agentdb-workload/main.go` (stub)
- Create: `workload/internal/persona/persona.go` (stub)
- Create: `workload/internal/client/rest.go` (stub)
- Create: `workload/config.yaml`
- Modify: `.gitignore` (workload binary)

- [ ] **Step 1: go.mod**

Save `workload/go.mod`:
```
module github.com/shenli/agentdb/workload

go 1.22

require (
	go.mongodb.org/mongo-driver v1.17.1
	gopkg.in/yaml.v3 v3.0.1
)
```

Run `cd workload && go mod tidy` to populate `go.sum`. Commit both.

- [ ] **Step 2: main.go stub**

Save `workload/cmd/agentdb-workload/main.go`:
```go
package main

import (
	"flag"
	"log"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to workload config YAML")
	flag.Parse()

	log.Printf("agentdb-workload stub; config=%s (real main in later task)", configPath)
}
```

- [ ] **Step 3: config.yaml**

Save `workload/config.yaml`:
```yaml
mongo_uri: mongodb://127.0.0.1:7879/?directConnection=true
rest_base: http://127.0.0.1:7878
control_addr: 127.0.0.1:9090

personas:
  - tenant_id: t-demo-a
    ticket_rate_per_min: 6
    concurrent_sessions_cap: 3
    message_interval_ms: [500, 1500]
    session_len: [3, 8]
    escalation_prob: 0.3
    memory_recall_prob: 0.2
  - tenant_id: t-demo-b
    ticket_rate_per_min: 3
    concurrent_sessions_cap: 2
    message_interval_ms: [1000, 3000]
    session_len: [2, 5]
    escalation_prob: 0.15
    memory_recall_prob: 0.5
```

- [ ] **Step 4: README.md**

Save `workload/README.md`:
```markdown
# AgentDB workload generator

Simulates a customer-support chatbot platform driving traffic against a
running `agentdb-server`.

## Quick start

```bash
# 1. Start the DB in another terminal:
cargo run -p agentdb-server

# 2. Run the workload:
cd workload
go run ./cmd/agentdb-workload -config config.yaml
```

The dashboard at http://127.0.0.1:7878/dashboard visualizes traffic
live.

## Demo control endpoints

The workload listens on `config.control_addr` (default `:9090`):

- `POST /demo/burst?tenant=T` — trigger a load spike on one tenant
- `POST /demo/fork?session=S` — force a fork on a running session
- `POST /demo/cold?tenant=T` — idle a tenant into the cold tier
```

- [ ] **Step 5: Stubs**

Save `workload/internal/persona/persona.go`:
```go
package persona

// Filled in by Task 8.
```

Save `workload/internal/client/rest.go`:
```go
package client

// Filled in by Task 9.
```

- [ ] **Step 6: .gitignore**

Append to `.gitignore`:
```
workload/agentdb-workload
```

- [ ] **Step 7: Verify Go build**

Run: `cd workload && go build ./...`
Expected: clean (may download modules; that's fine).

Run: `cd .. && cargo test --workspace` — unaffected by Go code, all pass.

- [ ] **Step 8: Format and commit**

```bash
cargo fmt --all --check
git add workload .gitignore
git commit -m "chore(workload): scaffold Go module with config + stubs"
```

---

## Task 8: Go persona state machine + main loop

**Files:**
- Modify: `workload/internal/persona/persona.go`
- Create: `workload/internal/persona/state_machine.go`
- Modify: `workload/cmd/agentdb-workload/main.go`
- Create: `workload/internal/client/rest.go` (real)
- Create: `workload/internal/client/mongo.go`

**Context:** Each tenant runs one goroutine that spawns sessions based on `ticket_rate_per_min`. Each session is a state machine that emits events via the REST `append_and_fetch` primitive (because the Mongo `insert` doesn't have a built-in `fetch_last_n`). Optional: fire occasional `$search` queries via the Mongo driver for realism.

- [ ] **Step 1: persona.go**

Save:
```go
package persona

type Persona struct {
	TenantID              string `yaml:"tenant_id"`
	TicketRatePerMin      float64 `yaml:"ticket_rate_per_min"`
	ConcurrentSessionsCap int     `yaml:"concurrent_sessions_cap"`
	MessageIntervalMs     [2]int  `yaml:"message_interval_ms"`
	SessionLen            [2]int  `yaml:"session_len"`
	EscalationProb        float64 `yaml:"escalation_prob"`
	MemoryRecallProb      float64 `yaml:"memory_recall_prob"`
}

type Config struct {
	MongoURI    string    `yaml:"mongo_uri"`
	RESTBase    string    `yaml:"rest_base"`
	ControlAddr string    `yaml:"control_addr"`
	Personas    []Persona `yaml:"personas"`
}
```

- [ ] **Step 2: state_machine.go**

Save:
```go
package persona

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

type SessionRun struct {
	SessionID string
	Persona   *Persona
	Appender  Appender
	Searcher  Searcher
}

type Appender interface {
	Append(ctx context.Context, tenantID, sessionID string, event map[string]any, fetchN int) error
}

type Searcher interface {
	Search(ctx context.Context, tenantID, query string) error
}

var ticketCounter atomic.Uint64

func NewSessionID(p *Persona) string {
	n := ticketCounter.Add(1)
	return fmt.Sprintf("ticket-%s-%d", p.TenantID, n)
}

// Run drives a single conversation to completion.
func (s *SessionRun) Run(ctx context.Context, rng *rand.Rand) {
	length := randRange(rng, s.Persona.SessionLen[0], s.Persona.SessionLen[1])
	for i := 0; i < length; i++ {
		if ctx.Err() != nil {
			return
		}
		event := map[string]any{
			"kind": "message",
			"text": fmt.Sprintf("message %d of %d on %s", i+1, length, s.SessionID),
			"i":    i,
		}
		_ = s.Appender.Append(ctx, s.Persona.TenantID, s.SessionID, event, 5)

		if s.Searcher != nil && rng.Float64() < s.Persona.MemoryRecallProb {
			_ = s.Searcher.Search(ctx, s.Persona.TenantID, "message")
		}

		sleep := time.Duration(randRange(rng, s.Persona.MessageIntervalMs[0], s.Persona.MessageIntervalMs[1])) * time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func randRange(rng *rand.Rand, lo, hi int) int {
	if hi <= lo {
		return lo
	}
	return lo + rng.Intn(hi-lo)
}
```

- [ ] **Step 3: rest.go**

Save:
```go
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type REST struct {
	Base string
	HTTP *http.Client
}

func (r *REST) Append(ctx context.Context, tenantID, sessionID string, event map[string]any, fetchN int) error {
	body := map[string]any{"event": event, "fetch_last_n": fetchN}
	buf, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/append_and_fetch", r.Base, tenantID, sessionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("append: %s", resp.Status)
	}
	return nil
}

func (r *REST) Fork(ctx context.Context, tenantID, parent, child string) error {
	body := map[string]any{"tenant_id": tenantID, "new_session_id": child}
	buf, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/api/sessions/%s/fork", r.Base, parent)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("fork: %s", resp.Status)
	}
	return nil
}
```

- [ ] **Step 4: mongo.go**

Save:
```go
package client

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	Client *mongo.Client
}

func NewMongo(ctx context.Context, uri string) (*Mongo, error) {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &Mongo{Client: cli}, nil
}

func (m *Mongo) Search(ctx context.Context, tenantID, query string) error {
	coll := m.Client.Database(tenantID).Collection("events")
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$search", Value: bson.D{
			{Key: "text", Value: bson.D{
				{Key: "query", Value: query},
				{Key: "path", Value: "payload.text"},
			}},
		}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
	}
	return nil
}
```

- [ ] **Step 5: Replace main.go**

Save:
```go
package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/shenli/agentdb/workload/internal/client"
	"github.com/shenli/agentdb/workload/internal/persona"

	"gopkg.in/yaml.v3"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to workload config YAML")
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("loaded %d personas", len(cfg.Personas))

	ctx, cancel := context.WithCancel(context.Background())
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		log.Printf("shutting down")
		cancel()
	}()

	rest := &client.REST{Base: cfg.RESTBase, HTTP: &http.Client{Timeout: 5 * time.Second}}
	mongoCli, err := client.NewMongo(ctx, cfg.MongoURI)
	if err != nil {
		log.Printf("mongo connect: %v (continuing without search)", err)
	}

	var wg sync.WaitGroup
	for i := range cfg.Personas {
		p := &cfg.Personas[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			runTenant(ctx, p, rest, mongoCli)
		}()
	}
	wg.Wait()
}

func runTenant(ctx context.Context, p *persona.Persona, rest *client.REST, mcli *client.Mongo) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(time.Duration(60.0/p.TicketRatePerMin*1000) * time.Millisecond)
	defer ticker.Stop()
	sem := make(chan struct{}, p.ConcurrentSessionsCap)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case sem <- struct{}{}:
			default:
				continue
			}
			go func() {
				defer func() { <-sem }()
				sr := persona.SessionRun{
					SessionID: persona.NewSessionID(p),
					Persona:   p,
					Appender:  rest,
					Searcher:  asSearcher(mcli),
				}
				sr.Run(ctx, rand.New(rand.NewSource(time.Now().UnixNano())))
			}()
		}
	}
}

func asSearcher(m *client.Mongo) persona.Searcher {
	if m == nil {
		return nil
	}
	return m
}

func loadConfig(path string) (*persona.Config, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c persona.Config
	if err := yaml.Unmarshal(buf, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
```

- [ ] **Step 6: Build**

Run: `cd workload && go build ./...`
Expected: clean.

Run: `cd .. && cargo test --workspace` — unaffected, all pass.

- [ ] **Step 7: Format and commit**

```bash
cd workload && go fmt ./... && cd ..
git add workload
git commit -m "feat(workload): persona state machine + REST append + Mongo search driving tenants"
```

---

## Task 9: Workload demo control server

**Files:**
- Create: `workload/internal/control/server.go`
- Modify: `workload/cmd/agentdb-workload/main.go`

**Context:** A tiny HTTP server on `:9090` that the dashboard buttons hit to trigger stage directions (burst a tenant, force fork, etc.).

- [ ] **Step 1: control/server.go**

Save:
```go
package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/shenli/agentdb/workload/internal/client"
)

type Server struct {
	Addr string
	REST *client.REST
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/demo/burst", s.handleBurst)
	mux.HandleFunc("/demo/fork", s.handleFork)
	mux.HandleFunc("/demo/cold", s.handleCold)
	mux.HandleFunc("/demo/hybrid", s.handleHybrid)
	srv := &http.Server{Addr: s.Addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Printf("control server listening on %s", s.Addr)
	return srv.ListenAndServe()
}

func (s *Server) handleBurst(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	if tenant == "" {
		http.Error(w, "missing ?tenant", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		_ = s.REST.Append(ctx, tenant, "burst-"+tenant, map[string]any{
			"kind": "burst",
			"i":    i,
		}, 0)
	}
	writeOK(w)
}

func (s *Server) handleFork(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	parent := r.URL.Query().Get("session")
	if tenant == "" || parent == "" {
		http.Error(w, "missing ?tenant or ?session", 400)
		return
	}
	child := fmt.Sprintf("%s-fork-%d", parent, time.Now().Unix())
	if err := s.REST.Fork(r.Context(), tenant, parent, child); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeOK(w)
}

func (s *Server) handleCold(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	session := r.URL.Query().Get("session")
	if tenant == "" || session == "" {
		http.Error(w, "missing ?tenant or ?session", 400)
		return
	}
	body, _ := json.Marshal(map[string]string{
		"tenant_id":  tenant,
		"session_id": session,
		"target":     "Cold",
	})
	url := fmt.Sprintf("%s/api/admin/tier/force", s.REST.Base)
	req, err := http.NewRequestWithContext(r.Context(), "POST", url, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.REST.HTTP.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = resp.Body.Close()
	writeOK(w)
}

func (s *Server) handleHybrid(w http.ResponseWriter, r *http.Request) {
	writeOK(w)
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}
```

- [ ] **Step 2: Start it from main.go**

In `workload/cmd/agentdb-workload/main.go`, after connecting mongo:
```go
ctrl := &control.Server{Addr: cfg.ControlAddr, REST: rest}
go func() {
	if err := ctrl.Start(ctx); err != nil && err != http.ErrServerClosed {
		log.Printf("control server: %v", err)
	}
}()
```

Add the import.

- [ ] **Step 3: Build**

Run: `cd workload && go build ./...`
Expected: clean (fix compile issues from the plan's bad snippets).

- [ ] **Step 4: Commit**

```bash
git add workload
git commit -m "feat(workload): demo control server on :9090 with burst/fork/cold endpoints"
```

---

## Task 10: Smoke script + final integration

**Files:**
- Create: `scripts/smoke.sh`

- [ ] **Step 1: scripts/smoke.sh**

Save:
```bash
#!/usr/bin/env bash
set -euo pipefail

# Minimal end-to-end smoke: starts the DB, fires off a handful of operations
# via HTTP, checks the dashboard is served, and tears down.

PORT_HTTP=${PORT_HTTP:-7878}
PORT_MONGO=${PORT_MONGO:-7879}
DATA_DIR=$(mktemp -d -t agentdb-smoke-XXXXXX)
OBJ_DIR=$(mktemp -d -t agentdb-smoke-obj-XXXXXX)

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR" "$OBJ_DIR"
}
trap cleanup EXIT

AGENTDB_DATA="$DATA_DIR" \
AGENTDB_OBJECTS="$OBJ_DIR" \
AGENTDB_ADDR="127.0.0.1:$PORT_HTTP" \
AGENTDB_MONGO_ADDR="127.0.0.1:$PORT_MONGO" \
cargo run -p agentdb-server &
SERVER_PID=$!

# Wait for the HTTP port
for i in $(seq 1 30); do
    if curl -s -o /dev/null "http://127.0.0.1:$PORT_HTTP/api/admin/boxes"; then
        break
    fi
    sleep 1
done

# Poke the endpoints
curl -s -X POST "http://127.0.0.1:$PORT_HTTP/api/tenants/t-smoke/sessions/s/append_and_fetch" \
    -H 'Content-Type: application/json' \
    -d '{"event":{"text":"hi"},"fetch_last_n":0}' > /dev/null

# Dashboard root
status=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT_HTTP/dashboard/")
if [[ "$status" != "200" ]]; then
    echo "dashboard returned $status"
    exit 1
fi

echo "smoke ok"
```

Make executable: `chmod +x scripts/smoke.sh`.

- [ ] **Step 2: Run it**

Run: `bash scripts/smoke.sh`
Expected: `smoke ok` prints.

- [ ] **Step 3: Commit**

```bash
git add scripts
git commit -m "chore(scripts): smoke.sh end-to-end health check"
```

---

## Task 11: Milestone exit gate

- [ ] **Step 1: Full workspace test**

Run: `cargo test --workspace`
Expected: all tests pass.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Fmt**

Run: `cargo fmt --all --check`
Expected: clean.

- [ ] **Step 4: Go build**

Run: `cd workload && go build ./... && cd ..`
Expected: clean.

- [ ] **Step 5: Smoke**

Run: `bash scripts/smoke.sh`
Expected: `smoke ok`.

## Milestone exit criteria

All five gates return exit 0:

```bash
cargo test --workspace && \
cargo clippy --workspace --all-targets -- -D warnings && \
cargo fmt --all --check && \
(cd workload && go build ./...) && \
bash scripts/smoke.sh
```

## Demo runbook

```bash
# Terminal 1
cargo run -p agentdb-server

# Terminal 2
cd workload && go run ./cmd/agentdb-workload -config config.yaml

# Browser
open http://127.0.0.1:7878/dashboard/
```

Watch the box grid fill with warm dots as the workload generates sessions, use the dashboard's **Inject fault** button to paint a box red, **Cool demo session** to flip one warm dot to cold, **Fork demo-parent** to see a ForkCreated event in the feed.

## Known limitations

- **Metrics are best-effort.** Gauges rely on tier-transition bumps; if a path is missed (it's possible in this prototype), gauges drift.
- **SSE channel is lossy.** `async_broadcast` with overflow=true drops old events when subscribers are slow.
- **No historical time-series.** Dashboard shows current state only.
- **Go module depends on mongo-driver v1.17** (not the v2 driver); the server's wire subset is compatible with both.
- **No auth.** All admin/dashboard/control endpoints open.
- **Dashboard doesn't poll the `workload` control server by default.** The demo buttons for fault/fork hit the DB directly; a future iteration could surface workload-driven demo shortcuts in the UI too.
- **Smoke script boots a full `cargo run` every time.** For CI you'd want a pre-built binary to avoid compile time.
