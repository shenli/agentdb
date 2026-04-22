# M15 Observability Foundation + Agent Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Write tests first for each non-trivial step (superpowers:test-driven-development).

**Goal:** Three orthogonal-but-related observability+schema upgrades:

- **Part A — OpenTelemetry tracing.** Every request carries a trace ID through wire/api → core → storage. Fanout spans are children. Exporter configurable via OTLP endpoint with stdout fallback for dev.
- **Part B — Hedged reads.** Cold/archive segment reads fire a secondary request after per-tier δ (50 ms cold, 200 ms archive); winner wins, loser is cancelled. Bounds p99.9 on object-storage tails.
- **Part C — Agent schema fields.** `actor_id` on Event, `agent_id` on Session, indexed for cross-session queries. Makes the §2.5 "all events for agent X" query a schema feature rather than a payload convention. Agent is NOT promoted to a first-class API resource (design §2.3).

**Architecture:** The three parts share one branch because they're all "foundation — low-cost now, expensive to retrofit." They interact lightly (tracing spans cover hedged reads; agent-filtered queries produce traces), so landing them together lets the tests exercise the combination.

**Source design:** `docs/agent-db-v2.md` §2.3 (agent metadata), §6.3 (hedged reads), §8.1 (tracing).
**Supporting note:** `docs/superpowers/notes/2026-04-21-architecture-review-decision-matrix.md` (A2/A3/A4 rows).
**Plan ref:** `docs/superpowers/plans/2026-04-21-v2-design-implementation.md` §4 M15.

**Stacking:** This branch is stacked on `feature/m14-hlc-wiring` so HLC fields are present. Cleanup strategy depends on which PR lands first; prefer rebasing M15 onto main after M14 merges.

---

## Part A — OpenTelemetry tracing

Goal: a Jaeger (or OTLP collector) trace for every REST / Mongo request, with child spans for fanout, HLC updates, hedged-read primaries and secondaries, and storage operations.

### Task A1: Dependencies

**Files:**
- Modify: workspace `Cargo.toml` — add `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`, `tracing-opentelemetry`
- Modify: `bin/agentdb-server/Cargo.toml` — use those deps for server init
- Modify: `crates/agentdb-core/Cargo.toml` / `agentdb-api/Cargo.toml` / `agentdb-wire/Cargo.toml` — use only `tracing` (already present) and `tracing-opentelemetry` if they need to emit spans

- [ ] **Step 1: Pick version-pinned OTel crates compatible with our current `tracing` version**

Currently: `tracing = "..."` in workspace. Choose OTel versions that play nicely: e.g., `opentelemetry = "0.24"`, `opentelemetry_sdk = "0.24"`, `opentelemetry-otlp = "0.17"`, `tracing-opentelemetry = "0.25"`.

- [ ] **Step 2: Spike — confirm all three exporters work**

Build a tiny standalone example that sets up OTLP exporter and emits one span. Verify it reaches a local Jaeger (docker-compose up jaeger). Abort/downgrade if incompatibilities surface.

### Task A2: Server-side tracer initialization

**Files:**
- New: `bin/agentdb-server/src/tracing_init.rs`
- Modify: `bin/agentdb-server/src/main.rs` — call `tracing_init::init()` at startup

- [ ] **Step 1: Define config**

Env vars:
- `AGENTDB_OTLP_ENDPOINT` — if set, emit OTLP; empty → fall back to stdout.
- `AGENTDB_TRACE_SERVICE_NAME` — default `"agentdb"`.
- `AGENTDB_TRACE_LEVEL` — default `"info"`.

- [ ] **Step 2: Implement `init()`**

- With OTLP endpoint: build `opentelemetry_otlp::new_pipeline()`, combine with `tracing_subscriber::Registry` via `tracing_opentelemetry::OpenTelemetryLayer`, add `EnvFilter` for level control.
- Without endpoint: use `tracing_subscriber::fmt::Layer` with the env filter — a plain stdout logger suitable for dev.
- Return a `TracerGuard` RAII type the caller keeps until shutdown so exporter threads flush on drop.

- [ ] **Step 3: Verify in dev mode**

Run the server with no `AGENTDB_OTLP_ENDPOINT`; send an append request; see the log line `span: append_and_fetch { ... }` on stdout.

### Task A3: Instrument wire dispatch

**Files:**
- Modify: `crates/agentdb-wire/src/command.rs`

- [ ] **Step 1: Wrap `dispatch` with `#[tracing::instrument]`**

Span name: command name extracted from body (`find`, `insert`, etc.). Attributes: `db` (from `$db`), `connection_id`, `request_size_bytes`.

- [ ] **Step 2: Child span for the HLC `update()` call**

Attributes: `hlc.inbound_physical_ns`, `hlc.local_physical_ns`, `hlc.accepted` (bool).

- [ ] **Step 3: Regression test**

Inject a dev-mode tracer capture (in-memory exporter), send one Mongo `ping`, assert the span tree has the shape `ping → update_hlc`.

### Task A4: Instrument REST handlers

**Files:**
- Modify: `crates/agentdb-api/src/handlers.rs`, `src/admin.rs`, `src/admin_boxes.rs`, `src/admin_sessions.rs`, `src/fork.rs`, `src/stream.rs`
- Modify: `crates/agentdb-api/src/hlc_header.rs` — middleware span

- [ ] **Step 1: `#[tracing::instrument]` on every handler**

Attributes: route path, method, tenant_id (from URL, sanitized).

- [ ] **Step 2: Middleware extracts W3C `traceparent` header**

If present, use it as parent span context (so tracing continues across a real distributed trace started at a client). If absent, create a new root span.

- [ ] **Step 3: Outbound `traceparent` header**

Reflect the current span on the response so the client can correlate.

### Task A5: Instrument core primitives

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs`, `src/planner.rs`, `src/fork.rs`

- [ ] **Step 1: Spans on `append_and_fetch`, `fetch_last_n_merged`, `HybridPlanner::plan`, `fork_session`**

Attributes: session_id, tenant_id (sanitized), seq, hlc_ts, fanout_size (where applicable).

- [ ] **Step 2: Spans on HLC `now()` and segment read**

Tiny spans for the hot-path primitives — helpful in p99.9 diagnostics.

### Task A6: Integration test

**Files:**
- New: `crates/agentdb-api/tests/tracing.rs`

- [ ] **Step 1: Install an in-memory OTel exporter for tests**

`opentelemetry_sdk::testing::trace::InMemorySpanExporter` or similar.

- [ ] **Step 2: Send an append_and_fetch, verify the span tree**

Assert: root `append_and_fetch` has child spans for `hlc.now`, the session-lock block, and `fetch_last_n_merged`.

---

## Part B — Hedged reads

Goal: bound p99.9 tail on cold/archive segment reads by firing a secondary request after δ; winner first, loser cancelled. Design §6.3.

### Task B1: Generic hedge primitive

**Files:**
- New: `crates/agentdb-core/src/hedge.rs`

- [ ] **Step 1: `hedged<F, T>(primary: F, delay: Duration, retry: impl FnOnce() -> F) -> (T, Winner)`**

Where `F: Future<Output = Result<T>>`. Spawn primary; after `delay`, spawn secondary via `retry()`; await either — return the first result and cancel the other via `drop`.

Return a `Winner` enum `{ Primary, Secondary }` for metrics.

- [ ] **Step 2: Unit tests**

- Primary wins when fast
- Secondary wins when primary is slow
- Both fail → aggregate error
- Cancellation confirmed: the loser task never produces side effects (use a counter)

### Task B2: Apply to segment reads

**Files:**
- Modify: `crates/agentdb-core/src/primitives.rs` — `fetch_last_n_merged` cold branch
- Modify: `crates/agentdb-core/src/segment.rs` — possibly expose a `read_segment_hedged` helper

- [ ] **Step 1: Config**

Env vars:
- `AGENTDB_HEDGE_COLD_MS` — default 50
- `AGENTDB_HEDGE_ARCHIVE_MS` — default 200
- `AGENTDB_HEDGE_ENABLED` — default `true`, emergency kill-switch

- [ ] **Step 2: Apply in cold-tier `read_segment` call site inside `fetch_last_n_merged`**

Wrap the existing single read with the hedge primitive using the cold delay. Parent already has a tracing span; add hedged-specific attributes `hedge.triggered`, `hedge.winner`.

### Task B3: Metrics

**Files:**
- Modify: `crates/agentdb-core/src/metrics.rs`

- [ ] **Step 1: Add counters**

- `hedged_triggers` — secondary was fired
- `hedged_primary_wins` — primary completed first
- `hedged_secondary_wins` — secondary completed first

- [ ] **Step 2: Expose in `BoxMetrics::snapshot()` → `MetricsSnapshot`**

### Task B4: Integration test with slow-object-store simulator

**Files:**
- New: `crates/agentdb-core/tests/hedged_read.rs`

- [ ] **Step 1: Build a `DelayingObjectStore` wrapper**

Wraps an inner `ObjectStore`; adds configurable delay to the first N reads of a path, then lets subsequent reads pass through.

- [ ] **Step 2: Scenario**

- Warm up: write a cold segment.
- Wrap store with `DelayingObjectStore { first_read_delay: 5s }`.
- Open ReadCtx; call `fetch_last_n_merged` on the cold-tier session.
- Assert: completion time < 5s (hedged secondary beat the slow primary), `hedged_secondary_wins > 0`.

---

## Part C — Agent schema fields

Goal: `actor_id` on Event and `agent_id` on Session become schema fields; a cross-session agent filter becomes a one-line query. No agent resource, no lifecycle, no quota — just a tagged dimension.

### Task C1: Event.actor_id

**Files:**
- Modify: `crates/agentdb-core/src/types.rs`

- [ ] **Step 1: Add field**

```rust
pub struct Event {
    pub seq: u64,
    pub ts: u64,
    pub hlc_ts: HlcTimestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    pub payload: Document,
}
```

Optional/absent-by-default: this is a metadata dimension, not a required tag.

- [ ] **Step 2: Plumb through `append_event`**

Add `actor_id: Option<String>` parameter (or take via the existing payload struct).

- [ ] **Step 3: Decide on segment-schema bump**

With `#[serde(default, skip_serializing_if = "Option::is_none")]`, v2 segments without `actor_id` deserialize cleanly to `None`; v3 (this bump) segments with `actor_id` deserialize it. Both are readable from the same reader. **Conclusion: no bump required for Option fields with serde-default**, mirroring the rule we adopted in M14 Phase B (we bumped there because `hlc_ts` was required). Document this choice explicitly in ARCHITECTURE.md.

### Task C2: Session.primary_agent_id

**Files:**
- Modify: `crates/agentdb-core/src/types.rs`

- [ ] **Step 1: Add field to `Session`**

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub primary_agent_id: Option<String>,
```

- [ ] **Step 2: Accept via session-creation paths (registry)**

### Task C3: API surface

**Files:**
- Modify: `crates/agentdb-api/src/dto.rs`, `src/handlers.rs`
- Modify: `crates/agentdb-wire/src/handlers/insert.rs`

- [ ] **Step 1: REST `AppendAndFetchRequest` gains optional `actor_id`**

- [ ] **Step 2: Mongo `insert` respects `actor_id` field in the inserted document**

Extract from the doc, set on the Event before persist.

- [ ] **Step 3: OpenAPI snapshot regeneration**

### Task C4: Index on agent_id for cross-session queries

**Files:**
- Modify: `crates/agentdb-core/src/indexes.rs`

- [ ] **Step 1: Add a `BTreeMap<agent_id, Vec<SessionId>>` index**

Built at lazy-index-init time (existing pattern). Each tenant's sessions are partitioned by `primary_agent_id`.

- [ ] **Step 2: HybridPlanner accepts `Source::Agent(agent_id)` as a filter**

The filter intersects with Text / Vector sources (pre-filter order preserved — design §4.4).

- [ ] **Step 3: Integration test**

Create 3 sessions under agent_a, 2 under agent_b. Query with `agent_id = "agent_a"` — get exactly the three sessions' events back.

---

## Part D — Documentation touch

### Task D1: Update `ARCHITECTURE.md`

- [ ] New section "Tracing and observability (M15)" — point at `AGENTDB_OTLP_ENDPOINT` / stdout fallback, the span hierarchy.
- [ ] New section "Hedged reads (M15)" — per-tier δ, env vars, metrics.
- [ ] New section "Agent schema fields (M15)" — `actor_id` / `primary_agent_id`, indexed, intentionally *not* a resource.

### Task D2: Update the CHANGELOG / release notes placeholder

- [ ] Note: Event's on-disk format changed by adding optional `actor_id`; segments written pre-M15 remain readable (`Option<T>` with serde default). Document this as the repo's policy for "additive optional fields" vs "bump schema version."

---

## Acceptance criteria

- [ ] Traces visible in Jaeger from wire → core → storage when `AGENTDB_OTLP_ENDPOINT` is set
- [ ] Dev-mode (no OTLP) emits human-readable span lines to stdout
- [ ] Hedged-read secondary-wins count non-zero under slow-S3 simulation
- [ ] Agent-filtered query returns expected sessions across tenants
- [ ] `cargo test --workspace` passes with no new failures
- [ ] `cargo clippy --workspace --all-targets` clean
- [ ] OpenAPI snapshot reflects new `actor_id` / `primary_agent_id` fields

---

## Risks and mitigations

- **OTel version compatibility with `tracing`.** The OTel / tracing-opentelemetry ecosystem is known for version-lock issues. Mitigate: A1 Step 2 is an explicit spike before committing.
- **Hedged-read cancellation correctness.** Rust `Future` cancel is cooperative — if the loser's I/O library doesn't honor drop, the task leaks. Mitigate: test B1 Step 4 explicitly measures side effects on the cancelled branch.
- **Segment-schema drift across M14 and M15.** M14 bumped v1→v2 (hlc_ts was required). M15 does NOT bump further because `actor_id` is `Option<String>` with serde default. Rule documented in D2.

---

## Out of scope for M15

- Agent as first-class resource (endpoints, registration, quota) — v2+, design §2.3
- Agent-level billing or per-agent rate limiting — v2+
- Cross-tablet fanout of agent-filtered queries — that's M23
- Micro-sharded segments for further tail reduction — M25 if hedging is insufficient
- Trace signing / OpenTelemetry Protocol security — v2+
