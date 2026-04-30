# AgentDB V2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement individual milestones. Each milestone in this roadmap will get its own detailed task-level plan when scheduled for execution. Steps in those per-milestone plans use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the V1 prototype runtime with the V2 runtime database described in
`docs/v2_design/agent-runtime-database.md`, on top of V1's existing storage and
lifecycle primitives. V2 is a clean break — no compatibility shims.

**Architecture:** Two-layer split per V2 §4. The **Runtime Core** owns
agent-native semantics (tenant, agent, session, event, artifact, searchable
record, skill, policy, subscription). The **Storage Engine** keeps V1's existing
capabilities (RocksDB + SlateDB KV with tenant-first 31-byte keys, object store
for cold segments, segment-based lifecycle tiering) and adds an **artifact
content backend** abstraction: artifact metadata lives in the runtime
metadata/event store next to events/agents/sessions; artifact content lives in
a separate content backend (local filesystem for the single-machine prototype,
  S3-compatible object store for distributed deployments). The runtime API
  exposes only put/get/head — the storage engine decides whether small content
  is inlined or spilled to the content backend. Wire surface is a clean-break
  native `/api/...` HTTP API with typed prefixed IDs. The Mongo wire facade
(`agentdb-wire`) and the Go SDK (`clients/go`) are deleted.

**Tech Stack:** Rust (existing crates), Axum (HTTP), Tokio (async), RocksDB +
SlateDB (KV), object_store crate (S3 / local FS as one impl of the artifact
content backend and as cold-segment storage), sqlx + Postgres (control-plane
meta), serde_json (event payloads), Server-Sent Events (subscriptions).
Python + TypeScript SDKs are rewritten; LangGraph is the P1 proof adapter.

---

## Plan Structure

This is a **milestone roadmap**, not a single task-level plan. V2 is too broad
for one bite-sized-step plan to cover end-to-end without becoming unreadable.

- **§1 Scoping decisions** — locks design choices that ripple across milestones.
- **§2 Milestone roadmap** — 9 milestones in build order, grouped to match
  AgentDB's "big PRs, not tiny ones" convention. Each milestone has scope,
  files, exit criteria, and dependencies.
- **§3 V2.0 Scaffolding — task-level plan.** This is the only milestone with
  full TDD task steps in this document, because it is the immediate next
  action. Each subsequent milestone gets its own task-level plan
  (`docs/v2_design/plan-v2-<n>-<name>.md`) authored at the start of its
  execution session.
- **§4 Spec-coverage cross-reference** — every V2 spec section maps to a
  milestone.
- **§5 Self-review** — checklist applied to this plan.

---

## §1 Scoping Decisions

These are decided up front so they don't get re-litigated in every milestone.

| Decision | Choice |
|---|---|
| Cutover model | Clean break. The native runtime API uses `/api/...`; no `/v2` path and no compatibility shim are required. Delete or ignore the old V1 surface instead of running parallel routes. |
| `agentdb-wire` Mongo facade | Deleted at V2.7. |
| `clients/go` | Deleted at V2.7. |
| Storage backends | Unchanged: RocksDB + SlateDB for KV, object_store for blobs/cold, Postgres for control-plane meta. |
| ID format | UUIDv7 with type prefixes (`ten_`, `agt_`, `ses_`, `evt_`, `art_`, `rec_`, `skl_`, `pol_`). Internal `u64` IDs persist inside the existing 31-byte key encoding for backward storage compatibility. |
| Policy persistence | Long-lived policy + attachment records in `agentdb-meta` (Postgres). Hot-path policy decisions cached locally in the runtime core; refreshed asynchronously (V2 §12.2). |
| SDK priority | Python rewrite at V2.6 (P0). TypeScript rewrite at V2.6 (P0). Go: deleted. |
| Framework adapter | LangGraph at V2.8. Validation rule: adapter must be mostly mapping code. |
| Dashboard | Built at V2.8 alongside the adapter, so it can demonstrate real adapter traffic. |
| Schema versioning | Every event payload, every searchable record, every skill manifest, every policy carries `schema_version: "YYYY-MM-DD"`. V2 baseline: `2026-04-28`. |
| Idempotency | P0 on event append + append-and-fetch (V2.0). Extended to artifact/record/skill/policy mutations at their respective milestones. |
| Deletion model | Soft-delete + GC. Every runtime object record carries `deleted_at_ms: Option<u64>`. Synchronous deletes write a marker, never destroy. A separate GC worker (V2.4) removes content backend bytes, index entries, and the metadata record itself once the retention window from policy elapses. Artifacts additionally carry `raw_purged_at_ms` for derivative-only mode (raw bytes purged while metadata + derivatives + provenance remain). GC is tenant-scoped, idempotent, resumable, respects legal hold, and runs in a worker pool isolated from hot-path append. Reads filter `deleted_at_ms IS NULL` by default; admin/audit may pass `include_deleted=true`. Delete and purge events flow through the changefeed so subscribers see them. |

---

## §2 Milestone Roadmap

Build order matches impl guide §15, grouped per PR-size preference. Each
milestone is one PR. LoC estimates assume RocksDB+SlateDB stay; counts include
tests.

### V2.0 — V2 scaffolding + tenant/agent/session/event core (~3000 LoC)

**Maps to impl guide §15 steps 1–6.**

Scope:

- New `agentdb-core::v2` module tree: `id`, `tenant`, `agent`, `session`,
  `event`, `runtime` submodules
- Typed-prefix ID generator (UUIDv7 + 3-char type prefix)
- V2 event envelope: `event_id`, `seq`, `schema_version`, `artifact_refs`,
  `source_refs`, `idempotency_key`, `policy_version`
- Tenant + Agent + Session as first-class runtime objects (impl guide §6.2)
- Every runtime object record carries `deleted_at_ms: Option<u64>` from day
  one (soft-delete model). Reads filter `deleted_at_ms IS NULL` by default.
  GC worker comes later in V2.4 — V2.0 just establishes the field and the
  read-time filter so later milestones don't retrofit
- Atomic per-session append; `next_seq` updated under per-session lock
- Append-and-fetch in one runtime call
- Idempotency record store + result cache (P0 scope: append + append-and-fetch)
- `/api/tenants`, `/api/tenants/{ten}/agents`, `/api/tenants/{ten}/agents/{agt}/sessions`, `/api/tenants/{ten}/sessions/{ses}/events`, `/api/tenants/{ten}/sessions/{ses}/append-and-fetch`, `/api/tenants/{ten}/sessions/{ses}/events?from_seq=`
- Structured error response shape with `code`, `message`, `retryable`, `suggestion`
- `agentdb serve` mounts the native runtime API directly; no `--v2` flag

Files:

- Create: `crates/agentdb-core/src/v2/{mod,id,tenant,agent,session,event,runtime,idempotency,errors}.rs`
- Create: `crates/agentdb-storage/src/v2_keys.rs`
- Create or replace: `crates/agentdb-api/src/{routes,dto,errors,middleware}.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (mount native runtime router)
- Modify: `bin/agentdb-server/src/serve.rs` (serve native runtime API)
- Create: `crates/agentdb-meta/migrations/0030_v2_agents_idempotency.sql`
- Test: `crates/agentdb-core/tests/v2_runtime_basics.rs`, `crates/agentdb-api/tests/v2_routes.rs`

Exit criteria:

- A new tenant can be created, an agent registered, a session opened, an event
  appended, and the same event fetched back via `/api/...`.
- `seq` is monotonic and contiguous within a session across restarts.
- Idempotency-Key on append returns identical result on retry; conflict on
  payload mismatch.
- Native `/api/...` routes pass their tests. There is no V1 compatibility route
  requirement.
- Restart preserves tenant, agent, session, and event records.

Dependencies: none (foundation milestone).

### V2.1 — Artifact subsystem (~2000 LoC)

**Maps to impl guide §15 steps 7–8.**

Scope:

- Artifact metadata record (impl guide §6.3) stored in the same metadata store as
  events/agents/sessions, plus an artifact content backend ref
- Local content backend path such as `artifact_content/{tenant_id}/{artifact_id}/blob`
- SHA-256 verify on put; filesystem content backend uses tmp-then-rename for
  atomic install
- Storage engine decides physical placement for artifact content; the API does
  not expose whether small content was inlined or spilled to the content backend
- `POST /api/.../artifacts` (buffered upload, P0), `GET /api/.../artifacts/{id}`,
  `GET /api/.../artifacts/{id}/blob`
- Event `artifact_refs` validation: every ref must point to a real artifact in
  the same tenant
- Idempotency-Key on artifact put
- Inline-vs-artifact size policy enforcement (max_inline_event_bytes,
  artifact_required_after_bytes, max_artifact_bytes)

Files:

- Create: `crates/agentdb-core/src/v2/artifact.rs`
- Create: `crates/agentdb-storage/src/v2_artifact.rs`
- Modify: `crates/agentdb-core/src/v2/event.rs` (add `artifact_refs` validator)
- Create: `crates/agentdb-api/src/v2/artifacts.rs`
- Create: `crates/agentdb-meta/migrations/0031_v2_artifacts.sql`
- Test: `crates/agentdb-core/tests/v2_artifact.rs`, `crates/agentdb-api/tests/v2_artifact_routes.rs`

Exit criteria:

- Artifact put returns `art_*` ID; round-trip verify SHA-256 matches.
- An event referencing a non-existent or wrong-tenant artifact is rejected with
  `ARTIFACT_NOT_FOUND`.
- An inline event payload exceeding `max_inline_event_bytes` is rejected with
  `EVENT_TOO_LARGE` and the suggestion to use an artifact.
- Artifact content survives restart; metadata survives restart in the runtime
  metadata store.

Dependencies: V2.0.

### V2.2 — Searchable records + indexing policy (~2500 LoC)

Status: implemented in `feature/v2.2-records-followup`.

**Maps to impl guide §15 step 9 + V2 §9.4.**

Scope:

- Searchable record type (impl guide §6.4) with `namespace`, `scope`,
  `metadata`, `tags`, `source` refs
- `POST /api/.../records`, `GET /api/.../records/search`,
  `POST /api/.../records/search` for vector/body search,
  `DELETE /api/.../records/{id}`
- Search must support: namespace, scope, tags, text query, source refs, limit
- Indexing policy enforced at write time: `no_indexing` / `metadata_only` /
  `selected_fields` / `summary_only`
- Vector indexing uses caller-supplied embedding (no built-in embedding model)
- Index invalidation hook: deletion or redaction of a source object invalidates
  derived index entries

Files:

- Create: `crates/agentdb-core/src/v2/record.rs`
- Modify: `crates/agentdb-index/src/text.rs` (add scope + policy parameter)
- Modify: `crates/agentdb-index/src/vector.rs` (impl with caller-supplied embedding)
- Create: `crates/agentdb-api/src/v2/records.rs`
- Create: `crates/agentdb-meta/migrations/0032_v2_records.sql`
- Test: `crates/agentdb-core/tests/v2_record.rs`, `crates/agentdb-api/tests/v2_record_search.rs`

Exit criteria:

- Records can be written with any of the four indexing modes; mode is enforced.
- Search across namespace + scope + text returns expected matches.
- Vector search returns nearest-neighbor by caller-supplied embedding.
- Deleting a record invalidates its index entries.

Validation:

- `cargo test -p agentdb-api --test api_records`
- `cargo test -p agentdb-core --test record --test record_runtime --test runtime_source_refs --test runtime_restart`
- `cargo test -p agentdb-core -p agentdb-api`
- `cargo test -p agentdb-index`

Dependencies: V2.0 (for tenant/agent scope).

### V2.3 — Skills (~1500 LoC)

Status: implemented in `feature/v2.3-skills`.

**Maps to impl guide §15 step 10.**

Scope:

- Skill type (impl guide §6.5) with `manifest`, `version`, `artifact_refs`
- Registry: `POST /api/.../skills`, `POST /api/.../agents/{agt}/skills/bind`,
  `GET /api/.../agents/{agt}/skills`
- `(tenant_id, skill_name, version)` uniqueness
- `skill_used` event kind that records `skill_id` + exact `version` for replay
- Skill packages may be stored as artifacts (cross-link to V2.1)
- Idempotency-Key on skill register and bind

Files:

- Create: `crates/agentdb-core/src/v2/skill.rs`
- Create: `crates/agentdb-api/src/v2/skills.rs`
- Modify: `crates/agentdb-core/src/v2/event.rs` (allow `skill_used` event kind validation)
- Create: `crates/agentdb-meta/migrations/0033_v2_skills.sql`
- Test: `crates/agentdb-core/tests/v2_skill.rs`, `crates/agentdb-api/tests/v2_skill_routes.rs`

Exit criteria:

- Skill register; bind to agent; `skill_used` event records exact version;
  replay returns the same exact version even after a newer version is
  registered.
- Duplicate `(tenant, name, version)` registration is rejected.

Validation:

- `cargo test -p agentdb-core --test skill --test runtime_restart`
- `cargo test -p agentdb-api --test api_skills`
- `cargo test -p agentdb-core -p agentdb-api`

Dependencies: V2.0; V2.1 for skill-as-artifact case.

### V2.4 — Policy model + lifecycle + GC + per-tenant priority/quota (~5000 LoC)

Status: in progress. V2.4a implements the hot-path policy slice:
tenant policy + agent override resolution, priority parsing, append/artifact/
record quota enforcement, `policy_version` stamping on events, and
`include_deleted=true` record reads. Lifecycle transitions, tier templates,
backpressure headers, full multi-dimensional quotas, and GC worker scheduling
remain for V2.4b+.

**Maps to impl guide §15 step 13 + V2 §17, §18, §12.8.**

Scope:

- Policy type with eight categories (V2 §17): quota, lifecycle, retention,
  indexing, artifact-size, approval/HITL, tool/skill access, tenant data access
- Versioned policy records; attachment to tenant or agent or session or skill
- Precedence: agent > tenant > built-in defaults (impl guide §11)
- **Per-tenant policy is the managed-service foundation.** Different tenants
  get different policies via attachment. Tier templates (e.g., `free`, `pro`,
  `enterprise`) are first-class: a tier is a named, versioned policy bundle
  that the managed control plane attaches to a tenant at provisioning. Tier
  policies can be updated centrally and propagate to all attached tenants on
  next refresh.
- **Multi-dimensional quotas** (V2 §12.3 made concrete): every quota carries
  both a `soft` and `hard` limit. Soft → throttle + emit backpressure signal;
  hard → reject with `RATE_LIMITED` or `POLICY_DENIED`. Dimensions:
  - append rate (events/sec) and event bytes/sec
  - active sessions; parallel appends per session
  - hot-session write rate
  - artifact upload rate, egress rate, max single artifact bytes, total
    artifact storage by tier
  - searchable record count, write rate, search/recall rate
  - subscription count, fanout, delivery backlog bytes
  - replay/scan/hydration budgets per minute
  - indexing operations/sec, index storage bytes
  - lifecycle work units issued per tenant per minute
- **Priority class** on the policy (`priority: low | normal | high |
  critical`). Priority informs:
  - hot-path admission: under contention, higher-priority tenants get
    preference; equal-priority tenants get fair share
  - bulk-work scheduling: GC, indexing, lifecycle, replay workers schedule
    higher-priority tenants more frequently
  - backpressure thresholds: higher tiers tolerate higher backlog before
    throttling
- **Backpressure signal** is a customer-visible operational signal (V2
  §12.11): tenants near their soft quota receive `X-AgentDB-Backpressure:
  <bucket>=<percent>` headers on writes so SDKs can slow down before being
  rejected.
- **Noisy-neighbor isolation:** per-tenant scheduler buckets ensure one
  tenant's bulk work cannot consume another tenant's hot-path budget (V2
  §12.9). Workers honor per-tenant rate caps independent of global capacity.
- Policy evaluation hooks at: append, artifact put, record write, lifecycle
  sweep, indexing decision, context build
- `policy_version` stamped on appended events for replay reconstruction
- Active state added to lifecycle; derivative-only mode plumbed through
  `Artifact::raw_purged_at_ms`
- `seal_segment`, `move_to_cold`, `archive`, `hydrate` surfaced as policy-driven
  actions on top of existing `tiering.rs`
- **GC subsystem** for the soft-delete model:
  - Background worker pool isolated from hot-path append (V2 §12.9)
  - Tenant-scoped scheduling; per-tenant retention window from policy
  - Per-object-kind GC handlers: tenant, agent, session, event, artifact,
    record, skill, policy attachment, idempotency record
  - Removal order per object: 1) drop index entries, 2) drop content
    backend bytes (artifact only), 3) drop the metadata record
  - Artifact derivative-only path: GC sets `raw_purged_at_ms` on metadata,
    purges content bytes, leaves metadata + derivatives + provenance
  - Idempotent + resumable after worker crash (V2 §12.8)
  - Honors legal hold flag (skips affected objects)
  - Range-delete API: `delete_tenant`, `delete_session` write a single
    prefix-level tombstone marker that the GC scan expands across children
  - Deletion and purge emit events into the changefeed so subscribers see
    them: `<kind>_deleted` (logical), `<kind>_purged` (physical removal)

Files:

- Create: `crates/agentdb-core/src/v2/policy.rs`
- Create: `crates/agentdb-core/src/v2/lifecycle.rs` (V2 wrapper around existing tiering)
- Create: `crates/agentdb-core/src/v2/gc.rs` (GC worker, scheduling, per-kind handlers)
- Modify: `crates/agentdb-core/src/v2/runtime.rs` (insert policy checks; soft-delete API)
- Modify: `crates/agentdb-core/src/v2/artifact.rs` (add `raw_purged_at_ms`)
- Modify: `crates/agentdb-storage/src/v2_keys.rs` (add tombstone prefix encoding for range-delete)
- Modify: `crates/agentdb-index/src/{text,vector}.rs` (drop-by-doc-id GC entry point)
- Create: `crates/agentdb-api/src/v2/policies.rs`
- Create: `crates/agentdb-meta/migrations/0034_v2_policies_gc.sql`
- Test: `crates/agentdb-core/tests/v2_policy.rs`,
  `crates/agentdb-core/tests/v2_lifecycle.rs`,
  `crates/agentdb-core/tests/v2_gc.rs`

Exit criteria:

- Tenant policy + agent override resolve correctly with recorded precedence.
- Two tenants with different policies (e.g., free-tier and pro-tier) enforce
  their own quota independently in concurrent traffic; one tenant exhausting
  its hard quota does not affect the other.
- Tier templates: a `free` template applied to multiple tenants resolves to
  the same policy_version; updating the template propagates on next refresh.
- Soft quota threshold emits `X-AgentDB-Backpressure` header; hard quota
  rejects with `RATE_LIMITED` or `POLICY_DENIED`.
- Priority class affects scheduling: under simulated contention, a `high`
  priority tenant's append latency stays under target while a `low` priority
  tenant's latency degrades first.
- Bulk-work scheduling honors priority: GC and indexing scan high-priority
  tenants more frequently than low-priority ones.
- All multi-dimensional quotas above are reachable and enforceable via test:
  append rate, artifact storage bytes, record count, subscription count,
  replay budget, indexing ops, lifecycle work — each has a unit + integration
  test.
- Append rejected with `POLICY_DENIED` + `policy_version` when over quota.
- An event records the `policy_version` evaluated at append time.
- A session transitions Active → Warm → Cold → Archive based on policy timer.
- Soft-delete: a deleted record returns 404 on default reads, returns the
  record with `deleted_at_ms` set when `include_deleted=true`.
- GC worker removes index entries, content bytes, then metadata, in that
  order, after the retention window elapses.
- Derivative-only path: GC purges raw artifact content while leaving
  metadata + derivative records intact (tested via redaction round-trip and
  provenance lookup after purge).
- GC respects legal hold (object marked under hold is skipped).
- Range delete: deleting a session writes one prefix tombstone; subsequent
  reads of any event in the session return 404; GC removes all child events
  + index entries within retention window.
- `<kind>_deleted` and `<kind>_purged` events appear in the changefeed.
- GC is resumable: killing the worker mid-pass and restarting completes
  without re-charging or skipping objects.

Dependencies: V2.0, V2.1, V2.2.

### V2.5 — Fork + subscribe + context builder (~2000 LoC)

**Maps to impl guide §15 steps 11–12, 14.**

Scope:

- `POST /api/.../sessions/{ses}/fork` with `at_seq`; child gets fresh
  `session_id` and child-local `seq` starting at 0
- Replay merging: parent events through `at_seq`, then child events
- `GET /api/.../sessions/{ses}/subscribe` — SSE replay-from-seq + live tail
- `GET /api/.../sessions/{ses}/replay` for explicit replay endpoint (impl guide
  §8.3)
- Generalized changefeed: `watch(stream_id | tenant_id | agent_id, from_cursor)`
- `POST /api/.../sessions/{ses}/context` (impl guide §12) with `max_events`,
  `max_bytes`, `include[]`, `record_query`

Files:

- Create: `crates/agentdb-core/src/v2/fork.rs` (V2 wrapper of existing fork)
- Create: `crates/agentdb-core/src/v2/subscribe.rs`
- Create: `crates/agentdb-core/src/v2/context.rs`
- Create: `crates/agentdb-api/src/v2/{fork,subscribe,replay,context}.rs`
- Test: `crates/agentdb-core/tests/v2_fork.rs`,
  `crates/agentdb-api/tests/v2_subscribe_sse.rs`,
  `crates/agentdb-core/tests/v2_context.rs`

Exit criteria:

- Forked session replay returns parent events through cutoff then child events.
- SSE subscribe from seq replays existing then live-tails new events; reconnect
  with `from_seq=N+1` does not duplicate.
- Context builder respects byte and token budgets and inclusion filters.
- Tenant-wide changefeed delivers events across all sessions in a tenant.

Dependencies: V2.0.

### V2.6 — SDK rewrites (Python + TypeScript) (~2500 LoC)

**Maps to impl guide §3.**

Scope:

- Python SDK rewrite: `clients/python/` mapped to `/api/...` surface
- TypeScript SDK rewrite: `clients/typescript/` mapped to `/api/...` surface
- Both SDKs expose: tenant, agent, session, event append, append-and-fetch,
  fetch, replay, fork, subscribe, artifact put/get, record write/search, skill
  register/bind, context build
- Idempotency-Key automatically generated client-side for retries
- Typed prefixed IDs surfaced as opaque `TenantId`, `AgentId`, etc. types

Files:

- Rewrite: `clients/python/agentdb/` (new module structure for V2)
- Rewrite: `clients/typescript/src/` (new module structure for V2)
- Test: `clients/python/tests/v2_*.py`, `clients/typescript/tests/v2_*.spec.ts`

Exit criteria:

- Both SDKs run the impl guide §16 sample (sales agent: register skill, open
  session, append-and-fetch with artifact, write record, fork, subscribe).
- E2E test against an `agentdb serve` instance passes for both SDKs.
- SDK versions bumped to `2.0.0`.

Dependencies: V2.0–V2.5 (full V2 surface).

### V2.7 — Legacy deletion and cleanup (~−5000 LoC net)

**Maps to spec §22 non-goals + change-list §11.2.**

Scope:

- Delete `crates/agentdb-wire/` entirely
- Delete `clients/go/` entirely
- Delete `crates/agentdb-api/src/{handlers,subscribe,broadcast,fork}.rs` V1 handlers
- Delete obsolete V1 handlers and compatibility code from the router
- Ensure no `--v2` flag exists; the native runtime API is the only surface
- Update `docs/agent-db.md`, `DESIGN.md`, `ARCHITECTURE.md` to V2 facts
- Migrate V1 integration tests still applicable to the new `/api` payload shapes
  and typed IDs
- Bump server major version to `2.0.0`

Files:

- Delete: `crates/agentdb-wire/`, `clients/go/`,
  `crates/agentdb-api/src/{handlers.rs,subscribe.rs,broadcast.rs,fork.rs}` and
  related v1 modules
- Modify: `Cargo.toml` workspace (remove `agentdb-wire` member)
- Modify: `crates/agentdb-api/src/lib.rs` (router cleanup)
- Modify: `bin/agentdb-server/src/serve.rs` (remove obsolete compatibility flags)
- Modify: `docs/agent-db.md`, `docs/DESIGN.md`, `docs/ARCHITECTURE.md`,
  top-level `README.md`, `AGENTS.md`
- Migrate: `crates/agentdb-api/tests/*` retain only V2-applicable cases

Exit criteria:

- `cargo build --workspace` clean with no `agentdb-wire` references.
- Only the native `/api/...` runtime routes and admin paths are mounted.
- Full test suite green.
- `cargo clippy --all-targets -- -D warnings` clean.
- README + DESIGN + ARCHITECTURE describe only V2.

Dependencies: V2.0–V2.6 (V2 must fully replace V1 first).

### V2.8 — Framework adapter (LangGraph) + session-first dashboard (~5000 LoC)

**Maps to impl guide §16 + V2 §16.1, §16.2.**

Scope:

- LangGraph adapter (Python): `langgraph-checkpoint-agentdb` package
  - checkpointer: thread checkpoints, pending writes, checkpoint history
  - store: namespaced `store` with vector search
  - validation: adapter is mostly mapping code; if it has to invent ordering /
    idempotency / fork lineage / artifact offload / search — that's a V2 gap
    bug, not an adapter concession
- Session-first dashboard (`crates/agentdb-dashboard/`):
  - timeline: events with kind/seq/ts/payload-summary/artifact-refs
  - fork tree: parent/child session graph
  - artifact viewer: metadata + blob preview
  - searchable record viewer: namespace + scope + tags + source refs
  - skill registry view
  - replay controls: scrub through session by seq
  - real session can be observed live through SSE

Files:

- Create: `adapters/langgraph-checkpoint-agentdb/` (new top-level dir)
- Build: `crates/agentdb-dashboard/src/` (Yew or Leptos frontend in Rust, or
  embedded SPA — choice deferred to dashboard plan)
- Test: `adapters/langgraph-checkpoint-agentdb/tests/`,
  dashboard E2E with Playwright

Exit criteria:

- LangGraph official integration tests pass against AgentDB-backed checkpointer
  + store.
- Adapter code volume <30% of equivalent custom Postgres implementation
  (validation rule from V2 §16.2).
- Dashboard renders timeline / fork tree / artifacts / records / skills for a
  live session and updates via SSE.
- Sample app (incident-response agent) demoed end-to-end through the dashboard.

Dependencies: V2.0–V2.7.

### V2.9 — Stretch / P1 backlog (not part of initial cutover)

These are noted but not scheduled into this plan. They're tracked as follow-up:

- OpenAI Agents SDK session adapter
- MCP server exposing AgentDB tools/resources/prompts
- Vercel AI SDK template
- Multi-node scale-out path
- Advanced runaway-agent detection
- Artifact deduplication
- Encryption + KMS hooks at storage boundaries (V2 §12.6)
- Region routing / residency enforcement (V2 §12.5)

Each will get its own plan when prioritized.

---

## §3 V2.0 Scaffolding — Task-Level Plan

This is the only milestone with full TDD task steps in this document. It is the
immediate next action after this plan is approved.

For executing milestones V2.1+, author a per-milestone plan at
`docs/v2_design/plan-v2-<n>-<slug>.md` with the same TDD task structure before
dispatching execution.

> **Pre-flight:** Before starting V2.0, check current `Cargo.toml` workspace,
> existing module names that would collide with `v2`, and the current working
> directory. The user prefers big PRs — V2.0 ships as one PR even though it
> has 13 tasks.

### Task 1: Add `agentdb-core::v2` module skeleton

**Files:**

- Create: `crates/agentdb-core/src/v2/mod.rs`
- Modify: `crates/agentdb-core/src/lib.rs` (export v2 module)

- [ ] **Step 1: Write a failing test that the v2 module exists**

```rust
// crates/agentdb-core/tests/v2_module_present.rs
#[test]
fn v2_module_is_exported() {
    let _: &str = agentdb_core::v2::VERSION;
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_module_present
```

Expected: FAIL with "no `v2` in the root" or "VERSION not found".

- [ ] **Step 3: Create the module with a VERSION constant**

```rust
// crates/agentdb-core/src/v2/mod.rs
pub const VERSION: &str = "2026-04-28";

pub mod id;
pub mod errors;
```

```rust
// crates/agentdb-core/src/lib.rs — add this line near other pub mod declarations
pub mod v2;
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_module_present
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/mod.rs crates/agentdb-core/src/lib.rs crates/agentdb-core/tests/v2_module_present.rs
git commit -m "feat(core): add v2 module skeleton (V2.0)"
```

### Task 2: Typed-prefix ID generator

**Files:**

- Create: `crates/agentdb-core/src/v2/id.rs`
- Test: `crates/agentdb-core/tests/v2_id.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_id.rs
use agentdb_core::v2::id::{Id, IdKind};

#[test]
fn tenant_id_has_ten_prefix_and_uuidv7_body() {
    let id = Id::new(IdKind::Tenant);
    let s = id.as_str();
    assert!(s.starts_with("ten_"), "got {s}");
    assert_eq!(s.len(), 4 + 36, "uuidv7 string is 36 chars; got {s}");
}

#[test]
fn ids_round_trip_through_str() {
    let id = Id::new(IdKind::Session);
    let parsed = Id::parse(id.as_str()).unwrap();
    assert_eq!(parsed.kind(), IdKind::Session);
    assert_eq!(parsed.as_str(), id.as_str());
}

#[test]
fn parse_rejects_wrong_prefix() {
    let err = Id::parse("xyz_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap_err();
    assert!(format!("{err}").contains("unknown prefix"));
}

#[test]
fn parse_rejects_wrong_kind() {
    let id = Id::new(IdKind::Tenant);
    let err = Id::parse_kind(id.as_str(), IdKind::Session).unwrap_err();
    assert!(format!("{err}").contains("expected Session"));
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_id
```

Expected: FAIL — module/types do not exist.

- [ ] **Step 3: Implement Id + IdKind**

Add `uuid` to `crates/agentdb-core/Cargo.toml` (already a dep in workspace
likely; verify and reuse). Then:

```rust
// crates/agentdb-core/src/v2/id.rs
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum IdKind {
    Tenant,
    Agent,
    Session,
    Event,
    Artifact,
    Record,
    Skill,
    Policy,
}

impl IdKind {
    pub fn prefix(&self) -> &'static str {
        match self {
            IdKind::Tenant => "ten",
            IdKind::Agent => "agt",
            IdKind::Session => "ses",
            IdKind::Event => "evt",
            IdKind::Artifact => "art",
            IdKind::Record => "rec",
            IdKind::Skill => "skl",
            IdKind::Policy => "pol",
        }
    }

    pub fn from_prefix(prefix: &str) -> Option<Self> {
        Some(match prefix {
            "ten" => IdKind::Tenant,
            "agt" => IdKind::Agent,
            "ses" => IdKind::Session,
            "evt" => IdKind::Event,
            "art" => IdKind::Artifact,
            "rec" => IdKind::Record,
            "skl" => IdKind::Skill,
            "pol" => IdKind::Policy,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Id {
    kind: IdKind,
    inner: String, // already includes the "kind_" prefix
}

#[derive(Debug, thiserror::Error)]
pub enum IdError {
    #[error("missing '_' separator in id")]
    MissingSeparator,
    #[error("unknown prefix '{0}'")]
    UnknownPrefix(String),
    #[error("invalid uuid body: {0}")]
    BadBody(String),
    #[error("expected {expected:?} but got {got:?}")]
    KindMismatch { expected: IdKind, got: IdKind },
}

impl Id {
    pub fn new(kind: IdKind) -> Self {
        let body = Uuid::now_v7();
        let inner = format!("{}_{body}", kind.prefix());
        Self { kind, inner }
    }

    pub fn parse(s: &str) -> Result<Self, IdError> {
        let (prefix, body) = s.split_once('_').ok_or(IdError::MissingSeparator)?;
        let kind = IdKind::from_prefix(prefix)
            .ok_or_else(|| IdError::UnknownPrefix(prefix.to_string()))?;
        Uuid::from_str(body).map_err(|e| IdError::BadBody(e.to_string()))?;
        Ok(Self { kind, inner: s.to_string() })
    }

    pub fn parse_kind(s: &str, expected: IdKind) -> Result<Self, IdError> {
        let id = Self::parse(s)?;
        if id.kind != expected {
            return Err(IdError::KindMismatch { expected, got: id.kind });
        }
        Ok(id)
    }

    pub fn kind(&self) -> IdKind { self.kind }
    pub fn as_str(&self) -> &str { &self.inner }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.inner) }
}
```

Add `pub use id::*;` to `crates/agentdb-core/src/v2/mod.rs` if you want
top-level re-exports — for V2.0 keep them under `v2::id` to avoid name
collisions with V1 types.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_id
```

Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/id.rs crates/agentdb-core/tests/v2_id.rs
git commit -m "feat(core): typed-prefix UUIDv7 ids for v2 (V2.0)"
```

### Task 3: V2 storage keys

**Files:**

- Create: `crates/agentdb-storage/src/v2_keys.rs`
- Modify: `crates/agentdb-storage/src/lib.rs` (export `v2_keys`)
- Test: `crates/agentdb-storage/tests/v2_keys.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-storage/tests/v2_keys.rs
use agentdb_storage::v2_keys::{
    tenant_record_key, agent_record_key, session_record_key,
    event_key, event_by_id_key, idempotency_key,
};

#[test]
fn tenant_key_format() {
    let k = tenant_record_key("ten_abc");
    assert_eq!(k, "record/tenant/ten_abc");
}

#[test]
fn agent_key_is_tenant_first() {
    let k = agent_record_key("ten_abc", "agt_xyz");
    assert_eq!(k, "record/agent/ten_abc/agt_xyz");
}

#[test]
fn session_key_is_tenant_first() {
    let k = session_record_key("ten_abc", "ses_xyz");
    assert_eq!(k, "record/session/ten_abc/ses_xyz");
}

#[test]
fn event_key_orders_by_seq() {
    let k1 = event_key("ten_a", "ses_s", 0);
    let k2 = event_key("ten_a", "ses_s", 1);
    assert!(k1 < k2);
}

#[test]
fn event_by_id_separate_lookup() {
    assert_eq!(
        event_by_id_key("ten_a", "evt_e"),
        "event_by_id/ten_a/evt_e"
    );
}

#[test]
fn idempotency_key_includes_all_scope_components() {
    let k = idempotency_key("ten_a", "append", "ses_s", "client-key-1");
    assert_eq!(k, "idempotency/ten_a/append/ses_s/client-key-1");
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-storage --test v2_keys
```

Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement key encoders**

```rust
// crates/agentdb-storage/src/v2_keys.rs
//! Tenant-first logical key encoders for V2 runtime objects.
//! See docs/v2_design/minimal-single-machine-implementation.md §7.

pub fn tenant_record_key(tenant_id: &str) -> String {
    format!("record/tenant/{tenant_id}")
}

pub fn tenant_alias_key(external_key: &str) -> String {
    format!("record/tenant_alias/{external_key}")
}

pub fn agent_record_key(tenant_id: &str, agent_id: &str) -> String {
    format!("record/agent/{tenant_id}/{agent_id}")
}

pub fn agent_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("record/agent_alias/{tenant_id}/{alias}")
}

pub fn session_record_key(tenant_id: &str, session_id: &str) -> String {
    format!("record/session/{tenant_id}/{session_id}")
}

pub fn session_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("record/session_alias/{tenant_id}/{alias}")
}

pub fn session_by_agent_key(tenant_id: &str, agent_id: &str, session_id: &str) -> String {
    format!("record/session_by_agent/{tenant_id}/{agent_id}/{session_id}")
}

pub fn event_key(tenant_id: &str, session_id: &str, seq: u64) -> String {
    // Zero-pad seq to 20 digits so lexicographic order matches numeric order.
    format!("event/{tenant_id}/{session_id}/{seq:020}")
}

pub fn event_by_id_key(tenant_id: &str, event_id: &str) -> String {
    format!("event_by_id/{tenant_id}/{event_id}")
}

pub fn idempotency_key(tenant_id: &str, operation: &str, target: &str, key: &str) -> String {
    format!("idempotency/{tenant_id}/{operation}/{target}/{key}")
}
```

```rust
// crates/agentdb-storage/src/lib.rs — add:
pub mod v2_keys;
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-storage --test v2_keys
```

Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-storage/src/v2_keys.rs crates/agentdb-storage/src/lib.rs crates/agentdb-storage/tests/v2_keys.rs
git commit -m "feat(storage): tenant-first v2 logical key encoders (V2.0)"
```

### Task 4: V2 structured error type

**Files:**

- Create: `crates/agentdb-core/src/v2/errors.rs`
- Test: `crates/agentdb-core/tests/v2_errors.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_errors.rs
use agentdb_core::v2::errors::{V2Error, ErrorCode};

#[test]
fn error_serializes_with_required_fields() {
    let e = V2Error::new(ErrorCode::EventTooLarge, "payload exceeds 64KB")
        .with_suggestion("store as artifact and reference via artifact_refs");
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v["code"], "EVENT_TOO_LARGE");
    assert_eq!(v["retryable"], false);
    assert!(v["suggestion"].as_str().unwrap().contains("artifact"));
    assert!(v["message"].as_str().unwrap().contains("64KB"));
}

#[test]
fn rate_limited_is_retryable() {
    let e = V2Error::new(ErrorCode::RateLimited, "tenant over qps");
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v["retryable"], true);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_errors
```

Expected: FAIL.

- [ ] **Step 3: Implement V2Error**

```rust
// crates/agentdb-core/src/v2/errors.rs
use serde::Serialize;

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    TenantNotFound,
    AgentNotFound,
    SessionNotFound,
    ArtifactNotFound,
    RecordNotFound,
    SkillNotFound,
    PolicyNotFound,
    EventTooLarge,
    ArtifactTooLarge,
    HashMismatch,
    IdempotencyConflict,
    PolicyDenied,
    InvalidCursor,
    RateLimited,
    Internal,
}

impl ErrorCode {
    pub fn retryable(&self) -> bool {
        matches!(self, ErrorCode::RateLimited | ErrorCode::Internal)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct V2Error {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

impl V2Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.retryable(),
            suggestion: None,
        }
    }

    pub fn with_suggestion(mut self, s: impl Into<String>) -> Self {
        self.suggestion = Some(s.into());
        self
    }
}
```

Add `pub mod errors;` to `crates/agentdb-core/src/v2/mod.rs` if not already
done in Task 1.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_errors
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/errors.rs crates/agentdb-core/tests/v2_errors.rs
git commit -m "feat(core): structured v2 errors with codes, retryable, suggestion (V2.0)"
```

### Task 5: Tenant runtime object

**Files:**

- Create: `crates/agentdb-core/src/v2/tenant.rs`
- Test: `crates/agentdb-core/tests/v2_tenant.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_tenant.rs
use agentdb_core::v2::tenant::{Tenant, NewTenant};

#[test]
fn create_tenant_assigns_typed_id() {
    let req = NewTenant { external_key: "acme".into(), policy: serde_json::json!({}) };
    let t = Tenant::create(req);
    assert!(t.tenant_id.as_str().starts_with("ten_"));
    assert_eq!(t.external_key, "acme");
    assert!(t.created_at_ms > 0);
}

#[test]
fn tenant_round_trips_through_serde() {
    let t = Tenant::create(NewTenant { external_key: "z".into(), policy: serde_json::json!({}) });
    let s = serde_json::to_string(&t).unwrap();
    let parsed: Tenant = serde_json::from_str(&s).unwrap();
    assert_eq!(parsed.tenant_id, t.tenant_id);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_tenant
```

Expected: FAIL.

- [ ] **Step 3: Implement Tenant type**

```rust
// crates/agentdb-core/src/v2/tenant.rs
use serde::{Deserialize, Serialize};
use crate::v2::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub tenant_id: Id,
    pub external_key: String,
    pub policy: serde_json::Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct NewTenant {
    pub external_key: String,
    pub policy: serde_json::Value,
}

impl Tenant {
    pub fn create(req: NewTenant) -> Self {
        Self {
            tenant_id: Id::new(IdKind::Tenant),
            external_key: req.external_key,
            policy: req.policy,
            created_at_ms: now_ms(),
        }
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
```

Add `pub mod tenant;` in `v2/mod.rs`. Note: `Id` needs `Serialize` +
`Deserialize` — go back to Task 2's `id.rs` and derive both on `Id` (using
`serde(transparent)` over the `inner: String` field, then implement
`Deserialize` to validate via `Id::parse`).

```rust
// crates/agentdb-core/src/v2/id.rs — additions:
use serde::{Serialize, Deserialize, Deserializer, Serializer};

impl Serialize for Id {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.inner)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let s = String::deserialize(de)?;
        Id::parse(&s).map_err(D::Error::custom)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_tenant
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/tenant.rs crates/agentdb-core/src/v2/id.rs crates/agentdb-core/tests/v2_tenant.rs
git commit -m "feat(core): v2 Tenant runtime object + Id serde (V2.0)"
```

### Task 6: Agent runtime object

**Files:**

- Create: `crates/agentdb-core/src/v2/agent.rs`
- Test: `crates/agentdb-core/tests/v2_agent.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_agent.rs
use agentdb_core::v2::agent::{Agent, NewAgent};
use agentdb_core::v2::id::{Id, IdKind};

#[test]
fn agent_belongs_to_tenant_and_has_alias() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Agent::create(NewAgent {
        tenant_id: tenant.clone(),
        alias: Some("sales-rep".into()),
        config: serde_json::json!({"model": "claude-sonnet-4.5"}),
    });
    assert!(a.agent_id.as_str().starts_with("agt_"));
    assert_eq!(a.tenant_id, tenant);
    assert_eq!(a.alias.as_deref(), Some("sales-rep"));
}

#[test]
fn agent_without_alias_is_allowed() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Agent::create(NewAgent {
        tenant_id: tenant,
        alias: None,
        config: serde_json::json!({}),
    });
    assert!(a.alias.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_agent
```

Expected: FAIL.

- [ ] **Step 3: Implement Agent**

```rust
// crates/agentdb-core/src/v2/agent.rs
use serde::{Deserialize, Serialize};
use crate::v2::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub agent_id: Id,
    pub tenant_id: Id,
    pub alias: Option<String>,
    pub config: serde_json::Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct NewAgent {
    pub tenant_id: Id,
    pub alias: Option<String>,
    pub config: serde_json::Value,
}

impl Agent {
    pub fn create(req: NewAgent) -> Self {
        Self {
            agent_id: Id::new(IdKind::Agent),
            tenant_id: req.tenant_id,
            alias: req.alias,
            config: req.config,
            created_at_ms: super::tenant::now_ms_pub(),
        }
    }
}
```

Refactor `now_ms()` from `tenant.rs` into a small shared helper or expose it
as `tenant::now_ms_pub()` for reuse. Simpler: move it into `v2/mod.rs` as
`pub(crate) fn now_ms()`. Update both `tenant.rs` and `agent.rs` to use it.

Add `pub mod agent;` in `v2/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_agent
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/{agent.rs,mod.rs,tenant.rs} crates/agentdb-core/tests/v2_agent.rs
git commit -m "feat(core): v2 Agent runtime object (V2.0)"
```

### Task 7: Session runtime object

**Files:**

- Create: `crates/agentdb-core/src/v2/session.rs`
- Test: `crates/agentdb-core/tests/v2_session.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_session.rs
use agentdb_core::v2::session::{Session, NewSession, Lifecycle, Status};
use agentdb_core::v2::id::{Id, IdKind};

#[test]
fn session_starts_active_warm_with_seq_zero() {
    let tenant = Id::new(IdKind::Tenant);
    let agent = Id::new(IdKind::Agent);
    let s = Session::create(NewSession {
        tenant_id: tenant.clone(),
        agent_id: agent.clone(),
        alias: Some("lead-followup-123".into()),
    });
    assert!(s.session_id.as_str().starts_with("ses_"));
    assert_eq!(s.tenant_id, tenant);
    assert_eq!(s.agent_id, agent);
    assert_eq!(s.next_seq, 0);
    assert_eq!(s.status, Status::Active);
    assert_eq!(s.lifecycle, Lifecycle::Warm);
    assert!(s.forked_from.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_session
```

Expected: FAIL.

- [ ] **Step 3: Implement Session**

```rust
// crates/agentdb-core/src/v2/session.rs
use serde::{Deserialize, Serialize};
use crate::v2::id::{Id, IdKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status { Active, Paused, Completed, Failed }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle { Warm, Cold, Archive, DerivativeOnly }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkRef {
    pub session_id: Id,
    pub at_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: Id,
    pub tenant_id: Id,
    pub agent_id: Id,
    pub alias: Option<String>,
    pub status: Status,
    pub lifecycle: Lifecycle,
    pub next_seq: u64,
    pub forked_from: Option<ForkRef>,
    pub created_at_ms: u64,
    pub last_active_at_ms: u64,
    pub policy_version: u64,
}

#[derive(Debug, Clone)]
pub struct NewSession {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub alias: Option<String>,
}

impl Session {
    pub fn create(req: NewSession) -> Self {
        let now = super::now_ms();
        Self {
            session_id: Id::new(IdKind::Session),
            tenant_id: req.tenant_id,
            agent_id: req.agent_id,
            alias: req.alias,
            status: Status::Active,
            lifecycle: Lifecycle::Warm,
            next_seq: 0,
            forked_from: None,
            created_at_ms: now,
            last_active_at_ms: now,
            policy_version: 1,
        }
    }
}
```

Add `pub mod session;` in `v2/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_session
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/session.rs crates/agentdb-core/src/v2/mod.rs crates/agentdb-core/tests/v2_session.rs
git commit -m "feat(core): v2 Session runtime object (V2.0)"
```

### Task 8: V2 Event envelope

**Files:**

- Create: `crates/agentdb-core/src/v2/event.rs`
- Test: `crates/agentdb-core/tests/v2_event_envelope.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_event_envelope.rs
use agentdb_core::v2::event::{EventEnvelope, NewEvent};
use agentdb_core::v2::id::{Id, IdKind};

#[test]
fn envelope_carries_required_fields() {
    let e = EventEnvelope::create(NewEvent {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        session_id: Id::new(IdKind::Session),
        seq: 0,
        kind: "tool_result".into(),
        payload: serde_json::json!({"summary": "ok"}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: Some("client-1".into()),
        policy_version: 1,
    });
    assert!(e.event_id.as_str().starts_with("evt_"));
    assert_eq!(e.seq, 0);
    assert_eq!(e.kind, "tool_result");
    assert_eq!(e.schema_version, agentdb_core::v2::VERSION);
    assert_eq!(e.idempotency_key.as_deref(), Some("client-1"));
}

#[test]
fn envelope_round_trips_through_json() {
    let e = EventEnvelope::create(NewEvent {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        session_id: Id::new(IdKind::Session),
        seq: 7,
        kind: "skill_used".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
        policy_version: 1,
    });
    let s = serde_json::to_string(&e).unwrap();
    let back: EventEnvelope = serde_json::from_str(&s).unwrap();
    assert_eq!(back.event_id, e.event_id);
    assert_eq!(back.seq, 7);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_event_envelope
```

Expected: FAIL.

- [ ] **Step 3: Implement EventEnvelope**

```rust
// crates/agentdb-core/src/v2/event.rs
use serde::{Deserialize, Serialize};
use crate::v2::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: Id,
    pub content_type: Option<String>,
    pub bytes: Option<u64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    pub session_id: Option<Id>,
    pub seq: Option<u64>,
    pub event_id: Option<Id>,
    pub artifact_id: Option<Id>,
    pub record_id: Option<Id>,
    pub skill_id: Option<Id>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub event_id: Id,
    pub seq: u64,
    pub created_at_ms: u64,
    pub kind: String,
    pub schema_version: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub policy_version: u64,
}

#[derive(Debug, Clone)]
pub struct NewEvent {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub seq: u64,
    pub kind: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    pub idempotency_key: Option<String>,
    pub policy_version: u64,
}

impl EventEnvelope {
    pub fn create(req: NewEvent) -> Self {
        Self {
            tenant_id: req.tenant_id,
            agent_id: req.agent_id,
            session_id: req.session_id,
            event_id: Id::new(IdKind::Event),
            seq: req.seq,
            created_at_ms: super::now_ms(),
            kind: req.kind,
            schema_version: super::VERSION.to_string(),
            payload: req.payload,
            artifact_refs: req.artifact_refs,
            source_refs: req.source_refs,
            idempotency_key: req.idempotency_key,
            policy_version: req.policy_version,
        }
    }
}
```

Add `pub mod event;` in `v2/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_event_envelope
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/event.rs crates/agentdb-core/src/v2/mod.rs crates/agentdb-core/tests/v2_event_envelope.rs
git commit -m "feat(core): v2 EventEnvelope with artifact_refs, source_refs, schema_version (V2.0)"
```

### Task 9: V2 runtime — atomic append + fetch

**Files:**

- Create: `crates/agentdb-core/src/v2/runtime.rs`
- Create: `crates/agentdb-core/src/v2/idempotency.rs`
- Test: `crates/agentdb-core/tests/v2_runtime_append.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_runtime_append.rs
use agentdb_core::v2::runtime::{Runtime, AppendRequest};
use agentdb_core::v2::tenant::{Tenant, NewTenant};
use agentdb_core::v2::agent::{Agent, NewAgent};
use agentdb_core::v2::session::{Session, NewSession};

#[tokio::test]
async fn append_assigns_contiguous_seqs() {
    let rt = Runtime::in_memory();
    let t = rt.create_tenant(NewTenant { external_key: "acme".into(), policy: serde_json::json!({}) }).await.unwrap();
    let a = rt.create_agent(NewAgent { tenant_id: t.tenant_id.clone(), alias: None, config: serde_json::json!({}) }).await.unwrap();
    let s = rt.create_session(NewSession { tenant_id: t.tenant_id.clone(), agent_id: a.agent_id.clone(), alias: None }).await.unwrap();

    let r1 = rt.append(AppendRequest {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        session_id: s.session_id.clone(),
        kind: "ping".into(),
        payload: serde_json::json!({"i": 1}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
    }).await.unwrap();
    assert_eq!(r1.seq, 0);

    let r2 = rt.append(AppendRequest {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        session_id: s.session_id.clone(),
        kind: "ping".into(),
        payload: serde_json::json!({"i": 2}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
    }).await.unwrap();
    assert_eq!(r2.seq, 1);
}

#[tokio::test]
async fn append_and_fetch_returns_recent() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}))).await.unwrap();
    rt.append(AppendRequest::simple(&t, &a, &s, "y", serde_json::json!({}))).await.unwrap();

    let r3 = rt.append_and_fetch(
        AppendRequest::simple(&t, &a, &s, "z", serde_json::json!({})),
        /* last */ 5,
        /* max_bytes */ 1_000_000,
    ).await.unwrap();
    assert_eq!(r3.seq, 2);
    assert_eq!(r3.recent.len(), 3);
    assert_eq!(r3.recent.last().unwrap().seq, 2);
}

#[tokio::test]
async fn idempotent_append_returns_original_result() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req.idempotency_key = Some("k1".into());

    let r1 = rt.append(req.clone()).await.unwrap();
    let r2 = rt.append(req).await.unwrap();
    assert_eq!(r1.seq, r2.seq);
    assert_eq!(r1.event_id, r2.event_id);
}

#[tokio::test]
async fn idempotent_conflict_on_payload_change() {
    use agentdb_core::v2::errors::ErrorCode;
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let mut req1 = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req1.idempotency_key = Some("k1".into());
    rt.append(req1).await.unwrap();

    let mut req2 = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 2}));
    req2.idempotency_key = Some("k1".into());
    let err = rt.append(req2).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::IdempotencyConflict);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_runtime_append
```

Expected: FAIL.

- [ ] **Step 3: Implement Runtime + idempotency store**

Implementation strategy: in-memory `Runtime` for V2.0 to validate semantics
quickly. Persistent backing comes later in this same milestone (Task 11). The
tests above use `Runtime::in_memory()`.

```rust
// crates/agentdb-core/src/v2/idempotency.rs
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    pub payload_hash: u64,
    pub result_json: String,
}

#[derive(Default, Debug)]
pub struct IdempotencyStore {
    inner: Mutex<HashMap<String, IdempotencyRecord>>,
}

impl IdempotencyStore {
    pub fn lookup(&self, key: &str) -> Option<IdempotencyRecord> {
        self.inner.lock().unwrap().get(key).cloned()
    }
    pub fn put(&self, key: String, record: IdempotencyRecord) {
        self.inner.lock().unwrap().insert(key, record);
    }
}

pub fn hash_payload(payload: &serde_json::Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(payload).unwrap().hash(&mut h);
    h.finish()
}
```

```rust
// crates/agentdb-core/src/v2/runtime.rs
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

use crate::v2::id::Id;
use crate::v2::tenant::{Tenant, NewTenant};
use crate::v2::agent::{Agent, NewAgent};
use crate::v2::session::{Session, NewSession};
use crate::v2::event::{EventEnvelope, NewEvent, ArtifactRef, SourceRef};
use crate::v2::errors::{V2Error, ErrorCode};
use crate::v2::idempotency::{IdempotencyStore, IdempotencyRecord, hash_payload};

pub type V2Result<T> = Result<T, V2Error>;

#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub kind: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    pub idempotency_key: Option<String>,
}

impl AppendRequest {
    pub fn simple(t: &Tenant, a: &Agent, s: &Session, kind: &str, payload: serde_json::Value) -> Self {
        Self {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            session_id: s.session_id.clone(),
            kind: kind.into(),
            payload,
            artifact_refs: vec![],
            source_refs: vec![],
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendResponse {
    pub event_id: Id,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendAndFetchResponse {
    pub event_id: Id,
    pub seq: u64,
    pub recent: Vec<EventEnvelope>,
}

#[derive(Default, Debug)]
struct State {
    tenants: BTreeMap<String, Tenant>,
    agents: BTreeMap<String, Agent>,
    sessions: BTreeMap<String, Session>,
    events: BTreeMap<(String, u64), EventEnvelope>, // (session_id, seq)
}

pub struct Runtime {
    state: Mutex<State>,
    idem: IdempotencyStore,
}

impl Runtime {
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            idem: IdempotencyStore::default(),
        })
    }

    pub async fn create_tenant(&self, req: NewTenant) -> V2Result<Tenant> {
        let t = Tenant::create(req);
        self.state.lock().await.tenants.insert(t.tenant_id.as_str().to_string(), t.clone());
        Ok(t)
    }

    pub async fn create_agent(&self, req: NewAgent) -> V2Result<Agent> {
        let mut s = self.state.lock().await;
        if !s.tenants.contains_key(req.tenant_id.as_str()) {
            return Err(V2Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id)));
        }
        let a = Agent::create(req);
        s.agents.insert(a.agent_id.as_str().to_string(), a.clone());
        Ok(a)
    }

    pub async fn create_session(&self, req: NewSession) -> V2Result<Session> {
        let mut s = self.state.lock().await;
        if !s.agents.contains_key(req.agent_id.as_str()) {
            return Err(V2Error::new(ErrorCode::AgentNotFound, format!("{}", req.agent_id)));
        }
        let sess = Session::create(req);
        s.sessions.insert(sess.session_id.as_str().to_string(), sess.clone());
        Ok(sess)
    }

    pub async fn bootstrap_test_session(&self) -> (Tenant, Agent, Session) {
        let t = self.create_tenant(NewTenant { external_key: "test".into(), policy: serde_json::json!({}) }).await.unwrap();
        let a = self.create_agent(NewAgent { tenant_id: t.tenant_id.clone(), alias: None, config: serde_json::json!({}) }).await.unwrap();
        let s = self.create_session(NewSession { tenant_id: t.tenant_id.clone(), agent_id: a.agent_id.clone(), alias: None }).await.unwrap();
        (t, a, s)
    }

    pub async fn append(&self, req: AppendRequest) -> V2Result<AppendResponse> {
        let idem_full_key = req.idempotency_key.as_ref().map(|k| {
            format!("{}::{}::{}::{}", req.tenant_id, "append", req.session_id, k)
        });
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                let h = hash_payload(&req.payload);
                if rec.payload_hash != h {
                    return Err(V2Error::new(ErrorCode::IdempotencyConflict, "payload differs for the same idempotency key"));
                }
                let resp: AppendResponse = serde_json::from_str(&rec.result_json).unwrap();
                return Ok(resp);
            }
        }

        let mut s = self.state.lock().await;
        let session = s.sessions.get_mut(req.session_id.as_str())
            .ok_or_else(|| V2Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id)))?;

        let seq = session.next_seq;
        let envelope = EventEnvelope::create(NewEvent {
            tenant_id: req.tenant_id.clone(),
            agent_id: req.agent_id.clone(),
            session_id: req.session_id.clone(),
            seq,
            kind: req.kind.clone(),
            payload: req.payload.clone(),
            artifact_refs: req.artifact_refs.clone(),
            source_refs: req.source_refs.clone(),
            idempotency_key: req.idempotency_key.clone(),
            policy_version: session.policy_version,
        });
        let key = (req.session_id.as_str().to_string(), seq);
        s.events.insert(key, envelope.clone());
        session.next_seq += 1;
        session.last_active_at_ms = super::now_ms();

        let resp = AppendResponse { event_id: envelope.event_id, seq };
        if let Some(full) = idem_full_key {
            self.idem.put(full, IdempotencyRecord {
                payload_hash: hash_payload(&req.payload),
                result_json: serde_json::to_string(&resp).unwrap(),
            });
        }
        Ok(resp)
    }

    pub async fn append_and_fetch(&self, req: AppendRequest, last: usize, _max_bytes: usize) -> V2Result<AppendAndFetchResponse> {
        let session_id = req.session_id.clone();
        let appended = self.append(req).await?;
        let s = self.state.lock().await;
        let recent: Vec<EventEnvelope> = s.events.range(
            (session_id.as_str().to_string(), 0)..=(session_id.as_str().to_string(), u64::MAX)
        ).rev().take(last).map(|(_, v)| v.clone()).collect::<Vec<_>>().into_iter().rev().collect();
        Ok(AppendAndFetchResponse {
            event_id: appended.event_id,
            seq: appended.seq,
            recent,
        })
    }
}
```

Add `pub mod runtime;` and `pub mod idempotency;` in `v2/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_runtime_append
```

Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/{runtime.rs,idempotency.rs,mod.rs} crates/agentdb-core/tests/v2_runtime_append.rs
git commit -m "feat(core): v2 Runtime with atomic append + idempotency (V2.0)"
```

### Task 10: V2 fetch by seq

**Files:**

- Modify: `crates/agentdb-core/src/v2/runtime.rs` (add `fetch_from_seq`)
- Test: `crates/agentdb-core/tests/v2_runtime_fetch.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_runtime_fetch.rs
use agentdb_core::v2::runtime::{Runtime, AppendRequest, FetchRequest};

#[tokio::test]
async fn fetch_from_seq_returns_ordered_events() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..5 {
        rt.append(AppendRequest::simple(&t, &a, &s, "k", serde_json::json!({"i": i}))).await.unwrap();
    }

    let events = rt.fetch_from_seq(FetchRequest {
        tenant_id: t.tenant_id.clone(),
        session_id: s.session_id.clone(),
        from_seq: 2,
        limit: 10,
    }).await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].seq, 2);
    assert_eq!(events[2].seq, 4);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_runtime_fetch
```

Expected: FAIL.

- [ ] **Step 3: Add FetchRequest + fetch_from_seq**

```rust
// addition to crates/agentdb-core/src/v2/runtime.rs
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub tenant_id: Id,
    pub session_id: Id,
    pub from_seq: u64,
    pub limit: usize,
}

impl Runtime {
    pub async fn fetch_from_seq(&self, req: FetchRequest) -> V2Result<Vec<EventEnvelope>> {
        let s = self.state.lock().await;
        if !s.sessions.contains_key(req.session_id.as_str()) {
            return Err(V2Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id)));
        }
        let lo = (req.session_id.as_str().to_string(), req.from_seq);
        let hi = (req.session_id.as_str().to_string(), u64::MAX);
        Ok(s.events.range(lo..=hi).take(req.limit).map(|(_, v)| v.clone()).collect())
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_runtime_fetch
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/runtime.rs crates/agentdb-core/tests/v2_runtime_fetch.rs
git commit -m "feat(core): v2 fetch_from_seq (V2.0)"
```

### Task 11: KV-backed runtime persistence

**Files:**

- Modify: `crates/agentdb-core/src/v2/runtime.rs` (add `Runtime::with_kv`)
- Create: `crates/agentdb-core/src/v2/persist.rs`
- Test: `crates/agentdb-core/tests/v2_runtime_restart.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-core/tests/v2_runtime_restart.rs
use agentdb_core::v2::runtime::{Runtime, AppendRequest, FetchRequest};
use agentdb_storage::rocks::RocksKvStore;
use tempfile::TempDir;

#[tokio::test]
async fn events_and_next_seq_survive_restart() {
    let dir = TempDir::new().unwrap();
    let kv = std::sync::Arc::new(RocksKvStore::open(dir.path()).unwrap());

    let rt = Runtime::with_kv(kv.clone());
    let (t, a, s) = rt.bootstrap_test_session().await;
    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 1}))).await.unwrap();
    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 2}))).await.unwrap();
    drop(rt);

    let rt2 = Runtime::with_kv(kv);
    let events = rt2.fetch_from_seq(FetchRequest {
        tenant_id: t.tenant_id.clone(),
        session_id: s.session_id.clone(),
        from_seq: 0,
        limit: 100,
    }).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].seq, 1);

    // next_seq should be 2 after restart
    let r3 = rt2.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 3}))).await.unwrap();
    assert_eq!(r3.seq, 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-core --test v2_runtime_restart
```

Expected: FAIL.

- [ ] **Step 3: Implement KV-backed persistence**

Use `agentdb-storage::v2_keys` for key encoding. Persist tenant, agent,
session, event records as JSON values under their respective keys. On
`Runtime::with_kv` startup, scan session records and rebuild the in-memory
session map. (Events do not need to be loaded eagerly; serve them via KV scan
on `fetch_from_seq` from now on.)

Sketch in `crates/agentdb-core/src/v2/persist.rs`:

```rust
use agentdb_storage::KvStore;
use agentdb_storage::v2_keys::*;
use crate::v2::tenant::Tenant;
use crate::v2::agent::Agent;
use crate::v2::session::Session;
use crate::v2::event::EventEnvelope;
use std::sync::Arc;

pub struct V2Persist { pub kv: Arc<dyn KvStore + Send + Sync> }

impl V2Persist {
    pub fn put_tenant(&self, t: &Tenant) -> std::io::Result<()> {
        self.kv.put(tenant_record_key(t.tenant_id.as_str()).as_bytes(),
                   serde_json::to_vec(t).unwrap().as_slice())
    }
    pub fn put_agent(&self, a: &Agent) -> std::io::Result<()> {
        self.kv.put(agent_record_key(a.tenant_id.as_str(), a.agent_id.as_str()).as_bytes(),
                   serde_json::to_vec(a).unwrap().as_slice())
    }
    pub fn put_session(&self, s: &Session) -> std::io::Result<()> {
        self.kv.put(session_record_key(s.tenant_id.as_str(), s.session_id.as_str()).as_bytes(),
                   serde_json::to_vec(s).unwrap().as_slice())
    }
    pub fn put_event(&self, e: &EventEnvelope) -> std::io::Result<()> {
        self.kv.put(event_key(e.tenant_id.as_str(), e.session_id.as_str(), e.seq).as_bytes(),
                   serde_json::to_vec(e).unwrap().as_slice())
    }
    pub fn get_session(&self, tenant_id: &str, session_id: &str) -> std::io::Result<Option<Session>> {
        match self.kv.get(session_record_key(tenant_id, session_id).as_bytes())? {
            Some(b) => Ok(Some(serde_json::from_slice(&b).unwrap())),
            None => Ok(None),
        }
    }
    pub fn scan_events(&self, tenant_id: &str, session_id: &str, from_seq: u64, limit: usize) -> std::io::Result<Vec<EventEnvelope>> {
        let prefix = format!("event/{tenant_id}/{session_id}/");
        let start_key = event_key(tenant_id, session_id, from_seq);
        let mut out = Vec::with_capacity(limit);
        for (k, v) in self.kv.scan_prefix_from(start_key.as_bytes(), prefix.as_bytes(), limit)? {
            let _ = k;
            out.push(serde_json::from_slice(&v).unwrap());
        }
        Ok(out)
    }
}
```

The `KvStore` trait may need a `scan_prefix_from` helper if it doesn't exist
— add it (and the corresponding RocksDB / SlateDB impls) as part of this task.
Look at existing `agentdb-storage/src/rocks.rs` for prior-art scan helpers
before adding a new one.

In `runtime.rs`:

```rust
impl Runtime {
    pub fn with_kv(kv: Arc<dyn KvStore + Send + Sync>) -> Arc<Self> {
        // For V2.0: keep the in-memory hot maps but back every mutation with put_*.
        // On startup, hydrate sessions+next_seq from KV scan; events are served via KV scan.
        // Replace the in-memory `events` BTreeMap; route fetch_from_seq through V2Persist.
        // ... (full implementation)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-core --test v2_runtime_restart
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-core/src/v2/{runtime.rs,persist.rs,mod.rs} crates/agentdb-storage/src/{rocks.rs,slate.rs,lib.rs} crates/agentdb-core/tests/v2_runtime_restart.rs
git commit -m "feat(core): KV-backed v2 Runtime; events and sessions survive restart (V2.0)"
```

### Task 12: V2 HTTP routes

**Files:**

- Create: `crates/agentdb-api/src/v2/{mod,routes,dto,errors}.rs`
- Modify: `crates/agentdb-api/src/lib.rs` (mount V2 router)
- Modify: `bin/agentdb-server/src/serve.rs` (serve native runtime API directly;
  no `--v2` flag)
- Test: `crates/agentdb-api/tests/v2_http_smoke.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/agentdb-api/tests/v2_http_smoke.rs
// Use the existing test harness in agentdb-api/tests/common/mod.rs.
use agentdb_api_tests::common::v2_test_app;
use serde_json::json;

#[tokio::test]
async fn create_tenant_agent_session_append_fetch_round_trip() {
    let app = v2_test_app().await;
    let client = app.client();

    let t: serde_json::Value = client.post("/api/tenants").json(&json!({"external_key":"acme"})).send().await.unwrap().json().await.unwrap();
    let tid = t["tenant_id"].as_str().unwrap();

    let a: serde_json::Value = client.post(&format!("/api/tenants/{tid}/agents")).json(&json!({"alias":"sales-rep"})).send().await.unwrap().json().await.unwrap();
    let aid = a["agent_id"].as_str().unwrap();

    let s: serde_json::Value = client.post(&format!("/api/tenants/{tid}/agents/{aid}/sessions")).json(&json!({})).send().await.unwrap().json().await.unwrap();
    let sid = s["session_id"].as_str().unwrap();

    let r: serde_json::Value = client.post(&format!("/api/tenants/{tid}/sessions/{sid}/events"))
        .json(&json!({"kind":"x","payload":{"i":1}}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);

    let resp: serde_json::Value = client.get(&format!("/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10"))
        .send().await.unwrap().json().await.unwrap();
    let events = resp["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["seq"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn append_and_fetch_returns_recent() {
    let app = v2_test_app().await;
    let client = app.client();
    let (tid, _aid, sid) = app.bootstrap_session().await;

    let r: serde_json::Value = client.post(&format!("/api/tenants/{tid}/sessions/{sid}/append-and-fetch"))
        .json(&json!({"kind":"x","payload":{},"last":5,"max_bytes":1000000}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);
    assert_eq!(r["recent"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn idempotency_key_replays_result() {
    let app = v2_test_app().await;
    let client = app.client();
    let (tid, _aid, sid) = app.bootstrap_session().await;

    let r1: serde_json::Value = client.post(&format!("/api/tenants/{tid}/sessions/{sid}/events"))
        .header("Idempotency-Key", "k1")
        .json(&json!({"kind":"x","payload":{"v":1}}))
        .send().await.unwrap().json().await.unwrap();
    let r2: serde_json::Value = client.post(&format!("/api/tenants/{tid}/sessions/{sid}/events"))
        .header("Idempotency-Key", "k1")
        .json(&json!({"kind":"x","payload":{"v":1}}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(r1["event_id"], r2["event_id"]);
    assert_eq!(r1["seq"], r2["seq"]);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p agentdb-api --test v2_http_smoke
```

Expected: FAIL — `v2_test_app` and `bootstrap_session` helpers + V2 routes do not exist.

- [ ] **Step 3: Implement native `/api` router, DTOs, error mapping, test harness**

Build out `crates/agentdb-api/src/v2/`:

- `mod.rs`: re-exports
- `dto.rs`: request/response DTOs that mirror `crate::v2::runtime` types but
  decouple wire shape (e.g., flatten `tenant_id` from path into the runtime request)
- `errors.rs`: map `V2Error` → `axum` response with HTTP status
  - `TenantNotFound`/`AgentNotFound`/`SessionNotFound` → 404
  - `EventTooLarge`/`HashMismatch`/`PolicyDenied` → 422
  - `IdempotencyConflict` → 409
  - `RateLimited` → 429
  - `Internal` → 500
- `routes.rs`: build `Router` with the routes used in the test
- `routes` keys off `Idempotency-Key` header; if present, set on `AppendRequest`

Modify `crates/agentdb-api/src/lib.rs`:

```rust
pub mod v2;

pub fn build_router(state: AppState) -> axum::Router {
    axum::Router::new()
        // ... admin/health mounting unchanged ...
        .merge(v2::routes::router(state.clone()))
}
```

Modify `bin/agentdb-server/src/serve.rs`:

```rust
// serve the native runtime API directly; do not add a --v2 flag.
```

Create `crates/agentdb-api/tests/common/mod.rs` (or extend the existing one)
with:

```rust
pub async fn v2_test_app() -> TestApp {
    // start a Runtime::with_kv on a temp dir, wrap in AppState, build_router(state),
    // launch on a random port, return a TestApp with `client()` and `bootstrap_session()`.
}
```

The exact wiring should match the harness already used by V1 tests
(`crates/agentdb-api/tests/common/`). Reuse not duplicate.

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p agentdb-api --test v2_http_smoke
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/agentdb-api/src/{lib.rs,routes.rs,dto.rs,errors.rs} crates/agentdb-api/tests/{common,v2_http_smoke.rs} bin/agentdb-server/src/serve.rs
git commit -m "feat(api): native runtime routes for tenant/agent/session/event"
```

### Task 13: V1 regression check + V2.0 PR

**Files:**

- None new; verification only.

- [ ] **Step 1: Run full V1 test suite to confirm no regression**

```bash
cargo test --workspace
```

Expected: PASS — native `/api/*` runtime tests pass.

- [ ] **Step 2: Run clippy**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 3: Run integration test against `agentdb serve`**

Start the server in one terminal:

```bash
cargo run -p agentdb-server -- serve --dev
```

In another terminal, exercise the native runtime routes manually:

```bash
curl -sX POST localhost:8080/api/tenants -H 'content-type: application/json' -d '{"external_key":"acme"}'
# capture tenant_id, then chain through agent → session → events
```

Expected: every step returns 200 with the documented JSON shape.

- [ ] **Step 4: Verify gap analysis exit criteria**

For each exit criterion in §2 V2.0 above, confirm:

- [x] V2 tenant create works
- [x] Agent register works
- [x] Session open works
- [x] Event append + fetch round trip works
- [x] `seq` monotonic + contiguous across restart (Task 11 test)
- [x] Idempotency-Key replay (Task 9 + Task 12 tests)
- [x] V1 tests still green
- [x] Native `/api/...` routes are mounted directly by `agentdb serve --dev`

- [ ] **Step 5: Open the V2.0 PR**

```bash
gh pr create --title "feat: V2 scaffolding — tenant/agent/session/event + idempotency (V2.0)" --body "$(cat <<'EOF'
## Summary
- V2 runtime scaffolding from docs/v2_design (V2 §11.2 change-list)
- Typed-prefix UUIDv7 IDs, V2 event envelope with artifact_refs/source_refs/schema_version
- Tenant/Agent/Session as first-class objects under native /api/...
- Atomic append + append-and-fetch + Idempotency-Key replay
- KV-backed: events and sessions survive restart
- Native runtime API mounted directly; no `--v2` compatibility flag

## Test plan
- [x] cargo test --workspace
- [x] cargo clippy --workspace --all-targets -- -D warnings
- [x] Manual smoke against `serve --dev`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## §4 Spec-Coverage Cross-Reference

Every section of `agent-runtime-database.md` and
`minimal-single-machine-implementation.md` is implemented by at least one
milestone.

| Spec section | Milestone(s) |
|---|---|
| V2 §1 Positioning | Captured in docs; no code work |
| V2 §2 Why existing storage is not enough | Captured in docs |
| V2 §3 Target use cases | Validated by V2.8 framework adapter |
| V2 §4 Architectural boundary | V2.0 (runtime/storage split established) |
| V2 §5 First-class runtime objects | V2.0 (tenant/agent/session/event), V2.1 (artifact), V2.2 (record), V2.3 (skill), V2.4 (policy), V2.5 (subscription/context) |
| V2 §6 Stable core vs extensible semantics | V2.0 (`schema_version` field on all events) |
| V2 §7 Data model | V2.0 + V2.1 + V2.2 + V2.3 |
| V2 §8 Event/artifact/record/skill split | V2.1 (artifact thresholds enforced) |
| V2 §9 Runtime core responsibilities | All milestones |
| V2 §10 Storage engine capabilities | V2.0 (ordered streams, metadata records), V2.1 (artifact store), V2.2 (indexing hooks), V2.4 (lifecycle), V2.5 (changefeed) |
| V2 §11 Lessons from V1 prototype | This entire plan honors §11.1 keep-list and §11.2 change-list |
| V2 §12 Managed-service data plane | Partial (V2.4 covers tenant isolation + admission; full coverage is V2.9 P1 backlog) |
| V2 §13–14 ID allocation + summary | V2.0 |
| V2 §15 Storage layout sketch | V2.0 (key encoders) + V2.1 (artifact metadata and content backend refs) |
| V2 §16 API sketch + adapter requirements | V2.0–V2.5 (native API), V2.6 (SDKs), V2.8 (LangGraph adapter as proof) |
| V2 §17 Policy model | V2.4 |
| V2 §18 Lifecycle model | V2.4 |
| V2 §19 Subscription semantics | V2.5 |
| V2 §20 What belongs in AgentDB vs application | This plan honors the boundary |
| V2 §21 Prototype priorities | Roadmap matches §21 P0 list |
| V2 §22 Non-goals | V2.7 (delete `agentdb-wire`, `clients/go`); KMS/region routing deferred to V2.9 backlog |
| Impl §1–§4 Goal/non-goals/SDK/repo | V2.0 sets foundation |
| Impl §5 Local storage | V2.0 (uses existing RocksDB+SlateDB+local FS) |
| Impl §6 Core data types | V2.0 |
| Impl §7 Logical storage keys | V2.0 (`v2_keys.rs`) |
| Impl §8 Runtime API contract | V2.0–V2.5 |
| Impl §9 Concurrency + idempotency | V2.0 (P0 idempotency scope) |
| Impl §10 Fork semantics | V2.5 |
| Impl §11 Minimal policy model | V2.4 |
| Impl §12 Context builder | V2.5 |
| Impl §13 Structured errors | V2.0 |
| Impl §14 Test plan | Each milestone's test list satisfies the relevant subset |
| Impl §15 Build order | This roadmap follows §15 grouped into bigger PRs |
| Impl §16 Framework adapter proof | V2.8 |

---

## §5 Self-Review

Per the writing-plans skill self-review checklist:

**1. Spec coverage:** §4 above maps every spec section to a milestone. KMS hooks
(V2 §12.6), region routing (V2 §12.5), and OpenAI/Vercel adapters are explicitly
deferred to V2.9 P1 backlog and noted as such.

**2. Placeholder scan:** Skill says no `TBD`, no "implement later", no "similar
to Task N". Re-checked V2.0 task plan: every step has actual code or commands.
The dashboard frontend tech (Yew vs Leptos vs embedded SPA) is explicitly
deferred to the V2.8 dashboard plan because it requires a separate brainstorming
pass on UI tech — that's a known scoping decision, not a placeholder.

**3. Type consistency:** `Id`, `IdKind`, `Tenant`, `Agent`, `Session`,
`EventEnvelope`, `ArtifactRef`, `SourceRef`, `AppendRequest`, `AppendResponse`,
`AppendAndFetchResponse`, `FetchRequest`, `V2Error`, `ErrorCode` — all defined
in Tasks 2–10 with consistent names. Method names: `Tenant::create`,
`Agent::create`, `Session::create`, `EventEnvelope::create`, `Runtime::append`,
`Runtime::append_and_fetch`, `Runtime::fetch_from_seq` — consistent.

**4. Big-PR check:** §1.1 confirms V2.0–V2.8 are each one PR per milestone (~9
PRs total to land V2). This matches the user's "AgentDB: big PRs, not tiny ones"
preference and is far from a 30-tiny-PRs failure mode.

---

## Execution Handoff

Plan complete and saved to `docs/v2_design/v2-implementation-plan.md`. Two
execution options:

**1. Subagent-Driven (recommended)** — Per `CLAUDE.md` project convention:
"Always use subagent-driven mode for complex tasks." Dispatch a fresh subagent
per task in V2.0, review between tasks, then for V2.1+ author each milestone's
detailed plan first then dispatch.

**2. Inline Execution** — Execute V2.0 tasks in the current session using
`superpowers:executing-plans`, batch checkpoint reviews. Riskier given V2.0
spans 13 tasks across 3 crates; recommend only if the user wants to drive
each commit interactively.

Which approach for V2.0?
