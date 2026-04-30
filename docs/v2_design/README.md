# AgentDB V2 Design

Status: draft

Date: 2026-04-28

Implementation checkpoint:

- V2.0 tenant/agent/session/event core, V2.1 artifacts, V2.2 searchable
  records/indexing policy, V2.3 skills, and the V2.4a hot-path policy
  enforcement slice are implemented in the Rust runtime/API prototype.
- V2.4b adds policy-driven session lifecycle transitions, derivative-only
  artifact raw purge, soft-deleted record/artifact GC, and legal-hold
  retention skips in the runtime core.
- V2.4c adds built-in tier policy templates, record/artifact storage soft
  quota backpressure headers, active-session and session-event pressure
  signals, hard active-session and artifact-storage quotas, and tenant-scoped
  lifecycle/GC sweep API endpoints.
- V2.4d adds a deterministic policy worker tick that schedules tenant
  lifecycle/GC work by priority and exposes it through the API.
- V2.4e adds session soft-delete plus session/event hard purge in tenant-scoped
  GC sweeps.
- V2.4f adds skill soft-delete plus skill/binding hard purge in tenant-scoped
  GC sweeps.
- V2.4g adds idempotency retention GC for in-memory and persisted idempotency
  records.
- V2.4h completes the single-machine V2.4 prototype with expanded quota
  dimensions, context byte budgeting, tenant/agent soft-delete plus cascading
  GC, lifecycle/tier action counters, and purge-event counters for the future
  changefeed transport.
- V2.5 adds session fork/replay, replay-style SSE subscribe, generalized
  session/tenant/agent watch cursors, and budgeted context packs over recent
  events, records, skills, and policy.
- V2.6 rewrites the Python and TypeScript SDKs around the native tenant-scoped
  V2 API surface, including opaque typed IDs, append-and-fetch, replay/fork,
  subscribe/watch, artifacts, records, skills, context, and client-side
  idempotency key generation.
- V2.7 removes the remaining legacy SDK/test/doc surface around the old
  tenantless routes, Mongo wire facade, Go SDK, and broadcast channel helpers,
  leaving the native V2 HTTP API as the supported runtime surface.
- V2.8 adds the LangGraph checkpoint/store adapter package plus a mounted
  session-first dashboard for timeline, replay, fork, artifact, record, skill,
  context, watch, and SSE inspection.
- V2.9 covers the P1 integration backlog with an OpenAI Agents SDK session
  adapter, MCP stdio server, and Vercel AI SDK persistence template on top of
  the V2 API. Multi-node scale-out is tracked in V2.10+.
- V2.10 starts the multi-node scale-out path with a pure metadata routing
  planner over nodes, tablets, shards, session locators, leases, and residency
  policy.
- V2.11 adds the HTTP-facing routing contract in `agentdb-api`: route planner
  output now has concrete local/forward/reject outcomes, forwarding headers,
  retryable 503 unassigned responses, 421 wrong-location responses, and
  residency denial responses for the runtime data path to wire into handlers.
- V2.12 adds handler-facing proxy preparation: remote-owner outcomes can now
  build same-cluster forward URIs, sanitize hop-by-hop headers, propagate
  routing metadata, track `x-agentdb-forwarded-for`, and reject already-forwarded
  requests to prevent loops.
- V2.13 adds the runtime routing guard to `AppState`: single-node deployments
  stay disabled by default, while tests and future handler wiring can inject
  fixed planner decisions and receive local/forward/reject guard outcomes.
- V2.14 replaces fixed-only guard testing with metadata snapshot-backed route
  evaluation: `RouteRuntime` can compute local/forward/reject outcomes from
  node/tablet/shard/session-locator snapshots without async metadata reads
  inside request handlers.
- V2.15 wires the metadata-backed routing guard into session-scoped handlers,
  adds boot-time route snapshot loading from `agentdb-meta`, and short-circuits
  remote/unassigned decisions before local state is mutated.
- V2.16 executes same-cluster `ForwardRequest`s through an internal HTTP client
  and passes owner-node responses back through the original handler path.
- V2.17 validates forwarded tablet epochs on owner nodes before session
  mutations run, rejecting stale forwarded writes when ownership changed.
- V2.18 refreshes route snapshots after boot: handlers share an updateable
  routing runtime, the server reloads snapshots periodically, and Postgres
  shard/node/tablet notifications trigger faster refreshes.
- V2.19 adds production proxy policy knobs: forwarded requests use a bounded
  timeout and owner responses are rejected when they exceed the configured
  proxy body limit.
- V2.20 splits the prototype routing location label into durable
  `nodes.region` metadata with AZ fallback, adds `--route-region`, and wakes
  route snapshot refreshes immediately on Postgres `session_locators` changes.
- V2.21 adds operator-facing routing visibility with `GET /api/admin/routing`,
  exposing route mode, local node, region/AZ node metadata, tablet owners, and
  snapshot counts.
- V2.22 adds `GET /api/admin/routing/decision` dry-run diagnostics for a
  tenant and optional session id, returning the local/forward/reject decision
  without issuing a session request.
- V2.24 adds provider-neutral KMS/key availability signals at storage
  boundaries plus structured runtime/API and route rejection diagnostics for
  unavailable tenant keys.
- V2.25 adds pluggable envelope encryption/decryption adapters at the artifact
  content boundary while preserving plaintext artifact identity metadata.
- V2.26 extends envelope encryption/decryption adapters to artifact metadata
  and event KV rows while preserving existing secondary indexes.
- V2.27 records key id/version metadata for encrypted artifact content,
  artifact metadata, and event rows so future reads and audits can identify
  the key material used at write time.
- Current next milestone: V2 design implementation follow-ups are complete for
  the current prototype scope.

This directory contains the V2 design direction for AgentDB. It does not replace
the current implementation docs yet. It captures the architectural consensus
reached after reviewing the V1 prototype, product positioning, large payload
requirements, and AI Agents Platform use cases.

Core thesis:

> AgentDB is the durable session database for AI Agents Platforms.

Agent platforms should be able to decouple stateless brains from replaceable
hands. Brains are models, harnesses, planners, and agent loops. Hands are
sandboxes, MCP servers, tools, browsers, filesystems, and external systems.
AgentDB owns the session layer between them: the durable, ordered, replayable
execution state that survives harness crashes, tool failures, context resets, and
sandbox replacement.

The session is not the model context window. Context windows are transient and
model-specific; sessions are durable execution truth. Harnesses and adapters can
build whatever context shape a model needs from events, artifacts, searchable
records, skills, and policy hints, while AgentDB preserves the recoverable source
of truth underneath.

Framework integrations are both an adoption path and a validation workload. A
good AgentDB adapter for LangGraph, OpenAI Agents SDK, Vercel AI SDK, or similar
frameworks should be mostly mapping code because AgentDB already provides ordered
history, checkpoints, artifacts, scoped search, idempotency, subscriptions, and
lifecycle primitives.

SDK priority is Python first, then TypeScript. Go/Golang SDK or client support is
not part of the V2 prototype scope.

Start with:

- [`agent-runtime-database.md`](agent-runtime-database.md) — V2 positioning,
  data model, runtime/storage split, ID allocation, and design principles.
- [`minimal-single-machine-implementation.md`](minimal-single-machine-implementation.md)
  — implementation handoff for AI coding agents building the local V2 prototype.
- [`gap-analysis.md`](gap-analysis.md) — V1-to-V2 delta, including what stays,
  what changes, and which crates are deleted or rewritten.
- [`session-layer-gap-audit.md`](session-layer-gap-audit.md) — current
  architecture-to-product audit for AgentDB as the durable session layer between
  stateless brains and replaceable hands.
- [`session-event-conventions.md`](session-event-conventions.md) — advisory
  event vocabulary for common brain/hand/session events across SDKs, adapters,
  dashboards, and replay tools.
- [`v2-implementation-plan.md`](v2-implementation-plan.md) — milestone roadmap
  and detailed V2.0 task-level plan.
