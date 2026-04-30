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
- Current next milestone: fuller quota dimensions, remaining GC handlers, and
  changefeed delete/purge events.

This directory contains the V2 design direction for AgentDB. It does not replace
the current implementation docs yet. It captures the architectural consensus
reached after reviewing the V1 prototype, product positioning, large payload
requirements, and AI Agents Platform use cases.

Core thesis:

> AgentDB is the runtime database for AI Agents Platforms.

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
- [`v2-implementation-plan.md`](v2-implementation-plan.md) — milestone roadmap
  and detailed V2.0 task-level plan.
