# AgentDB Design

AgentDB V2 is a runtime database for AI agent platforms. It is designed around
agent-native objects rather than a document-store compatibility facade.

## Product Thesis

Agent platforms repeatedly need the same primitives:

- ordered session history
- append-and-fetch for the agent turn loop
- artifact offload for large or binary payloads
- searchable scoped memory records
- skill registry and exact-version replay
- fork and replay for branchable runs
- subscriptions/watch cursors for observers and dashboards
- policy, quota, retention, and GC per tenant

AgentDB makes those primitives first-class so framework adapters are mostly
mapping code.

## Active Interfaces

The supported public interface is the native V2 HTTP API mounted under
`/api/...`, plus the Python and TypeScript SDKs. Route paths are tenant-scoped
and use typed opaque IDs.

The V2 prototype intentionally does not maintain:

- a MongoDB wire-protocol facade
- a Go SDK
- tenantless V1 session routes
- a `--v2` server flag

## Identity

IDs are public opaque strings:

| Kind | Prefix |
|---|---|
| Tenant | `ten_` |
| Agent | `agt_` |
| Session | `ses_` |
| Event | `evt_` |
| Artifact | `art_` |
| Record | `rec_` |
| Skill | `skl_` |
| Policy | `pol_` |

Clients must not parse meaning from the suffix. The prefix is for validation,
debugging, and route-shape clarity.

## Data Model

- **Tenant**: isolation, policy, retention, and scheduling boundary.
- **Agent**: actor configuration inside one tenant.
- **Session**: ordered execution log with lifecycle state and optional fork
  parent.
- **Event**: session-local sequence, typed kind, JSON payload, artifact refs,
  source refs, schema version, idempotency key, and policy version.
- **Artifact**: metadata plus content-backend bytes.
- **Record**: searchable memory with namespace, scope, metadata, tags, source
  refs, and indexing policy.
- **Skill**: manifest/version plus optional artifact refs; bindings attach
  skills to agents.

## Runtime Semantics

Append is serialized per session and produces contiguous session-local `seq`
values. `append-and-fetch` writes one event and returns a recent-event slice in
the same runtime operation. Idempotency is part of mutating runtime calls so
retrying clients do not duplicate events or registrations.

Fork creates a child session with fresh child-local sequence numbers and replay
visibility over parent events through the fork point. Replay is explicit and
fork-aware.

Subscribe currently emits replay-style SSE frames from a requested sequence.
Tenant/session/agent watch returns changefeed items with cursors for higher
level consumers.

## Policy, Lifecycle, And GC

Policy is resolved per tenant with agent overrides. It covers quota admission,
backpressure headers, lifecycle transitions, context budgets, retention, legal
hold, and GC scheduling. Deletes are soft first; GC later performs physical
purge in a policy-controlled order.

## Storage

Runtime metadata is stored through the existing KV abstraction. Local persistent
mode uses RocksDB. Artifact content uses a separate content backend so metadata
and blob bytes can be placed, retained, and purged independently.

## SDKs And Adapters

Python and TypeScript are the maintained SDKs for V2. The next validation step
is framework adapters, starting with LangGraph, to prove the native primitives
cover real agent framework persistence needs without recreating ordering,
forking, idempotency, artifact, search, and replay behavior in adapter code.
