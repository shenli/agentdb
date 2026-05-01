# AgentDB Design

AgentDB is a runtime database for AI agent platforms. It is designed around
agent-native objects rather than a document-store compatibility facade.

## Product Thesis

Long-running agent platforms need a stable boundary between:

- **brains**: models, harnesses, planners, and agent loops
- **hands**: sandboxes, tools, MCP servers, browsers, filesystems, and external
  systems
- **sessions**: durable execution state that lets brains recover, inspect,
  resume, replay, fork, and audit work

AgentDB owns the session layer. It is not the model runtime, harness, sandbox,
or workflow engine. A harness should be able to crash and restart with
`wake(session_id)`, fetch the durable event log, append the next event, and
continue. A sandbox or tool should be replaceable because its outputs and errors
are recorded as session events and artifacts rather than hidden inside a process.

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

The session is not the model context window. Context windows are transient,
lossy, model-specific, and often transformed by compaction or trimming. The
session log is durable, ordered, queryable, and recoverable. Context builders and
framework adapters may choose which slices, summaries, records, artifacts, and
skills to pass to a model, but AgentDB preserves the underlying execution truth.

## Active Interfaces

The supported public interface is the native HTTP API mounted under `/api/...`,
plus the Python and TypeScript SDKs. Route paths are tenant-scoped and use typed
opaque IDs.

The current prototype intentionally does not maintain:

- a MongoDB wire-protocol facade
- a Go SDK
- tenantless session routes from earlier prototypes
- a version-gating server flag

HTTP is the right public API for the current product shape: agent frameworks,
SDKs, dashboards, and local operators can use it without custom database
drivers, it handles JSON/event payloads naturally, and SSE gives a simple
subscription path. AgentDB should not pretend HTTP is a universal database wire
protocol. If the data plane needs lower overhead later, the right evolution is
to keep HTTP as the public/operational surface and add a binary SDK transport
or gRPC-style internal node protocol for hot paths.

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

The canonical harness flow is:

```text
wake(session_id)
get events, records, artifacts, skills, and policy hints
build model context in the harness
call the model
execute requested tools or sandboxes as external hands
append tool calls, tool results, model outputs, approvals, and artifacts
repeat or sleep
```

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

Python and TypeScript are the maintained SDKs. Framework adapters prove that the
native primitives cover real agent framework persistence needs without
recreating ordering, forking, idempotency, artifact, search, and replay behavior
in adapter code.
