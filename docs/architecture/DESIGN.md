# AgentDB Design

AgentDB is a runtime database for AI agent platforms. It is designed around
agent-native runtime objects rather than a compatibility facade for an existing
database protocol.

## Product Thesis

Long-running agent platforms need a stable boundary between:

- **brains**: models, harnesses, planners, context builders, and agent loops
- **hands**: sandboxes, tools, MCP servers, browsers, filesystems, and external
  systems
- **sessions**: durable execution state that lets brains recover, inspect,
  resume, replay, fork, and audit work

AgentDB owns the session layer. It is not the model runtime, harness, sandbox,
or workflow engine. A harness should be able to crash and restart with a
`session_id`, fetch durable state, append the next event, and continue. A tool or
sandbox should be replaceable because its outputs and errors are recorded as
session events and artifacts rather than hidden inside a process.

Agent platforms repeatedly need the same primitives:

- ordered session history
- append-and-fetch for the agent turn loop
- artifact offload for large or binary payloads
- searchable scoped records
- skill registry and exact-version replay
- fork and replay for branchable runs
- subscriptions and watch cursors for observers and dashboards
- policy, quota, retention, lifecycle, and garbage collection per tenant

AgentDB makes those primitives first-class so framework adapters are mostly
mapping code.

The session is not the model context window. Context windows are transient,
lossy, model-specific, and often transformed by compaction or trimming. The
session log is durable, ordered, queryable, and recoverable. Context builders and
framework adapters choose which slices, summaries, records, artifacts, and
skills to pass to a model, while AgentDB preserves the underlying execution
truth.

## Current Commitments

The current product shape is intentionally narrow:

- single-node operation must be excellent before distributed operation expands
- configuration is file-first through `agentdb.toml`
- Python and TypeScript are the maintained SDKs
- the native HTTP API is the public protocol and SDK substrate
- framework adapters should prove the runtime primitives, not reimplement them
- compatibility layers and extra SDKs are secondary until the core experience is
  stable

## Active Interfaces

The primary developer interface is the SDK surface. The supported public
protocol underneath it is the native HTTP API mounted under `/api/...`. Routes
are tenant-scoped and use typed opaque IDs.

HTTP is the right public API for the current product shape: agent frameworks,
SDKs, dashboards, examples, and local operators can use it without custom
database drivers, it handles JSON/event payloads naturally, and SSE gives a
simple subscription path.

AgentDB should not pretend HTTP is a universal database wire protocol. If the
data plane later needs lower overhead, the expected evolution is to keep HTTP as
the public and operational surface while adding a binary SDK transport or
internal node protocol for hot paths.

The current workspace intentionally does not maintain:

- a MongoDB wire-protocol facade
- a Go SDK
- tenantless routes from earlier prototypes
- a version-gating server flag

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

SDKs may let developers provide domain-friendly external names where that helps
ergonomics, but the server-assigned opaque IDs remain the durable identifiers.

## Data Model

- **Tenant**: isolation, policy, retention, quota, and lifecycle boundary.
- **Agent**: actor configuration inside one tenant.
- **Session**: ordered execution log with lifecycle state and optional fork
  parent.
- **Event**: session-local sequence, typed kind, JSON payload, artifact refs,
  source refs, schema version, idempotency key, and policy version.
- **Artifact**: metadata plus content-backend bytes.
- **Record**: searchable memory or observation with namespace, scope, metadata,
  tags, source refs, and indexing policy.
- **Skill**: manifest/version plus optional artifact refs; bindings attach
  skills to agents.
- **Policy**: tenant and agent behavior for quota, retention, lifecycle,
  context budgets, legal hold, and garbage collection.

## Runtime Semantics

Append is serialized per session and produces contiguous session-local `seq`
values. `append-and-fetch` writes one event and returns a recent-event slice in
the same runtime operation. Idempotency is part of mutating runtime calls so
retrying clients do not duplicate events or registrations.

Fork creates a child session with fresh child-local sequence numbers and replay
visibility over parent events through the fork point. Replay is explicit and
fork-aware.

Subscribe currently emits replay-style SSE frames from a requested sequence.
Tenant, session, and agent watch returns changefeed items with cursors for
higher-level consumers.

Record indexes follow local/global semantics. A search with `sharding_key` uses
the owner-local index and is the strong read-after-write boundary. A search
without `sharding_key` uses the global index, which is GSI-like and eventually
consistent. Record writes return a tenant-scoped commit sequence; callers can
pass `min_visible_record_commit_seq`, and SDKs propagate the latest sequence
automatically, so same-client reads repair from committed base records when an
async index is behind.

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

## Configuration And Operation

AgentDB is config-file first:

```bash
agentdb init-config
agentdb doctor --config agentdb.toml
agentdb serve --config agentdb.toml
```

`agentdb.toml` is the durable operational contract. CLI flags are explicit
overrides for local development, scripts, and tests. Server runtime behavior
should not depend on a large set of environment variables.

Docker, Compose, and Helm deployments should mount or generate a config file
instead of encoding normal runtime settings as environment variables.

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

Python and TypeScript are the maintained SDKs. They should make the common
agent path feel natural: create or open a workspace, create or open an agent,
create or open a session, append events, fetch context, store artifacts, search
records, and subscribe to progress.

Framework adapters validate that the native primitives cover real persistence
needs without recreating ordering, forking, idempotency, artifact, search, and
replay behavior in adapter code.
