# MongoDB Compatibility API Design Draft

## Status

Initial compatibility slice implemented.

Implemented scope:

- opt-in MongoDB wire listener
- `hello` / `isMaster`, `ping`, `buildInfo`, `endSessions`
- `listDatabases`, `listCollections`
- `insert`, `find`, `count`
- limited `aggregate` with `$match` and `$count`
- no-op `createIndexes`
- custom `agentdbAppendAndFetch`
- custom `agentdbFork`
- custom `agentdbReplay`
- custom `agentdbContext`
- custom `agentdbTransitionLifecycle`
- custom `agentdbRestoreSession`
- runtime-backed `sessions`, `events`, `records`, `artifacts`, and `skills`
  collection access

Still intentionally deferred:

- transactions
- replica-set emulation
- change streams
- full cursor continuation
- arbitrary update expressions
- arbitrary delete semantics
- MongoDB auth compatibility
- MongoDB sharding metadata compatibility

## Context

AgentDB currently exposes a native tenant-scoped HTTP API for agent workflows.
That API is intentionally semantic: tenants, agents, sessions, events, replay,
forking, context packing, artifacts, records, skills, lifecycle, watch, and
policy are first-class concepts.

An alternative API direction under consideration is to expose a familiar
database-compatible API instead of, or alongside, the native API. Two candidates
are:

- DynamoDB API plus AgentDB-specific extensions
- MongoDB API plus AgentDB-specific extensions

The working recommendation is:

**Keep the native AgentDB API as canonical. Add MongoDB compatibility as the
first broad compatibility layer. Borrow DynamoDB-style consistency and indexing
ideas internally where they fit.**

## Decision Direction

MongoDB compatibility is a better first compatibility API than DynamoDB
compatibility for AgentDB.

The native AgentDB API should remain the source of truth for agent-specific
operations. MongoDB compatibility should be an adapter for document access,
developer adoption, local inspection, and integration with existing drivers and
tools.

## Why MongoDB First

AgentDB's public data is naturally document-shaped:

- session metadata
- ordered event envelopes
- flexible event payloads
- searchable records
- artifact metadata
- skill manifests
- policy metadata

MongoDB's document model lets developers inspect and query that shape directly.
It also gives a strong tooling story through MongoDB drivers, `mongosh`, and
GUI tools such as Compass.

DynamoDB's API is useful for partition-key, sort-key, conditional-write, and
GSI/LSI thinking, but it would make AgentDB feel more like a low-level table
service. That is less aligned with the agent workflow model.

## API Role Split

### Canonical API

The native `/api/...` HTTP surface remains canonical for AgentDB semantics:

```text
append-and-fetch
replay
fork
context
watch / subscribe
artifact blob reads and writes
lifecycle transition and restore
policy and quota operations
cluster/admin operations
```

These operations should not be forced into generic MongoDB CRUD semantics.

### Compatibility API

The MongoDB-compatible surface exists for:

- existing MongoDB drivers
- `mongosh` inspection
- simple document insert and find workflows
- app developers who want familiar collection semantics
- future query/aggregation compatibility where it fits

The compatibility layer should call the same AgentDB runtime methods as the
native API. It should not duplicate ordering, idempotency, lifecycle, fork, or
policy behavior in a separate path.

## Proposed Mapping

```text
MongoDB database        -> AgentDB tenant or workspace external key
sessions collection     -> AgentDB session metadata
events collection       -> AgentDB append-only event log
records collection      -> AgentDB searchable records
artifacts collection    -> AgentDB artifact metadata
skills collection       -> AgentDB skill manifests and bindings
```

Example event insert:

```javascript
db.events.insertOne({
  session_id: "ses_...",
  agent_id: "agt_...",
  kind: "message",
  payload: {
    role: "user",
    content: "hello"
  }
})
```

Internally this should call the AgentDB append path, assign the next session
sequence number, enforce idempotency and policy, and return a document-shaped
representation of the committed event.

## Initial MongoDB Compatibility Subset

Start narrow and be explicit about compatibility boundaries.

Recommended first subset:

```text
hello / isMaster
ping
buildInfo
listDatabases
listCollections
find
insert
```

Likely next candidates:

```text
createIndexes
aggregate
countDocuments-style behavior
limited update for metadata collections only
limited delete for soft-delete-capable resources
```

Deferred until there is clear demand:

```text
transactions
replica-set emulation
change streams
full cursor management
arbitrary update expressions
arbitrary delete semantics
MongoDB auth compatibility
MongoDB sharding metadata compatibility
```

## AgentDB Extensions

AgentDB-specific operations can be exposed in two ways:

1. Prefer the native REST API and SDKs for first-class agent operations.
2. Optionally add custom MongoDB commands for convenience.

Example custom command:

```javascript
db.runCommand({
  agentdbAppendAndFetch: "events",
  session_id: "ses_...",
  agent_id: "agt_...",
  payload: {
    role: "user",
    content: "hello"
  },
  fetch_last_n: 20
})
```

Custom MongoDB commands should be convenience wrappers, not the primary SDK
contract.

## DynamoDB Ideas To Keep

DynamoDB is still useful as a design reference for consistency and indexing.

AgentDB should continue borrowing these ideas where they fit:

- partition-key and sort-key style physical organization
- conditional writes
- idempotent mutations
- GSI-like global indexes that are asynchronously maintained
- LSI-like local indexes scoped to an ownership boundary
- same-session read-after-write visibility through commit tokens and visibility
  fences

This means the product API can feel MongoDB-like while the indexing contract
borrows DynamoDB's useful consistency split.

## Risks

Full MongoDB compatibility is a large surface area. If AgentDB claims too much
compatibility too early, users will expect behavior that is expensive or
misaligned with the runtime model.

Specific risks:

- drivers may issue commands beyond the supported subset
- MongoDB update/delete semantics can conflict with append-only event history
- aggregation can become an unbounded compatibility project
- auth, replica-set, cursor, and change-stream behavior can create long-tail
  work
- custom commands may not be portable across tools

The mitigation is to document a strict subset, return clear unsupported-command
errors, and keep native AgentDB APIs as the stable semantic contract.

## Open Questions

- Should MongoDB database names map to tenant IDs, tenant aliases, or workspace
  external keys?
- Should `events.insertOne` require `session_id`, or should it create a session
  when one is omitted?
- Should `records.insertOne` call the record API directly or share event append
  semantics through source references?
- Should custom commands be implemented at all in the first version?
- How much of MongoDB query matching should be supported before aggregation?
- Should compatibility mode be enabled by default or require an explicit
  listener/config flag?
- What exact error shape should unsupported MongoDB commands return?

## Current Recommendation

Use MongoDB compatibility as the first database-compatible API surface.

Do not replace the native AgentDB API. Keep it canonical for agent workflows.

Use DynamoDB-style concepts internally for index consistency, placement, and
read-after-write guarantees.
