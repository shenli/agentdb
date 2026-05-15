# AgentDB Coding-Agent Instructions

These instructions are for AI coding agents modifying this repository.

## Default Client Surface

When generating application code that uses AgentDB, prefer the native SDKs:

- Python: `clients/python`
- TypeScript: `clients/typescript`
- Native HTTP API: `docs/reference/api-sdk.md`

The native API is the canonical AgentDB runtime surface.

## Engineering Principles

- Quantify claims. When discussing performance, capacity, reliability,
  scalability, or cost, prefer concrete numbers, workloads, limits, and
  validation commands over qualitative claims. If exact numbers are unavailable,
  state the assumption and the measurement needed.
- Design for manageability. Prefer changes that are observable, debuggable,
  configurable, and operable by a small team. Include clear defaults, failure
  modes, logs/metrics, and rollback or migration considerations when behavior
  changes.
- Design for scalability, but do not hide current limits. Consider tenant count,
  session count, event volume, artifact size, concurrency, storage growth, and
  multi-node operation. Call out bottlenecks and whether the change improves,
  preserves, or worsens them.
- Keep operational complexity proportional to the feature. Avoid adding moving
  parts, background services, deployment requirements, or cross-component
  protocols unless they are justified by a concrete product or reliability need.
- Preserve the agent-runtime model. AgentDB is optimized for session events,
  records, artifacts, skills, context, fork/replay, and lifecycle operations;
  do not reshape features into generic CRUD/database abstractions without a
  clear reason.

## MongoDB Client Compatibility

AgentDB has a limited MongoDB wire adapter for clients and tools. It is not a
general MongoDB-compatible database.

When generating code against AgentDB through a MongoDB client:

- Always connect with `directConnection=true`.
- If username/password auth is configured, use
  `authMechanism=PLAIN&authSource=%24external`.
- Do not generate SCRAM, role, user-management, or database-auth code. The
  MongoDB listener supports only basic `PLAIN` authentication.
- Bind the MongoDB listener to localhost unless separate infrastructure
  provides access control and TLS.
- Treat the MongoDB database name as the AgentDB tenant external key.
- Use only fixed collections: `sessions`, `events`, `records`, `artifacts`,
  `skills`.
- Do not create arbitrary app collections such as `users`, `orders`, `tasks`,
  or `messages`.
- Do not generate `createCollection`.
- Do not rely on MongoDB indexes. `createIndexes` is a no-op compatibility
  command.
- Write events with `db.events.insertOne(...)` or
  `db.runCommand({ agentdbAppendAndFetch: ... })`.
- Use `session_alias` unless code already has a canonical `ses_*` ID.
- Use `db.runCommand(...)` for AgentDB primitives:
  `agentdbAppendAndFetch`, `agentdbFork`, `agentdbReplay`, `agentdbContext`,
  `agentdbTransitionLifecycle`, and `agentdbRestoreSession`.
- Do not generate `updateOne`, `deleteOne`, transactions, change streams, or
  broad aggregation pipelines.

Reference:

- `docs/reference/mongodb-compatibility.md`
- `examples/mongodb/node/quickstart.js`
- `examples/mongodb/python/quickstart.py`

## Verification

For MongoDB wire adapter changes, run:

```bash
cargo check -q -p agentdb-server
cargo test -q -p agentdb-wire
```
