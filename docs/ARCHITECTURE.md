# AgentDB Architecture

This document describes the current prototype. Older Mongo-wire material has
been removed from the active architecture docs; historical implementation notes
remain under `docs/superpowers/`.

## Layers

```text
SDKs / HTTP clients
        |
        v
crates/agentdb-api        native tenant-scoped /api/... HTTP routes
        |
        v
crates/agentdb-core       runtime semantics: tenants, agents, sessions,
                          events, artifacts, records, skills, policy,
                          lifecycle, fork/replay, watch, context
        |
        +--> crates/agentdb-storage   KV + artifact content backends
        +--> crates/agentdb-index     text/vector helper indexes
        +--> crates/agentdb-meta      control-plane metadata helpers
```

`bin/agentdb-server` wires a `Runtime` into the API router. In-memory mode is
used for smoke tests and local throwaway runs. Persistent local mode stores
runtime metadata in RocksDB under `<data-dir>/runtime` and artifact content in
`<data-dir>/content` unless `--content-dir` is provided.

## Runtime Model

- **Tenant**: isolation, policy, quota, lifecycle, and watch boundary.
- **Agent**: runtime actor within a tenant.
- **Session**: ordered event log for one run or thread. Sessions can be forked.
- **Event**: append-only payload with tenant, agent, session, event ID, `seq`,
  kind, artifact refs, source refs, idempotency key, policy version, and time.
- **Artifact**: metadata in the runtime store plus bytes in the content backend.
- **Record**: searchable memory/document entry with namespace, scope, tags, and
  optional source refs.
- **Skill**: registered tool/package manifest and agent binding.
- **Policy**: quota, lifecycle, retention, legal hold, and priority settings.

All public IDs are opaque typed strings: `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`.

## HTTP Surface

The active API is the native `/api/...` route family:

```text
POST /api/tenants
GET  /api/tenants/{tenant_id}
POST /api/tenants/{tenant_id}/agents
POST /api/tenants/{tenant_id}/agents/{agent_id}/sessions
POST /api/tenants/{tenant_id}/sessions/{session_id}/events
GET  /api/tenants/{tenant_id}/sessions/{session_id}/events
POST /api/tenants/{tenant_id}/sessions/{session_id}/append-and-fetch
POST /api/tenants/{tenant_id}/sessions/{session_id}/fork
GET  /api/tenants/{tenant_id}/sessions/{session_id}/replay
GET  /api/tenants/{tenant_id}/sessions/{session_id}/subscribe
POST /api/tenants/{tenant_id}/sessions/{session_id}/context
GET  /api/tenants/{tenant_id}/watch
POST /api/tenants/{tenant_id}/artifacts
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}/blob
POST /api/tenants/{tenant_id}/records
GET  /api/tenants/{tenant_id}/records/search
POST /api/tenants/{tenant_id}/records/search
POST /api/tenants/{tenant_id}/skills
POST /api/tenants/{tenant_id}/agents/{agent_id}/skills/bind
POST /api/tenants/{tenant_id}/policy/lifecycle-sweep
POST /api/tenants/{tenant_id}/policy/gc-sweep
POST /api/policy/worker/run
GET  /health/ready
```

There is no version-gating server flag, Mongo wire listener, or maintained Go
SDK in the current workspace.

## Event Append

Append is atomic per session. The runtime assigns the next session-local `seq`,
stores an event envelope, stamps policy metadata, records idempotency results,
and returns either the append result or an append-and-fetch response. Retries
with the same `Idempotency-Key` return the same result unless the payload
conflicts.

`append-and-fetch` is the hot path for agent loops: write the current turn and
return recent context in one request.

## Replay, Subscribe, Watch, Context

- `replay` reconstructs a fork-aware event view.
- `subscribe` returns an SSE replay snapshot from `from_seq`.
- `watch` returns generalized tenant/session/agent changefeed items.
- `context` builds a budgeted pack from recent events, searched records, active
  skills, and policy.

The current subscribe endpoint is deterministic replay-style SSE. Long-held
live push can be layered onto the same watch cursor model.

## Storage

The runtime persists records through `KvStore`. The local implementation uses
RocksDB. Artifact content is intentionally separated from metadata through a
`ContentBackend`; local mode uses a filesystem content backend.

The storage crate still contains lower-level RocksDB/SlateDB primitives and KV
contract tests because the runtime reuses those foundations.

## Policy And GC

Policy evaluation covers append, artifact writes, record writes, skill bindings,
context byte budgets, active-session quotas, lifecycle work units, and GC work
units. Soft deletes mark runtime objects first; tenant-scoped GC sweeps later
purge metadata, index entries, and artifact content when policy allows.

## SDKs

The Python and TypeScript SDKs map directly to the native API. They generate
idempotency keys for mutating calls and expose the typed ID aliases as opaque
strings.
