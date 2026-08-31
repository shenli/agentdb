# AgentDB Implementation Architecture

This document describes the current implementation shape. The product thesis is
in [`product-thesis.md`](product-thesis.md), and the design commitments are in
[`design.md`](design.md).

## Layers

```text
agentdb.toml / CLI overrides
        |
        v
bin/agentdb-server
        |
        v
crates/agentdb-api        bearer-protected tenant-scoped /api/... HTTP routes
        |                 export/watch/backfill/consumer admin routes
        |                 public liveness/readiness and embedded dashboard
        |
        v
crates/agentdb-core       multi-tenant runtime semantics
                          tenants -> agents -> sessions -> events
                          artifacts, records, skills, policy,
                          lifecycle, fork/replay, context
                          watch/changefeed cursors
                          export ledger and managed consumer cursors
                          record index outbox
        |
        +--> crates/agentdb-storage   KV + artifact content backends
        |
        +--> crates/agentdb-index     owner-local record index
        |                             async global record index
        |                             text/vector helper indexes
        |
        +--> crates/agentdb-meta      control-plane metadata helpers

bin/agentdbctl            operations CLI
                          cluster/tablet administration
                          ClickHouse, ClickStack, JSONL, Parquet export flows
```

Runtime data is always tenant-scoped. Tenant IDs are the server-side boundary
for routing, policy, quota, lifecycle, storage accounting, watch/changefeed
visibility, and future placement. SDKs may bind that scope from authentication,
but the runtime model remains explicitly multi-tenant.

SDKs and raw HTTP clients talk to `agentdb-api`. The API layer should stay thin:
decode requests, apply route-level validation, call runtime methods, and encode
responses.

`agentdb-core` owns durable runtime semantics. Framework adapters and SDKs
should not need to recreate ordering, idempotency, fork visibility, lifecycle, or
policy behavior in client code.

Developer-facing SDKs add one ergonomic layer above the tenant-scoped HTTP
routes: they bind workspace, agent, and session handles around the underlying
tenant ID. In authenticated deployments, tenant selection should come from
user-info metadata rather than application code. Low-level `Client` methods
still expose tenant IDs for admin tooling, tests, and direct protocol
integrations.

## Server Startup

The server is config-file first:

```bash
agentdb init-config
agentdb doctor --config agentdb.toml
agentdb serve --config agentdb.toml
```

`agentdb.toml` describes the bind address, storage mode, data directory, content
directory, and development-mode behavior. CLI flags are explicit overrides for
local runs and tests.

Normal single-node runtime settings are not configured through environment
variables. Docker, Compose, and Helm should mount a config file and start the
server with `--config`. Secrets are deliberate exceptions:
`AGENTDB_API_AUTH_TOKEN` / `AGENTDB_API_AUTH_PREVIOUS_TOKEN` keep external API
credentials out of the file, and the internal Raft credentials have their own
environment overrides. Startup hydration safety caps are also
environment overrides because they are operational guardrails rather than
durable data semantics.

## Runtime Model

- **Tenant**: isolation, policy, quota, lifecycle, and watch boundary.
- **Agent**: runtime actor within a tenant.
- **Session**: ordered event log for one run or thread. Sessions can be forked.
- **Event**: append-only payload with tenant, agent, session, event ID, `seq`,
  kind, artifact refs, source refs, idempotency key, policy version, and time.
- **Artifact**: metadata in the runtime store plus bytes in the content backend.
- **Record**: searchable memory or observation with namespace, scope, tags, and
  optional source refs.
- **Skill**: registered tool/package manifest and agent binding.
- **Policy**: quota, lifecycle, retention, legal hold, and priority settings.

All public IDs are opaque typed strings: `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, `skl_*`, and `pol_*`.

## HTTP Surface

The canonical API is the native `/api/...` route family:

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
GET  /api/tenants/{tenant_id}/exports/watch
POST /api/tenants/{tenant_id}/exports/backfill
GET  /api/tenants/{tenant_id}/exports/status
POST /api/tenants/{tenant_id}/exports/consumers
GET  /api/tenants/{tenant_id}/exports/consumers/{name}
POST /api/tenants/{tenant_id}/exports/consumers/{name}/claim
POST /api/tenants/{tenant_id}/exports/consumers/{name}/ack
POST /api/tenants/{tenant_id}/exports/consumers/{name}/rewind
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
GET  /api/admin/exports
GET  /api/admin/tokens
POST /api/admin/tokens
DELETE /api/admin/tokens/{token_id}
GET  /health/live
GET  /health/ready
```

The native API has two bearer-token mechanisms. A rotating static installation
token protects every external `/api/*` route and is mandatory for a
non-loopback bind; either its current or previous value is accepted during
rotation. On `/api/admin/*`, metadata-backed tokens are an alternate credential
with `admin` or read-only scope. When no static token is configured, loopback
data routes remain open for local development and local admin bootstrap remains
open only until the first metadata-backed admin token is ever minted; revoking
all tokens does not reopen bootstrap. These controls
are coarse installation/admin authentication, not tenant-scoped end-user
authorization. Internal Raft routes keep a separate rotating token, while
health and dashboard assets remain public. Network deployments must terminate
TLS before the HTTP listener.

Dashboard HTML, JavaScript, and CSS are compiled into `agentdb`, so a release
binary does not depend on a source-tree asset directory. The page keeps a
manually entered bearer token in JavaScript memory only, removes legacy stored
tokens, and scrubs `token` / `access_token` query parameters from the URL.

Hosted/authenticated deployments should provide user-info metadata so SDKs can
bind the authenticated principal to a tenant/workspace. The server-side runtime
still treats tenant IDs as the routing, policy, quota, lifecycle, watch,
storage-accounting, and admin boundary; this is SDK/API ergonomics on top of a
multi-tenant runtime, not a tenantless data model.

The workspace intentionally does not maintain a version-gating server flag, a
Go SDK, a MongoDB wire-compatibility listener, or a SlateDB `KvStore` backend.
The native HTTP API is the only client protocol.

## Record Index Consistency

Record search follows DynamoDB-style secondary-index behavior:

- Searches with `sharding_key` use the owner-local LSI-like record index. This
  is the read-after-write path for a caller that knows the placement key.
- Searches without `sharding_key` use the GSI-like global record index. The
  global index is updated asynchronously from the durable record-index outbox
  and is eventually consistent.
- Record writes return `X-AgentDB-Record-Commit-Seq`. Reads can pass
  `min_visible_record_commit_seq` to require visibility through that commit.
  If the global index is stale for that token, the runtime falls back to
  owner-local repair from committed base records before replying.
- Python and TypeScript SDK clients remember the latest commit sequence per
  tenant and attach it to later record searches unless the caller supplied an
  explicit value.

Operational status is exposed at `GET /api/admin/indexes/records`, including
local/global watermarks, lag tokens, repair counters, and async apply failures.

The async global-index worker consumes durable record-index outbox entries. This
keeps the record write path durable without requiring the global derived index
to be synchronously up to date before the write returns.

```text
record write
    |
    v
committed base record  -----> owner-local index
    |                         sharding_key search
    |                         read-after-write path
    |
    +---- durable index outbox
             |
             v
       async global-index worker
             |
             v
       global record index
       tenant-wide discovery
       eventually consistent
```

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

```text
append / lifecycle / record mutation
        |
        v
 durable runtime mutation
        |
        +--> session replay stream
        |    ordered by session seq
        |
        +--> watch/changefeed cursor
             tenant scope
             agent scope
             session scope
             dashboard / supervisor / evaluator consumers
```

The current subscribe endpoint is deterministic replay-style SSE. Long-held
live push can be layered onto the same watch cursor model.

## Observability Export

The export surface is separate from the hot append path. Runtime mutations first
commit to AgentDB; durable export envelopes are then available to external
sinks through backfill/watch APIs. Named managed consumers store claim leases,
last acked cursors, include filters, and lag status. Connectors must write the
sink batch before acking AgentDB, so a crashed worker can replay the last
unacked cursor range.

Persistent v2 export cursors carry a per-tenant sequence assigned at the
serialized KV commit boundary in a single-tablet runtime. This orders cursor
delivery by commit rather than by wall-clock time. Page `next_cursor`
identifies the last delivered row; request results stop at the captured
`high_watermark`, while metadata pagination retains one first-page
`snapshot_high_watermark`. Managed ack/rewind verifies that a cursor is an
actual retained row for the same tenant, scope, and include filter; metadata
snapshot acks normalize to the captured ledger high watermark. Retained legacy
timestamp-based cursors remain readable during migration. Snapshot-routed
clusters reject export endpoints until a cross-tablet frontier is implemented.

```text
runtime mutation commits
        |
        v
 durable export envelope
        |
        +--> exports/backfill for snapshots and historical event ranges
        |
        +--> exports/watch for tailing committed changes
                 |
                 v
          managed consumer lease
          last_acked_cursor
                 |
                 v
      agentdbctl export connectors
          |          |             |
          v          v             v
   ClickHouse   ClickStack      JSONL / Parquet
   native rows  OTel views      archives / lakehouse
```

`agentdbctl export clickhouse migrate --apply` installs the current
ClickHouse schema stamp, native raw/event/tombstone tables, OTel-shaped
projection tables, and ClickStack-compatible views. `run --mode native,otel`
copies export envelopes with retry-safe ClickHouse deduplication tokens.
`doctor`, `status`, `queries`, and `clickstack-config` provide operational
inspection and setup templates.

`agentdbctl export jsonl run` writes the same export envelopes to local files,
stdout, or object-store URIs. `agentdbctl export parquet from-jsonl` converts
JSONL archives through DuckDB for warehouse and lakehouse ingestion. Both
ClickHouse and JSONL exporters support connector-side object/event filters,
field redaction, payload hash/drop modes, and a local status JSON file refreshed
after each processed batch.

## Storage

The runtime persists metadata through `KvStore`. The supported backend is
`RocksKvStore` for local-disk runtime metadata. Persistent single-node RocksDB
mode stores runtime metadata under `<data-dir>/runtime`.

An object-store-backed `KvStore` (SlateDB) was evaluated and retired. The
evaluation confirmed that the `KvStore` abstraction can run over object
storage, but object storage is not a replication mechanism and does not replace
a multi-node consensus protocol. The production direction for clustered hot
writes is a replicated local log/tablet path that commits after quorum, with
object-store upload happening asynchronously for sealed history, artifacts, and
archives.

Cluster routing distinguishes virtual shards from physical tablets. A shard is
a stable routing slot; it has no Raft group, LSM, snapshot, lease, or independent
lifecycle. A tablet is the physical serving and placement unit that owns one or
more shard ranges. When no session locator exists yet, session-scoped routing
selects the shard from `hash(tenant_id, session_id)`, not from `tenant_id` alone;
record/search routing should use `hash(tenant_id, sharding_key)` when a
`sharding_key` is present. This keeps split/merge at the tablet layer while
allowing a single tenant to spread across tablets.

```text
tenant or shared pool
  -> virtual shards: stable routing slots
  -> tablet map: shard range -> tablet
  -> tablet: Raft group + local LSM + local indexes + warm state
  -> object storage: sealed segments + archives + artifacts + snapshots
```

Artifact content is separated from metadata through a `ContentBackend`. The
server now supports:

- `LocalFsContentBackend` for local-disk blobs
- `ObjectStoreContentBackend` for S3-compatible or other `object_store`
  backends

Local mode stores content under the configured `content_dir`; if no content
directory is configured, it uses `<data-dir>/content`. Object-store mode stores
artifacts, sealed cold segments, and archive bundles under the fixed prefix
`agentdb/content`.

The hot session append path should not synchronously upload event history to
object storage. Appends target the warm runtime store; lifecycle workers later
seal cold segments or archive bundles and write those immutable objects through
the content backend. Large artifact bytes are already out-of-line through
`ContentBackend`; the current object-store content backend performs a
synchronous object PUT for artifact uploads, which should remain separate from
ordinary event append.

Local filesystem writes fsync the content file before rename and then fsync the
directory chain. If metadata persistence fails after a content write, the
runtime deletes the blob. Persistent standalone startup also lists local
content before opening the listener and deletes blobs not referenced by
metadata that still retains raw content. The object-store backend does not
currently implement object listing, so the startup orphan sweep is unsupported
there and remains an explicit operational boundary. The local sweep is a full
O(number of content objects) namespace scan, removes crash-left `blob.tmp.*`
files, and materializes at most 500,000 entries by default before failing
closed. `AGENTDB_STARTUP_MAX_CONTENT_OBJECTS` changes that explicit bound.
Artifact policies default to 16 MiB per object and clamp at the 128 MiB
absolute request/runtime limit.

The storage crate still contains lower-level RocksDB primitives and KV
contract tests because the runtime reuses those foundations.

The runtime code itself is `KvStore`-agnostic; tiered session metadata works
with any conforming `KvStore` implementation. The `agentdb-server` binary
selects the content backend from `agentdb.toml`.

The current runtime still hydrates persisted metadata and in-memory record
indexes during startup. A preflight applies these defaults before hydration:

| Environment variable | Default | Prefixes limited by that value |
|---|---:|---|
| `AGENTDB_STARTUP_MAX_HYDRATED_BYTES` | 512 MiB (536,870,912 bytes) | Total serialized keys and values across all prefixes below |
| `AGENTDB_STARTUP_MAX_TENANTS` | 10,000 | Tenants; tenant aliases |
| `AGENTDB_STARTUP_MAX_AGENTS` | 100,000 | Agents; agent aliases |
| `AGENTDB_STARTUP_MAX_SESSIONS` | 250,000 | Sessions; session aliases |
| `AGENTDB_STARTUP_MAX_ARTIFACTS` | 250,000 | Artifact metadata |
| `AGENTDB_STARTUP_MAX_RECORDS` | 100,000 | Records; record-index outbox rows; record commit rows |
| `AGENTDB_STARTUP_MAX_RECORD_EDGES` | 500,000 | Record edges |
| `AGENTDB_STARTUP_MAX_SKILLS` | 100,000 | Skills; unique skill keys |
| `AGENTDB_STARTUP_MAX_SKILL_BINDINGS` | 500,000 | Agent/skill bindings |
| `AGENTDB_STARTUP_MAX_EXPORT_CONSUMERS` | 10,000 | Managed export consumers |
| `AGENTDB_STARTUP_MAX_TENANT_META` | 500,000 | Tenant metadata rows |
| `AGENTDB_STARTUP_MAX_IDEMPOTENCY` | 1,000,000 | Idempotency rows |
| `AGENTDB_STARTUP_MAX_CONTENT_OBJECTS` | 500,000 | Local content entries listed for orphan reconciliation; separate from the serialized hydration-byte total |

Where a setting covers multiple prefixes, the row cap is applied separately to
each named prefix. Exceeding a row or aggregate-byte bound fails startup with
an actionable `AGENTDB_STARTUP_MAX_*` message rather than proceeding to
unbounded hydration. The 512 MiB limit is serialized KV size, not a process-RSS
ceiling: decoded objects, maps, and record indexes require additional memory.
The preflight is O(N), then normal hydration scans the same bounded state again.
Removing whole-state hydration remains future scale work.

## Session Tiered Storage

Session history now has three physical forms:

- `Warm`: appendable head in the runtime event store
- `Cold`: immutable sealed session segments in the content backend
- `Archive`: one compact archive bundle in the content backend plus a manifest
  in session metadata

Reads stay logical and session-centric: the runtime merges archive bundle,
sealed segments, and warm-head events into one ordered event stream.

```mermaid
flowchart TD
    A["append / append-and-fetch"] --> B["Warm session head<br/>KvStore metadata + hot event rows"]

    B -->|seal to cold| C["Session row<br/>lifecycle = cold<br/>warm_head_from_seq = next_seq<br/>sealed_segments = [seg_...]"]
    C --> D["ContentBackend segment blob<br/>seg_....json.enc<br/>full-fidelity events for one seq range"]

    C -->|compact to archive| E["Session row<br/>lifecycle = archive<br/>archive_manifest = seg_...<br/>sealed_segments = []"]
    E --> F["ContentBackend archive blob<br/>seg_....archive.json.enc<br/>compact full-history archive bundle"]

    E -->|restore| G["Session row<br/>lifecycle = cold or warm<br/>archive_manifest = null<br/>sealed_segments = [seg_...]"]
    G --> H["ContentBackend segment blob<br/>restored sealed history"]
```

Operationally:

- `transition_lifecycle(cold)` seals the current warm head into an immutable
  segment and removes hot event rows for that range.
- `transition_lifecycle(archive)` compacts full session history into one
  archive bundle and clears sealed-segment references.
- `restore(target=cold|warm)` materializes archived history back into a sealed
  segment so the session is no longer archive-backed.
- Appending to an archived or cold session reopens a warm head without
  rewriting old history.

This logic is implemented in `agentdb-core`; the API and SDKs expose it through
session lifecycle, storage, and restore endpoints.

## Deployment Shapes

- **In-memory dev/test**: throwaway runtime state for smoke tests and quick
  local experiments.
- **Persistent single-node**: RocksDB metadata plus local filesystem artifact
  content, configured by `agentdb.toml`.
- **Docker and Compose**: containerized single-node server with a mounted config
  file and data volume. The image contains both `agentdb` and `agentdbctl`.
- **Observability export workers**: `agentdbctl` processes that tail managed
  export consumers into ClickHouse/ClickStack or JSONL/Parquet archives. They
  can run as systemd services or AWS deployment helpers and fail over through
  consumer lease expiry.
- **Helm**: evaluation-oriented Kubernetes deployment that mounts config through
  a ConfigMap and uses the same health endpoints as the binary.

Tagged release archives likewise contain both operator binaries; `agentdbctl`
is therefore available for backup, restore, export, and diagnostic workflows
without a source checkout.

Distributed control-plane behavior is not the current user-facing deployment
target. The priority is making one node easy to start, inspect, back up, upgrade,
and maintain.

## Policy And GC

Policy evaluation covers append, artifact writes, record writes, skill bindings,
context byte budgets, active-session quotas, lifecycle work units, and GC work
units. Soft deletes mark runtime objects first; tenant-scoped GC sweeps later
purge metadata, index entries, and artifact content when policy allows.

## SDKs And Adapters

The Python and TypeScript SDKs map directly to the native API. They send an
idempotency key only when the caller supplies one, keeping default same-session
appends eligible for server-side coalescing; applications that retry ambiguous
mutations should provide a stable key. SDKs expose typed ID aliases as opaque
strings.

Their friendly surfaces keep tenant IDs out of normal application flows. In
authenticated deployments, SDKs bind the workspace/tenant from `/api/me`-style
user-info metadata; local development can use explicit workspace aliases. The
Python client falls back by listing an existing external key before creation,
and both friendly surfaces reuse agent/session aliases after restart rather
than treating a create conflict as a new identity. After binding, SDKs reuse or
create aliases under the resolved tenant.

Framework adapters should be thin. Their job is to map framework concepts onto
AgentDB tenants, agents, sessions, events, records, artifacts, and skills while
leaving runtime semantics on the server.
