# API and SDK Reference

This page is the compact reference for AgentDB's native HTTP API and local SDK
surfaces. The API is tenant-scoped and uses opaque typed IDs such as `ten_*`,
`agt_*`, `ses_*`, `evt_*`, `art_*`, `rec_*`, and `skl_*`.

## HTTP API

Humans should usually use `agentdbctl` for operations. The HTTP API below is
the stable backend contract for SDKs, automation, and the CLI.

AgentDB also has an opt-in MongoDB wire-protocol compatibility listener for a
narrow driver/tool subset. Enable it with `agentdb serve --mongo-bind
127.0.0.1:7879` or `[mongodb].bind` in config. Optional MongoDB `PLAIN`
username/password auth is available with `--mongo-username` and
`--mongo-password`. The native API remains canonical for agent runtime
operations. See
[mongodb-compatibility.md](mongodb-compatibility.md) for a tutorial, mapping
rules, limitations, and AI coding-agent guidance.

## MongoDB Compatibility Subset

The MongoDB listener is for driver/tool access to AgentDB runtime collections.
It is not full MongoDB compatibility.

Supported initial commands:

```text
hello / isMaster
saslStart / saslContinue   PLAIN auth only, when configured
ping
buildInfo
endSessions
listDatabases
listCollections
insert
find
count
aggregate                 limited $match + $count support
createIndexes             accepted as a no-op compatibility command
agentdbAppendAndFetch     AgentDB extension command
agentdbFork               AgentDB extension command
agentdbReplay             AgentDB extension command
agentdbContext            AgentDB extension command
agentdbTransitionLifecycle AgentDB extension command
agentdbRestoreSession     AgentDB extension command
```

Supported collections:

```text
sessions
events
records
artifacts     metadata find only
skills        insert and find
```

Current write mapping:

```text
db.events.insertOne(...)    -> Runtime::append(...)
db.sessions.insertOne(...)  -> Runtime::create_session(...)
db.records.insertOne(...)   -> Runtime::write_record(...)
db.skills.insertOne(...)    -> Runtime::register_skill(...)
db.artifacts.find(...)      -> Runtime::list_artifacts(...)
```

Unsupported MongoDB commands return a Mongo-shaped error document with `ok: 0`
and `codeName: "CommandNotFound"` rather than falling through to a separate
storage path.

Example:

```javascript
db.events.insertOne({
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "user",
    content: "hello"
  }
})

db.events.find({ kind: "message" })
db.sessions.find({ alias: "support-ticket-42" })

db.runCommand({
  agentdbAppendAndFetch: "events",
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "assistant",
    content: "hi"
  },
  fetch_last_n: 20
})

db.runCommand({
  agentdbFork: "sessions",
  session_id: "ses_...",
  at_seq: 42,
  alias: "experiment-branch"
})

db.runCommand({
  agentdbReplay: "sessions",
  session_id: "ses_..."
})

db.runCommand({
  agentdbContext: "sessions",
  session_id: "ses_...",
  include: ["events", "records", "skills", "policy"],
  max_events: 20,
  max_bytes: 262144
})
```

If an event insert omits `session_id`, the compatibility layer creates or
reuses a session under the provided `session_alias` or the default
`mongo-session` alias. If `agent_id` is omitted, it creates or reuses the
tenant-local `mongo-default` agent.

Streaming watch/subscribe, artifact blob transfer, and admin operations remain
native API first. They do not fit the request/response Mongo command shape well.

Health and admin:

```text
GET  /health/ready
GET  /api/admin/cluster/health[?stale_threshold_ms={ms}]
GET  /api/admin/cluster/readiness[?stale_threshold_ms={ms}]
GET  /api/admin/scheduler
GET  /api/admin/drain
POST /api/admin/drain
GET  /api/admin/cells
POST /api/admin/cells
GET  /api/admin/nodes[?stale_threshold_ms={ms}]
POST /api/admin/nodes
DELETE /api/admin/nodes/{node_id}
POST /api/admin/nodes/{node_id}/maintenance
GET  /api/admin/tablets
GET  /api/admin/tablet-stats[?tablet_id={tablet_id}]
POST /api/admin/tablets/create
POST /api/admin/tablets/rebalance
POST /api/admin/tablets/{tablet_id}/assign
POST /api/admin/tablets/{tablet_id}/relinquish
POST /api/admin/tablets/{tablet_id}/handoff
POST /api/admin/tablets/{tablet_id}/handoff/complete
POST /api/admin/tablets/{tablet_id}/handoff/cancel
GET  /api/admin/routing
GET  /api/admin/routing/decision?tenant_id={tenant_id}[&session_id={session_id}]
GET  /api/admin/indexes/records
POST /api/admin/backups
POST /api/admin/restores
POST /api/policy/worker/run
```

Node maintenance body:

```json
POST /api/admin/nodes/{node_id}/maintenance
{ "state": "draining" }
```

Allowed states are `alive`, `draining`, and `maintenance`. Setting the local
node back to `alive` clears the local drain flag; setting it to `draining` or
`maintenance` refuses new local mutations.

Tablet admin request bodies:

```json
GET /api/admin/tablet-stats?tablet_id=7
{
  "stats": [
    {
      "tablet_id": 7,
      "node_id": "node-a",
      "key_count": 1000,
      "key_bytes": 64000,
      "value_bytes": 1048576,
      "storage_bytes": 1112576,
      "applied_index": 42,
      "sample_interval_ms": 1000,
      "write_ops_delta": 12,
      "write_ops_per_sec_milli": 12000,
      "compaction_debt_ops": 500,
      "updated_at_ms": 1713900000000
    }
  ],
  "aggregates": [
    {
      "tablet_id": 7,
      "replica_count": 1,
      "key_count": 1000,
      "key_bytes": 64000,
      "value_bytes": 1048576,
      "storage_bytes": 1112576,
      "applied_index": 42,
      "sample_interval_ms": 1000,
      "write_ops_delta": 12,
      "write_ops_per_sec_milli": 12000,
      "compaction_debt_ops": 500,
      "updated_at_ms": 1713900000000
    }
  ]
}

POST /api/admin/tablets/create
{ "tablet_id": 0 }

POST /api/admin/tablets/{tablet_id}/assign
{ "node_id": "node-a" }

POST /api/admin/tablets/{tablet_id}/handoff
{ "target_node_id": "node-b" }

POST /api/admin/tablets/{tablet_id}/handoff/complete
{ "expected_source_node_id": "node-a" }

POST /api/admin/tablets/rebalance
{
  "dry_run": true,
  "stale_threshold_ms": 15000,
  "max_movements": 10
}
```

`assign` only pre-claims an unowned tablet for a fresh `alive` node. `handoff`
sets a cooperative movement target and keeps the source owner active until the
source completes the handoff. `rebalance` assigns unowned tablets, moves tablets
off fresh draining nodes through handoff, and balances load across fresh `alive`
nodes; it does not force-move tablets from stale or dead owners.

Backup/restore admin request bodies:

```json
POST /api/admin/backups
{
  "backup_id": "nightly-2026-05-04",
  "directory": "/var/lib/agentdb/backups",
  "include_meta": true,
  "include_runtime_summary": true
}

POST /api/admin/restores
{
  "manifest_path": "/var/lib/agentdb/backups/nightly-2026-05-04/manifest.json",
  "dry_run": true
}
```

The backup endpoint writes an online manifest and, when metadata is configured,
a metadata snapshot. Runtime summary data is observability metadata, not a full
copy of runtime KV/content data. The restore endpoint validates manifests only;
destructive restore apply remains an offline operator procedure.

Tenants, agents, and sessions:

```text
POST   /api/tenants
GET    /api/tenants/{tenant_id}
DELETE /api/tenants/{tenant_id}
POST   /api/tenants/{tenant_id}/agents
GET    /api/tenants/{tenant_id}/agents/{agent_id}
DELETE /api/tenants/{tenant_id}/agents/{agent_id}
POST   /api/tenants/{tenant_id}/agents/{agent_id}/sessions
GET    /api/tenants/{tenant_id}/sessions/{session_id}
DELETE /api/tenants/{tenant_id}/sessions/{session_id}
```

Events, replay, fork, subscribe, and context:

```text
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
GET  /api/admin/exports
```

`replay` returns the stitched logical session history plus scan stats:
`fork_depth`, `segments_scanned`, `events_scanned`, and
`materialized_prefix`. Fork ancestor reads are bounded to the fork point
(`at_seq + 1`) instead of scanning the full ancestor tail. The core runtime can
also materialize a forked session's non-leaf replay prefix so later replay reads
a compact prefix object plus the leaf session tail.

Sandbox filesystem snapshots are coordinated through AgentDB snapshot refs and
an external sandbox service. See
`docs/reference/sandbox-snapshot-contract.md` for trigger ownership, restore,
fork, retention, and dangling-ref behavior.

`exports/watch` returns stable export envelopes for observability sinks.
Persistent runtimes return durable ledger cursors (`ledger:...`) for events and
metadata object mutations; in-memory/dev runtimes fall back to current event
watch cursors (`frontier:v1:...`). Runtime `watch` accepts both frontier cursors
and older `v2:` item cursors for resume compatibility. Use the optional
comma-separated `include` query parameter, for example
`include=events,records,skills`, to filter object types:

```json
{
  "items": [
    {
      "export_version": "2026-05-14",
      "cursor": "ledger:ten_...",
      "op": "insert",
      "object_type": "event",
      "tenant_id": "ten_...",
      "agent_id": "agt_...",
      "session_id": "ses_...",
      "object_id": "evt_...",
      "seq": 0,
      "created_at_ms": 1778780000000,
      "observed_at_ms": 1778780000001,
      "event_id": "evt_...",
      "kind": "model_call",
      "model": "gpt-...",
      "duration_ms": 123,
      "tombstone": null,
      "data": {}
    }
  ],
  "next_cursor": "ledger:ten_...",
  "high_watermark": "ledger:ten_..."
}
```

Delete envelopes use `op = "delete"` and include `tombstone.reason`.
Current reasons are `soft_delete`, `retention_gc`, and `tenant_purge`.

Use `agentdbctl export clickhouse ...` for the first-party ClickHouse path.
`migrate --apply` creates native raw/event/tombstone tables plus optional
OTel-shaped `agentdb_otel_logs` and `agentdb_otel_traces` projection tables,
with ClickStack-compatible `agentdb_clickstack_logs` and
`agentdb_clickstack_traces` views.
Use `run --mode native,otel --backfill --tail` to write both native AgentDB
tables and ClickStack-friendly OTel projections. `doctor` and `status` validate
the ClickHouse schema stamp, required tables/views, query grants, and optional
write-smoke behavior.

Use `agentdbctl export jsonl run --output ./events.jsonl` or an object-store URI
such as `s3://bucket/prefix/events.jsonl` for raw JSONL archives. The JSONL
command writes the same export envelopes returned by `exports/watch` and
supports managed consumer claim/ack options.
ClickHouse and JSONL runs also support exporter-side minimization flags:
`--allow-object-type`, `--deny-object-type`, `--allow-event-kind`,
`--deny-event-kind`, `--redact-field`, `--payload-mode full|hash|drop`, and
`--metrics-output`. These controls run in the connector before external sink
writes and the metrics file is refreshed after each processed batch.
Use `agentdbctl export parquet from-jsonl` to convert those archives to Parquet
through DuckDB for warehouse and lakehouse ingestion.
`exports/backfill` supports event snapshots plus tenant-scope metadata
snapshots:

```json
{
  "scope": "tenant",
  "include": ["events"],
  "from_cursor": "ledger:ten_...",
  "limit": 1000
}
```

Metadata snapshot includes use `snapshot:...` cursors and currently support
`["tenants", "agents", "sessions", "artifacts", "records", "skills",
"skill_bindings"]`. Do not mix `events` with metadata includes in one backfill
request; run metadata backfill first, then event backfill/tail.

Managed export consumers provide durable checkpoints and leases. Official
ClickHouse event exporters usually use `include=["events"]`; generic consumers
can include any supported object types:

```json
{
  "name": "prod-clickhouse",
  "scope": "tenant",
  "include": ["events"]
}
```

Use `claim` with `lease_owner` and `lease_ms`, `ack` with the durable sink
cursor after the sink accepts a batch, and `rewind` to replay from an older
cursor or from the beginning. `GET /exports/consumers/{name}` and
`GET /api/admin/exports` include derived status fields such as
`pending_items`, `high_watermark`, `pending_oldest_created_at_ms`, `lag_ms`, and
`lease_active`. The admin response also includes an aggregate `metrics` object
with total pending export items, max consumer lag, oldest pending timestamp,
lease counts, and per-tenant oldest retained cursor/high-watermark entries.

For high availability, run at least two connector processes with the same
consumer name and different `lease_owner` values. Only the active lease holder
can ack. If it dies, another process claims the lease after expiry and resumes
from the last durable acked cursor. To avoid data loss, connectors must insert
into the sink before acking AgentDB.

Artifacts:

```text
POST /api/tenants/{tenant_id}/artifacts
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}/blob
```

Records:

```text
POST   /api/tenants/{tenant_id}/records
POST   /api/tenants/{tenant_id}/records/upsert
GET    /api/tenants/{tenant_id}/records/{record_id}
PATCH  /api/tenants/{tenant_id}/records/{record_id}
POST   /api/tenants/{tenant_id}/records/{record_id}/touch
DELETE /api/tenants/{tenant_id}/records/{record_id}
GET    /api/tenants/{tenant_id}/records/search
POST   /api/tenants/{tenant_id}/records/search
POST   /api/tenants/{tenant_id}/records/edges
GET    /api/tenants/{tenant_id}/records/{record_id}/edges
DELETE /api/tenants/{tenant_id}/records/edges/{edge_id}
GET    /api/tenants/{tenant_id}/meta/{key}
PUT    /api/tenants/{tenant_id}/meta/{key}
```

`POST /records`, `POST /records/upsert`, and `PATCH /records/{record_id}`
return `X-AgentDB-Record-Commit-Seq`. A record body may include
`external_id`; it is unique per tenant. Repeating `POST /records` with the same
`external_id` is idempotent and returns the existing record. `POST
/records/upsert` updates the existing row for that `external_id` or creates a
new row when none exists.

Records have logical `status` (`active` or `superseded`), monotonic `version`,
`access_count`, and `updated_at_ms`. `PATCH /records/{record_id}` accepts
`expected_version` for optimistic compare-and-set updates and returns
`VERSION_CONFLICT` with HTTP 409 when the current version differs. `POST
/records/{record_id}/touch` increments `access_count` without bumping
`version`.

Record search returns `matches`, not bare records. Each match contains
`record`, `score`, `rank`, and per-mode `components` such as `text` and
`vector`. Scores are query-local ranking metadata; do not store them as durable
business values.

Record search accepts `sharding_key` and `min_visible_record_commit_seq` in both
GET query and POST JSON forms. POST search also accepts `metadata_filter`, a
small scalar predicate language for record metadata:

```json
{
  "namespace": ["db0", "memory"],
  "text": "mongo regression",
  "metadata_filter": {
    "all": [
      { "path": "repo", "op": "eq", "value": "agentdb" },
      { "path": "component", "op": "eq", "value": "wire" },
      { "path": "confidence", "op": "gte", "value": 0.8 }
    ],
    "none": [
      { "path": "status", "op": "eq", "value": "stale" }
    ]
  }
}
```

Supported metadata operators are `eq`, `neq`, `in`, `exists`, `gte`, and `lte`.
`path` is a dotted object path into the metadata JSON object. Search also
accepts `include_superseded`, `min_score`, `created_after_ms`,
`created_before_ms`, and `touch`. Superseded records are excluded by default;
set `include_superseded=true` to retrieve memory history. With `sharding_key`,
search uses the owner-local LSI-like index. Without it, search uses the
GSI-like global index, which is eventually consistent. If
`min_visible_record_commit_seq` is ahead of the global index watermark, the
runtime falls back to committed base records to preserve same-client
read-after-write visibility. `GET /api/admin/indexes/records` returns
local/global index watermarks, lag tokens, repair counters, and async apply
failures.

Record edges connect two records with an `edge_type` of `related`, `derived`,
`contradicts`, `supports`, or `supersedes` plus optional metadata. Tenant meta
is a small JSON KV surface for adapter cursors and runtime-local configuration,
not a replacement for application tables.

Skills and policy:

```text
POST   /api/tenants/{tenant_id}/skills
GET    /api/tenants/{tenant_id}/skills/{skill_id}
DELETE /api/tenants/{tenant_id}/skills/{skill_id}
GET    /api/tenants/{tenant_id}/agents/{agent_id}/skills
POST   /api/tenants/{tenant_id}/agents/{agent_id}/skills/bind
POST   /api/tenants/{tenant_id}/policy/lifecycle-sweep
POST   /api/tenants/{tenant_id}/policy/gc-sweep
```

Tenant and agent policy may include `secret_handling`. Setting
`secret_handling.deny_raw_secret_fields` to `true` makes event append reject JSON
payload keys shaped like raw credential fields, including `api_key`, `password`,
`token`, `access_token`, `authorization`, `client_secret`, `refresh_token`,
`secret_access_key`, and `private_key`. Use `credential_refs` for opaque vault /
broker references instead of putting credential values in `payload`.

Common headers:

```text
Idempotency-Key          Optional mutation idempotency key.
X-Sha256                 Optional artifact content checksum.
X-AgentDB-Backpressure   Response header for backpressure signals.
Authorization            Bearer token or Basic auth, depending on server config.
```

## Python SDK

Install:

```bash
pip install -e clients/python
pip install -e 'clients/python[async]'
```

Session-first surface:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
workspace = db.workspace("acme")
agent = workspace.agent("sales-agent")
session = agent.session("support-ticket-42")

session.append_user("hello")
events = session.replay(limit=100)
```

Primary sync classes:

```text
AgentDB
  health_ready()
  workspace(external_key=None, *, tenant_id=None, policy=None)
  session(tenant_id, session_id=None, *, agent_id=None)
  session_by_id(tenant_id, session_id, *, agent_id=None)

Workspace
  id
  agent(alias=None, *, agent_id=None, config=None)
  agent_by_id(agent_id, *, alias=None)
  session(alias=None, *, agent_id=None, session_id=None)
  session_by_id(session_id, *, agent_id=None)
  watch(scope="tenant", agent_id=None, from_cursor=None, limit=100)
  artifacts.put/get/blob
  records.write/search
  skills.register

AgentHandle
  id
  session(alias=None, *, session_id=None)
  session_by_id(session_id)
  bind_skill(skill_id, *, idempotency_key=None)

SessionHandle / Session
  append(payload, kind="message", idempotency_key=None, fetch_last_n=0, ...)
  append_and_fetch(payload, fetch_last_n=10, ...)
  append_user(text, ...)
  model_message(text, ...)
  tool_call(...)
  tool_result(...)
  agent_status(status, ...)
  context_pack_built(...)
  fetch(from_seq=0, limit=100)
  fetch_recent(last_n=10)
  replay(from_seq=None, limit=100)
  fork(child_session_id=None, at_seq=0, alias=None)
  subscribe(from_seq=0, limit=100)
  context(max_events=None, max_bytes=None, include=None, record_query=None)
```

`append` and `append_and_fetch` accept `artifact_refs`, `source_refs`, and
`credential_refs`. Credential refs carry opaque `cred_ref` values resolved by a
vault or broker outside AgentDB; do not put credential values in `payload`. The
Python SDK rejects append payload keys shaped like raw credentials, such as
`api_key`, `access_token`, `password`, and `private_key`, before sending the
request.

Lower-level sync `Client` methods:

```text
health_ready()
user_info()
list_tenants()
create_tenant(external_key, policy=None)
get_tenant(tenant_id)
list_agents(tenant_id)
create_agent(tenant_id, alias=None, config=None)
list_sessions(tenant_id)
create_session(tenant_id, agent_id, alias=None)
session(tenant_id, session_id=None, *, agent_id=None)
put_artifact(tenant_id, data, content_type=None, sha256=None)
get_artifact(tenant_id, artifact_id)
get_artifact_blob(tenant_id, artifact_id)
write_record(tenant_id, **body)
search_records(tenant_id, **body)
register_skill(tenant_id, name, version, manifest=None, artifact_refs=None)
bind_skill(tenant_id, agent_id, skill_id, idempotency_key=None)
watch(tenant_id, scope="tenant", agent_id=None, from_cursor=None, limit=100)
```

Async support mirrors the lower-level client through `AsyncClient` and
`AsyncSession`.

Python clients remember the latest `X-AgentDB-Record-Commit-Seq` observed from
`write_record` per tenant and attach it as `min_visible_record_commit_seq` on
later `search_records` calls unless the caller supplied one explicitly.

## TypeScript SDK

Build from this workspace:

```bash
cd clients/typescript
npm install
npm run build
```

Session-first surface:

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878" });
const workspace = await db.workspace();
const agent = await workspace.agent("sales-agent");
const session = await agent.session("support-ticket-42");

await session.appendUser("hello");
const events = await session.replay({ limit: 100 });
```

In authenticated deployments, `workspace()` resolves the workspace/tenant from
user-info metadata. For local development against a server without `/api/me`,
pass a workspace alias, such as `workspace("acme-dev")`.

Primary classes:

```text
AgentDB
  healthReady()
  workspace(externalKey?, opts?)
  session(sessionId, opts?)
  sessionById(sessionId, { tenantId, agentId? })

Workspace
  tenantId
  agent(alias?, opts?)
  agentById(agentId, opts?)
  sessionById(sessionId, opts?)
  watch(opts?)
  artifacts.put/get/blob
  records.write/search
  skills.register

AgentHandle
  agentId
  session(alias?, opts?)
  sessionById(sessionId)
  bindSkill(skillId, opts?)

SessionHandle / Session
  append(payload, opts?)
  appendAndFetch(payload, opts?)
  appendUser(text, opts?)
  modelMessage(text, opts?)
  toolCall(...)
  toolResult(...)
  agentStatus(status, opts?)
  contextPackBuilt(...)
  fetch(opts?)
  fetchRecent(n)
  replay(opts?)
  fork(opts)
  subscribe(opts?)
  context(opts?)
```

`AppendOptions` supports `artifactRefs`, `sourceRefs`, and `credentialRefs`.
`credentialRefs` are refs only; real secrets stay in the control plane / vault.
The TypeScript SDK rejects append payload keys shaped like raw credentials, such
as `api_key`, `access_token`, `password`, and `private_key`, before sending the
request.

Lower-level `Client` methods:

```text
healthReady()
userInfo()
listTenants()
createTenant(externalKey, policy?)
listAgents(tenantId)
createAgent(tenantId, opts?)
listSessions(tenantId)
createSession(tenantId, agentId, opts?)
session(tenantId, sessionId?, opts?)
putArtifact(tenantId, data, opts?)
getArtifact(tenantId, artifactId)
getArtifactBlob(tenantId, artifactId)
writeRecord(tenantId, body)
searchRecords(tenantId, body)
registerSkill(tenantId, body)
bindSkill(tenantId, agentId, skillId, opts?)
watch(tenantId, opts?)
```

The TypeScript client mirrors Python's record visibility behavior: after
`writeRecord`, it stores the tenant's latest record commit sequence and adds
`min_visible_record_commit_seq` to later `searchRecords` calls unless the body
already includes that field.
