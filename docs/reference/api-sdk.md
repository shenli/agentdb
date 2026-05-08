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
```

Artifacts:

```text
POST /api/tenants/{tenant_id}/artifacts
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}/blob
```

Records:

```text
POST   /api/tenants/{tenant_id}/records
GET    /api/tenants/{tenant_id}/records/{record_id}
DELETE /api/tenants/{tenant_id}/records/{record_id}
GET    /api/tenants/{tenant_id}/records/search
POST   /api/tenants/{tenant_id}/records/search
```

`POST /records` returns `X-AgentDB-Record-Commit-Seq`. Record search accepts
`sharding_key` and `min_visible_record_commit_seq` in both GET query and POST
JSON forms. With `sharding_key`, search uses the owner-local LSI-like index.
Without it, search uses the GSI-like global index, which is eventually
consistent. If `min_visible_record_commit_seq` is ahead of the global index
watermark, the runtime falls back to committed base records to preserve
same-client read-after-write visibility. `GET /api/admin/indexes/records`
returns local/global index watermarks, lag tokens, repair counters, and async
apply failures.

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
