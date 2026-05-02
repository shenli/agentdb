# API and SDK Reference

This page is the compact reference for AgentDB's native HTTP API and local SDK
surfaces. The API is tenant-scoped and uses opaque typed IDs such as `ten_*`,
`agt_*`, `ses_*`, `evt_*`, `art_*`, `rec_*`, and `skl_*`.

## HTTP API

Health and admin:

```text
GET  /health/ready
GET  /api/admin/routing
GET  /api/admin/routing/decision?tenant_id={tenant_id}[&session_id={session_id}]
POST /api/policy/worker/run
```

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
  workspace(external_key, *, tenant_id=None, policy=None)
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
create_tenant(external_key, policy=None)
get_tenant(tenant_id)
create_agent(tenant_id, alias=None, config=None)
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
const workspace = await db.workspace("acme");
const agent = await workspace.agent("sales-agent");
const session = await agent.session("support-ticket-42");

await session.appendUser("hello");
const events = await session.replay({ limit: 100 });
```

Primary classes:

```text
AgentDB
  healthReady()
  workspace(externalKey, opts?)

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
createTenant(externalKey, policy?)
createAgent(tenantId, opts?)
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
