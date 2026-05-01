# AgentDB

AgentDB is an agent-native runtime database. It stores tenant-scoped agents,
sessions, ordered events, artifacts, searchable records, skills, policy state,
fork lineage, replay streams, and context packs behind one native HTTP API.

## Current Status

This repository is a pre-release prototype. The legacy Mongo wire facade and Go
SDK are no longer part of the workspace. The supported surfaces are:

- Rust runtime/API crates in `crates/agentdb-core` and `crates/agentdb-api`
- Python SDK in `clients/python`
- TypeScript SDK in `clients/typescript`
- Framework adapters and templates in `adapters/`
- Session dashboard mounted at `/dashboard`

## Run Locally

```bash
cargo run -p agentdb-server -- serve --dev
```

This starts an in-memory dev server at:

```text
http://127.0.0.1:7878
```

Persistent single-node state uses RocksDB metadata and filesystem artifact
content:

```bash
cargo run -p agentdb-server -- init-config
cargo run -p agentdb-server -- doctor
cargo run -p agentdb-server -- serve --config agentdb.toml
```

The readiness probe is:

```bash
curl -i http://127.0.0.1:7878/health/ready
```

The session dashboard is available at:

```text
http://127.0.0.1:7878/dashboard/
```

For install, Docker, backup, and maintenance notes, see
[`docs/SINGLE_NODE.md`](docs/SINGLE_NODE.md).

## API Shape

Core routes are tenant-scoped:

```text
POST /api/tenants
GET  /api/tenants/{tenant_id}
POST /api/tenants/{tenant_id}/agents
POST /api/tenants/{tenant_id}/agents/{agent_id}/sessions
POST /api/tenants/{tenant_id}/sessions/{session_id}/events
GET  /api/tenants/{tenant_id}/sessions/{session_id}/events
POST /api/tenants/{tenant_id}/sessions/{session_id}/append-and-fetch
GET  /api/tenants/{tenant_id}/sessions/{session_id}/replay
POST /api/tenants/{tenant_id}/sessions/{session_id}/fork
GET  /api/tenants/{tenant_id}/sessions/{session_id}/subscribe
POST /api/tenants/{tenant_id}/sessions/{session_id}/context
GET  /api/tenants/{tenant_id}/watch
POST /api/tenants/{tenant_id}/artifacts
POST /api/tenants/{tenant_id}/records
POST /api/tenants/{tenant_id}/records/search
POST /api/tenants/{tenant_id}/skills
POST /api/tenants/{tenant_id}/agents/{agent_id}/skills/bind
```

IDs are opaque typed strings such as `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`.

## Python SDK

```bash
pip install -e clients/python
```

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
workspace = db.workspace("acme")
agent = workspace.agent("sales-agent")
session = agent.session("support-ticket-42")

result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
print(result.seq, [event.payload for event in result.recent])
```

Async support is available with:

```bash
pip install -e 'clients/python[async]'
```

## TypeScript SDK

```bash
cd clients/typescript
npm install
npm run build
```

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878" });
const workspace = await db.workspace("acme");
const agent = await workspace.agent("sales-agent");
const session = await agent.session("support-ticket-42");

const result = await session.append(
  { role: "user", text: "hello" },
  { fetchLastN: 5 }
);
console.log(result.seq, result.recent.map((event) => event.payload));
```

## Workspace Layout

```text
bin/agentdb-server           server binary
crates/agentdb-core          runtime model and tenant/session/event semantics
crates/agentdb-api           native HTTP routes
crates/agentdb-storage       RocksDB/SlateDB KV and artifact content backends
crates/agentdb-index         text/vector indexing helpers
crates/agentdb-meta          control-plane metadata and routing planner
crates/agentdb-dashboard     session-first dashboard assets
adapters/langgraph-checkpoint-agentdb
                              LangGraph checkpoint/store adapter
adapters/openai-agents-agentdb
                              OpenAI Agents SDK session adapter
adapters/mcp-agentdb          MCP stdio server for AgentDB
adapters/vercel-ai-sdk-agentdb-template
                              Vercel AI SDK persistence template
clients/python               Python SDK
clients/typescript           TypeScript SDK
```

## Validation

```bash
cargo build --workspace
cargo test -p agentdb-core -p agentdb-api
cargo test -p agentdb-meta routing
cargo test -p agentdb-api routing_http
cargo test -p agentdb-api
cargo test -p agentdb-server
python3 -m pytest -q clients/python/tests
PYTHONPATH=adapters/langgraph-checkpoint-agentdb python3 -m pytest -q adapters/langgraph-checkpoint-agentdb/tests
PYTHONPATH=clients/python:adapters/openai-agents-agentdb python3 -m pytest -q adapters/openai-agents-agentdb/tests
PYTHONPATH=clients/python:adapters/mcp-agentdb python3 -m pytest -q adapters/mcp-agentdb/tests
(cd adapters/vercel-ai-sdk-agentdb-template && npm test && npm run check)
(cd clients/typescript && npm test && npm run build)
```

## Reading Order

- `docs/SINGLE_NODE.md`
- `docs/ARCHITECTURE.md`
- `docs/DESIGN.md`
- `docs/agent-db.md`
- `docs/session-event-conventions.md`
- `docs/adapter-readiness-checklist.md`
- `docs/ROADMAP.md`
