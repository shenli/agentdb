<div align="center">
  <h1>AgentDB</h1>
  <h3>Scalable, durable, cost-efficient runtime state for AI agents.</h3>
</div>

<div align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
  <a href="docs/README.md"><img src="https://img.shields.io/badge/docs-local-0f766e" alt="Docs"></a>
  <a href="docs/operations/DEPLOYMENT.md"><img src="https://img.shields.io/badge/deploy-self--hosted%20today%20%2B%20cluster%20target-7c3aed" alt="Deployment"></a>
</div>

<br>

AgentDB is a runtime database for AI agent platforms, built for the workload
shape of autonomous agents: many ordered session events, large tool/model
outputs, long-running tasks, sparse tenant traffic, and retained history. It
stores durable execution state outside the model context window so agents can
resume, replay, fork, inspect, and stream work without forcing every application
to rebuild session semantics on top of a generic database.

**What AgentDB gives you:**

- **Session-native runtime semantics** — tenant-scoped agents, sessions,
  ordered events, artifacts, records, skills, policy, replay, fork, subscribe,
  context, and lifecycle state
- **Multi-tenant runtime architecture** — tenant boundaries are built into
  routing, policy, quota, lifecycle, watch streams, storage layout, and future
  placement decisions instead of being bolted on in application code
- **DynamoDB-style record index behavior** — owner-local record searches with
  read-after-write semantics when a `sharding_key` is known, plus async
  eventually consistent global discovery when it is not
- **Object-store based storage engine** — scale retained history and keep costs
  predictable by separating hot session metadata from cold events, large
  artifacts, and derived state
- **Agent-runtime performance and cost efficiency** — optimized append,
  append-and-fetch, hot-tail reads, and bounded local storage paths reduce the
  infrastructure needed for common agent session workloads
- **Compatibility where it helps** — native HTTP/SDK APIs remain canonical,
  while the optional MongoDB listener lets existing drivers and tools inspect
  and write AgentDB runtime collections
- **Operate and debug agent runs** — dashboard, watch streams, structured
  errors, runbooks, health checks, and deployment docs make runtime state
  inspectable
- **SDK and adapter surfaces** — Python and TypeScript clients plus LangGraph,
  OpenAI Agents SDK, MCP, and Vercel AI SDK integration paths

In a typical AI agent architecture, AgentDB is the durable runtime state service
between the agent harness and its tools. The harness writes every model output,
tool call, tool result, artifact, policy decision, and lifecycle transition to
AgentDB, then reads it back to resume, replay, fork, inspect, or stream a run.

```text
                 AI agent platform

  tenant A / workspace A             tenant B / workspace B
  support agents, sessions           research agents, sessions
             |                                  |
             | append/read/replay/search/watch |
             v                                  v
      +--------------------------------------------------+
      |                    AgentDB                       |
      | multi-tenant runtime boundary                    |
      |                                                  |
      |  tenants -> agents -> sessions -> events         |
      |      |          |          |          |           |
      |      |          |          +--> artifacts         |
      |      |          |          +--> records           |
      |      |          |                 |               |
      |      |          |                 +--> local index |
      |      |          |                 |    sharding_key|
      |      |          |                 +--> global index|
      |      |          |                      async/GSI   |
      |      |          |                                  |
      |      +--> policy, quota, lifecycle, storage       |
      |      +--> watch/changefeed cursors                |
      +--------------------------------------------------+
             |                                  |
             v                                  v
   dashboards, supervisors,          tools, sandboxes, MCP,
   evaluators, replay viewers        application systems
```

This repository is a pre-release prototype. The supported developer surfaces
are:

- Rust runtime and HTTP API crates in `crates/`
- Server binary in `bin/agentdb-server`
- Python SDK in `clients/python`
- TypeScript SDK in `clients/typescript`
- Framework adapters and templates in `adapters/`
- Session dashboard mounted at `/dashboard`

## Performance Snapshot

AgentDB's performance goal is narrow and practical: make the common agent
runtime loop cheap to run on a single node, then scale out with the same session
semantics. The benchmark in this repo does not try to prove that AgentDB is a
better general-purpose database. It compares AgentDB with reasonable MongoDB
and Postgres event-log schemas for agent-shaped state.

The core workload is:

- Append the next session event.
- Read the recent tail needed for the next model/tool step.
- Occasionally write/search memory records, fork a session, or attach artifact
  metadata.

Current long-run result on this benchmark configuration:

- Host: Ubuntu 22.04, Linux 6.8, x86_64.
- CPU/RAM: Intel Core i7-14700KF, 20 cores / 28 hardware threads, 31 GiB RAM.
- Storage/runtime: local NVMe filesystem, AgentDB with RocksDB, MongoDB and
  Postgres in Docker.
- Workload profile: `single-node-wal`, 1000 sessions, Zipf-1.2 session skew,
  concurrency 32, 30s warmup, 10m measured windows, 3 repeats.

| Workload | What It Measures | AgentDB | MongoDB | Postgres |
|---|---|---:|---:|---:|
| `mixed-agent-loop` | Full agent loop: append+fetch, recent reads, records, forks, artifacts | 38,358 ops/s, p99 2.26ms | 24,166 ops/s, p99 2.19ms | 5,378 ops/s, p99 129.75ms |
| `append-only` | Ordered event appends without tail reads | 73,345 ops/s, p99 1.27ms | 18,191 ops/s, p99 4.07ms | 13,154 ops/s, p99 28.12ms |
| `append-fetch-hot` | Fused append plus hot-tail read, the common agent step path | 34,154 ops/s, p99 2.60ms | 16,742 ops/s, p99 2.86ms | 21,039 ops/s, p99 13.08ms |

Speedup versus each baseline:

```text
mixed-agent-loop
  vs MongoDB   | ###             1.59x
  vs Postgres  | ##############  7.13x

append-only
  vs MongoDB   | ########        4.03x
  vs Postgres  | ###########     5.58x

append-fetch-hot
  vs MongoDB   | ####            2.04x
  vs Postgres  | ###             1.62x
```

How to read this:

- This is a single-node, localhost benchmark with RocksDB-backed AgentDB,
  MongoDB standalone, and Postgres, all using normalized event-log layouts.
- The profile is `single-node-wal`: fast local developer durability, not a
  managed replicated production comparison.
- The run uses 1000 sessions, Zipf-1.2 session skew, concurrency 32, 30s warmup,
  10m measured windows, and 3 repeats.
- AgentDB's fused `append-and-fetch` API is part of the product advantage; the
  benchmark calls that out instead of hiding it.

Run a quick local sanity check:

```bash
OUT=/tmp/agentdb-bench \
  PROFILE=smoke \
  WORKLOADS=mixed-agent-loop \
  DURATION=10s \
  WARMUP=2s \
  REPEATS=1 \
  scripts/agentdb-bench-value-matrix.sh
```

Run the full long-run comparison on a machine with enough disk:

```bash
OUT=/path/with/free-space \
  MATRIX_ID=local-longrun-value \
  scripts/agentdb-bench-longrun-value-matrix.sh
```

For the benchmark tool, schema details, hardware/software notes, and caveats,
see [tools/agentdb-bench](tools/agentdb-bench/README.md). For the longer design
and tuning history, see
[docs/operations/AGENTDB_BENCH.md](docs/operations/AGENTDB_BENCH.md).

## Quick Start

Run an in-memory development server:

```bash
cargo run -p agentdb-server -- dev
```

The API listens on:

```text
http://127.0.0.1:7878
```

Check readiness:

```bash
curl -i http://127.0.0.1:7878/health/ready
```

Open the session dashboard at:

```text
http://127.0.0.1:7878/dashboard/
```

`agentdb dev` also enables the local MongoDB compatibility listener at
`127.0.0.1:7879`, creates a `dev` workspace with a demo agent/session, and
prints copy-paste commands for SDK examples, diagnostics, and local CI status.

Check the full local developer environment:

```bash
cargo run -p agentdb-server -- doctor --dev --dev-tools
```

### MongoDB Client Compatibility

AgentDB can also expose a narrow MongoDB-compatible listener for existing
MongoDB clients and tools. `agentdb dev` enables it by default; the lower-level
server command is:

```bash
cargo run -p agentdb-server -- serve --dev --mongo-bind 127.0.0.1:7879
```

Then connect with `mongosh`:

```bash
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
```

Optional basic authentication uses MongoDB `PLAIN`:

```bash
cargo run -p agentdb-server -- serve --dev \
  --mongo-bind 127.0.0.1:7879 \
  --mongo-username agentdb \
  --mongo-password secret

mongosh "mongodb://agentdb:secret@127.0.0.1:7879/?directConnection=true&authMechanism=PLAIN&authSource=%24external"
```

Use normal collection helpers for simple document access:

```javascript
use quickstart

db.events.insertOne({
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "user",
    content: "hello from mongosh"
  }
})

db.events.find({ kind: "message" })
db.sessions.find({ alias: "support-ticket-42" })
```

Use AgentDB extension commands for runtime primitives that are not native
MongoDB CRUD:

```javascript
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
```

The MongoDB listener is an adapter for driver/tool access, not a full MongoDB
replacement. The native HTTP API and SDKs remain the canonical AgentDB runtime
surface. See
[docs/reference/mongodb-compatibility.md](docs/reference/mongodb-compatibility.md)
for the mapping rules, supported subset, and AI coding-agent guidance.

For persistent single-node state:

```bash
cargo run -p agentdb-server -- init-config
cargo run -p agentdb-server -- doctor
cargo run -p agentdb-server -- serve --config agentdb.toml
```

See [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) for
deployment guidance and [docs/operations/OPERATIONS.md](docs/operations/OPERATIONS.md)
for concrete start/stop, backup, and upgrade commands.

## Documentation

Start with [docs/README.md](docs/README.md) for the full documentation map.

- **Run and deploy**:
  [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) covers
  supported deployment paths and current cluster status;
  [docs/operations/OPERATIONS.md](docs/operations/OPERATIONS.md) has exact
  start/stop, backup, upgrade, and troubleshooting guidance for the supported
  single-node shape; [docs/operations/CLUSTER_STATUS.md](docs/operations/CLUSTER_STATUS.md)
  explains what multi-node workflows are not yet supported.

- **Understand the server**:
  [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md)
  documents the current crate and request-flow shape;
  [docs/architecture/DESIGN.md](docs/architecture/DESIGN.md) captures
  constraints and non-goals; [docs/architecture/agent-db.md](docs/architecture/agent-db.md)
  is the long-form product and systems rationale.

- **Build SDKs and adapters**:
  [docs/reference/api-sdk.md](docs/reference/api-sdk.md) lists the native HTTP
  API and SDK surfaces;
  [docs/reference/session-event-conventions.md](docs/reference/session-event-conventions.md)
  defines event payload conventions, and
  [docs/reference/adapter-readiness-checklist.md](docs/reference/adapter-readiness-checklist.md)
  defines adapter expectations.

- **Operate and debug**:
  [docs/operations/PERFORMANCE.md](docs/operations/PERFORMANCE.md) has
  benchmark notes; [docs/operations/AGENTDB_BENCH.md](docs/operations/AGENTDB_BENCH.md)
  defines the MongoDB/Postgres comparison plan; and
  [docs/runbooks/README.md](docs/runbooks/README.md) links runbooks for API
  errors that include `doc_url`.

- **Roadmap and planning archive**:
  [docs/roadmap.md](docs/roadmap.md) covers future direction; historical plans
  live under [docs/superpowers/](docs/superpowers/).

## API and SDKs

See [docs/reference/api-sdk.md](docs/reference/api-sdk.md) for the full native
HTTP API and Python/TypeScript SDK surface.

## Python SDK

Install the local client:

```bash
pip install -e clients/python
```

Append an event and fetch recent session history:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
workspace = db.workspace("acme")
agent = workspace.agent("sales-agent")
session = agent.session("support-ticket-42")

result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
print(result.seq, [event.payload for event in result.recent])
```

In authenticated deployments, tenant scope is an authorization boundary derived
from user-info metadata. Normal application code works with workspaces, agents,
and sessions; local development can still pass `workspace("acme")` as an
explicit alias.

Async support is available through the optional extra:

```bash
pip install -e 'clients/python[async]'
```

## TypeScript SDK

Build the local client:

```bash
cd clients/typescript
npm install
npm run build
```

Append an event and fetch recent session history:

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({
  baseUrl: "http://127.0.0.1:7878",
  token: process.env.AGENTDB_API_KEY,
});
const workspace = await db.workspace();
const agent = await workspace.agent("sales-agent");
const session = await agent.session("support-ticket-42");

const result = await session.append(
  { role: "user", text: "hello" },
  { fetchLastN: 5 }
);
console.log(result.seq, result.recent.map((event) => event.payload));
```

For local development against an unauthenticated server without `/api/me`, use
`await db.workspace("acme")` to bind an explicit workspace alias.

## OpenAI Agents SDK

Start the local developer server:

```bash
cargo run -p agentdb-server -- dev
```

Run the adapter quickstart without an OpenAI API key:

```bash
python examples/agents/python/openai_agents_quickstart.py
```

That exercises the OpenAI Agents SDK session protocol over AgentDB. To run a
live SDK call:

```bash
pip install openai-agents
OPENAI_API_KEY=... AGENTDB_OPENAI_LIVE=1 \
  python examples/agents/python/openai_agents_quickstart.py
```

## Workspace Layout

```text
bin/agentdb-server            Server binary
crates/agentdb-api            Native HTTP routes
crates/agentdb-core           Runtime model and tenant/session/event semantics
crates/agentdb-dashboard      Session-first dashboard assets
crates/agentdb-index          Text/vector indexing helpers
crates/agentdb-meta           Control-plane metadata and routing planner
crates/agentdb-storage        RocksDB/SlateDB KV and artifact content backends
clients/python                Python SDK
clients/typescript            TypeScript SDK
adapters/langgraph-checkpoint-agentdb
                               LangGraph checkpoint/store adapter
adapters/openai-agents-agentdb
                               OpenAI Agents SDK session adapter
adapters/mcp-agentdb           MCP stdio server for AgentDB
adapters/vercel-ai-sdk-agentdb-template
                               Vercel AI SDK persistence template
config/agentdb.toml            Example server config
docs/                          Architecture, operation, design, and runbooks
examples/                      Usage examples
workload/                      Load-generation and benchmark helpers
```

## Validation

Rust:

```bash
cargo build --workspace
cargo test -p agentdb-core -p agentdb-api
cargo test -p agentdb-meta routing
cargo test -p agentdb-api routing_http
cargo test -p agentdb-api
cargo test -p agentdb-server
```

Python:

```bash
python3 -m pytest -q clients/python/tests
PYTHONPATH=adapters/langgraph-checkpoint-agentdb python3 -m pytest -q adapters/langgraph-checkpoint-agentdb/tests
PYTHONPATH=clients/python:adapters/openai-agents-agentdb python3 -m pytest -q adapters/openai-agents-agentdb/tests
PYTHONPATH=clients/python:adapters/mcp-agentdb python3 -m pytest -q adapters/mcp-agentdb/tests
```

TypeScript:

```bash
(cd clients/typescript && npm test && npm run build)
(cd adapters/vercel-ai-sdk-agentdb-template && npm test && npm run check)
```
