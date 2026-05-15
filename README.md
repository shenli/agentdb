<div align="center">
  <h1>AgentDB</h1>
  <h3>Scalable, durable, cost-efficient runtime state for AI agents.</h3>
</div>

<div align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
  <a href="docs/README.md"><img src="https://img.shields.io/badge/docs-local-0f766e" alt="Docs"></a>
  <a href="docs/operations/DEPLOYMENT.md"><img src="https://img.shields.io/badge/deployment-guide-7c3aed" alt="Deployment"></a>
</div>

<br>

AgentDB is a purpose-built distributed database for AI agent runtime state. It
stores the durable execution state that sits outside the model context window:
ordered session events, tool calls, tool results, artifacts, memory records,
lifecycle state, and replay metadata.

Agents create a different data shape than request/response applications. A
single run can stay open for minutes or hours, emit many partial events, attach
large model outputs and tool payloads, fork into alternate timelines, and keep
accumulating history long after the active context window has moved on. The
write path is usually append-heavy, while the hot read path repeatedly asks for
the newest session tail, selected memory records, artifacts, and enough context
to take the next step.

Generic databases and log stores can hold pieces of that data, but application
teams still have to rebuild agent-specific semantics around ordering,
idempotency, replay, fork, hot-tail reads, retained artifacts, lifecycle, and
tenant boundaries. AgentDB makes those runtime primitives first-class so agent
harnesses can keep moving while the durable record remains inspectable,
recoverable, and cost-aware as data accumulates.

Use AgentDB when you are building agents that need to:

- resume long-running work after process, model, sandbox, or worker failures;
- replay or fork a session to debug a decision or explore another path;
- keep model outputs, tool calls, files, memory, and context decisions in one
  queryable runtime record;
- stream live agent state to dashboards, supervisors, or evaluators;
- scale retained history without keeping every event and artifact in a hot
  application database.

The native HTTP API, Python SDK, and TypeScript SDK are the canonical runtime
surface. A narrow MongoDB-compatible listener is available for existing drivers
and inspection tools.

## Quick Start

Run an in-memory development server in one terminal:

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

In another terminal, install the local Python SDK and run the session
quickstart:

```bash
pip install -e clients/python
AGENTDB_URL=http://127.0.0.1:7878 python3 examples/agents/python/quickstart.py
```

That quickstart appends three agent events, reads the recent session tail after
each append, then forks the session into an alternate timeline.

Optional: check the full local developer environment:

```bash
cargo run -p agentdb-server -- doctor --dev --dev-tools
```

`doctor --dev-tools` checks optional tooling too. Missing Docker or libclang is
only a blocker if you plan to run compose-backed cluster smoke tests or rebuild
RocksDB bindings locally.

For a shorter walkthrough, see [docs/quickstart.md](docs/quickstart.md).

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

## Architecture

AgentDB sits between agent harnesses, tools, dashboards, and downstream
analytics systems. It exposes session-native APIs for append, fetch, replay,
fork, context, subscribe, and watch, while the runtime owns tenant boundaries,
policy, routing, storage layout, lifecycle, and durable export cursors.

```text
                         AI agent platform

  tenant A / workspace A                         tenant B / workspace B
  support agents, sessions                       research agents, sessions
             |                                              |
             | native SDK / HTTP: append, fetch, search     |
             | replay, fork, context, subscribe, watch      |
             v                                              v
      +------------------------------------------------------------------+
      |                              AgentDB                             |
      | multi-tenant runtime, policy, routing, and storage boundary      |
      |                                                                  |
      |  tenants -> agents -> sessions -> ordered events                 |
      |      |          |          |                                      |
      |      |          |          +--> artifacts -> content backend      |
      |      |          |          +--> records   -> local/global indexes |
      |      |          |          +--> skills, lifecycle, policy         |
      |      |                                                         |
      |      +--> watch cursors for live dashboards and supervisors      |
      |      +--> managed export consumers with durable ack cursors      |
      |                                                                  |
      |  single node: RocksDB runtime store                              |
      |  distributed: cells -> shards -> tablets -> Raft/local LSM       |
      +------------------------------------------------------------------+
          |                     |                         |
          | read/replay/watch   | export backfill/tail    | tool outputs
          v                     v                         v
   dashboard, replay     ClickHouse / ClickStack     tools, sandboxes,
   viewers, evaluators   JSONL / Parquet archives    MCP, app systems
```

For the full crate layout, request flow, storage model, and distributed
components, see [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md).

## Performance

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

Throughput speedup versus each baseline:

![AgentDB benchmark speedup chart](docs/assets/agentdb-benchmark-speedup.png)

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
  tools/agentdb-bench/scripts/agentdb-bench-value-matrix.sh
```

Run the full long-run comparison on a machine with enough disk:

```bash
OUT=/path/with/free-space \
  MATRIX_ID=local-longrun-value \
  tools/agentdb-bench/scripts/agentdb-bench-longrun-value-matrix.sh
```

For the benchmark tool, schema details, hardware/software notes, and caveats,
see [tools/agentdb-bench](tools/agentdb-bench/README.md). For the longer design
and tuning history, see
[docs/operations/AGENTDB_BENCH.md](docs/operations/AGENTDB_BENCH.md).

## Documentation

Start with [docs/README.md](docs/README.md) for the full documentation map.

- **Run and deploy**:
  [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) covers
  supported deployment paths and current cluster status;
  [docs/operations/OPERATIONS.md](docs/operations/OPERATIONS.md) has exact
  start/stop, backup, upgrade, and troubleshooting guidance for the supported
  deployment shapes; [docs/operations/CLUSTER_STATUS.md](docs/operations/CLUSTER_STATUS.md)
  tracks distributed deployment status and operations.

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

- **Roadmap**:
  [docs/roadmap.md](docs/roadmap.md) covers future direction and open work.

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
python3 examples/agents/python/openai_agents_quickstart.py
```

That exercises the OpenAI Agents SDK session protocol over AgentDB. To run a
live SDK call:

```bash
pip install openai-agents
OPENAI_API_KEY=... AGENTDB_OPENAI_LIVE=1 \
  python3 examples/agents/python/openai_agents_quickstart.py
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
infra/                         Cloud provisioning and deployment assets
ops/                           Operational scripts, local CI, smoke tests, and systemd units
scripts/                       Small repo-level entrypoints, install, and license checks
tools/                         Developer/operator tools such as agentdb-bench
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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, validation
commands, and pull-request expectations. Please report security issues through
[SECURITY.md](SECURITY.md), not public issues.

## License

AgentDB is licensed under the [MIT License](LICENSE).
Third-party dependencies keep their own licenses; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for release compliance notes.
