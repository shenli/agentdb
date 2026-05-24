<div align="center">
  <h1>AgentDB</h1>
  <h3>Runtime state for AI agents.</h3>
</div>

<div align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
  <a href="docs/README.md"><img src="https://img.shields.io/badge/docs-local-0f766e" alt="Docs"></a>
  <a href="docs/quickstart.md"><img src="https://img.shields.io/badge/quickstart-5_min-7c3aed" alt="Quickstart"></a>
</div>

<br>

AgentDB is a durable session log for AI agents. It stores what happened while an
agent was running: messages, tool calls, tool outputs, artifacts, records,
context decisions, lifecycle state, replay metadata, and fork history.

Use AgentDB when agent state needs to survive process restarts, worker moves,
sandbox failures, model retries, human review pauses, or long context windows.
The native HTTP API plus the Python and TypeScript SDKs are the canonical
runtime surface.

## Why AgentDB

- **Resume and recover agents** from durable session history instead of
  reconstructing state from scattered logs.
- **Replay and fork runs** to debug decisions, inspect lineage, or explore an
  alternate path.
- **Keep runtime state together** across events, records, artifacts, context
  packs, skills, and lifecycle transitions.
- **Stream live state** to dashboards, supervisors, evaluators, and tools.

General databases can store agent data. AgentDB focuses on the runtime semantics
that agent platforms usually have to build around those stores.

| AI agent need | Postgres / RDBMS | MongoDB / DynamoDB / NoSQL | Log and stream stores | AgentDB |
|---|---|---|---|---|
| Append an event and fetch the recent session tail | Strong transactions and queries; app owns the session schema | Fast document/key writes; app owns ordering and tail reads | Strong append path; recent context reads need another layer | Native append-and-fetch |
| Replay or fork a run | Durable history if modeled carefully | Durable documents; branch semantics are app code | Replay by offset; session forks are app code | Native replay and fork |
| Link events, artifacts, records, and context | Possible with joins and schema discipline | Flexible documents; cross-object consistency is app code | Events can reference blobs; state is usually scattered | First-class runtime references |
| Manage agent lifecycle and inspection | Mature ops; agent lifecycle is custom | Managed scale options; lifecycle is custom | Retention is strong; inspection needs external state | Session lifecycle, SDKs, and dashboard-friendly state |

AgentDB is not a vector database, memory platform, agent framework, workflow
engine, or general MongoDB replacement. It is the runtime-state database those
systems can build on.

## Quick Start

Start the in-memory development server:

```bash
cargo run -p agentdb-server -- dev
```

The API listens on `http://127.0.0.1:7878`, and the dashboard is available at
`http://127.0.0.1:7878/dashboard/`.

Run the Python quickstart in another terminal:

```bash
pip install -e clients/python
AGENTDB_URL=http://127.0.0.1:7878 python3 examples/agents/python/quickstart.py
```

The example appends agent events, reads the recent session tail after each
append, then forks the session into an alternate timeline.

For the full walkthrough, including TypeScript and local tooling checks, see
[docs/quickstart.md](docs/quickstart.md).

## SDKs

Python:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
session = db.workspace("acme").agent("sales-agent").session("ticket-42")

result = session.append_user("hello", fetch_last_n=5)
print(result.seq, [event.payload for event in result.recent])
```

TypeScript:

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878" });
const workspace = await db.workspace("acme");
const agent = await workspace.agent("sales-agent");
const session = await agent.session("ticket-42");

const result = await session.appendUser("hello", { fetchLastN: 5 });
console.log(result.seq, result.recent.map((event) => event.payload));
```

More examples:

- [Python SDK](clients/python/README.md)
- [TypeScript SDK](clients/typescript/README.md)
- [Native HTTP API](docs/reference/api-sdk.md)
- [OpenAI Agents SDK adapter](adapters/openai-agents-agentdb/README.md)
- [LangGraph adapter](adapters/langgraph-checkpoint-agentdb/README.md)

## MongoDB Client Compatibility

AgentDB includes an optional MongoDB wire listener for existing drivers and
inspection tools. It is intentionally narrow: use fixed AgentDB collections,
`directConnection=true`, and AgentDB extension commands for runtime primitives.

Start with [docs/reference/mongodb-compatibility.md](docs/reference/mongodb-compatibility.md)
or the copyable examples in [examples/mongodb](examples/mongodb/README.md).

## Performance

AgentDB's benchmark target is the common agent loop: append the next event and
fetch the recent session tail needed for the next model or tool step. Current
single-node benchmark notes and reproduction commands live in
[docs/operations/PERFORMANCE.md](docs/operations/PERFORMANCE.md) and
[tools/agentdb-bench](tools/agentdb-bench/README.md).

## Documentation

Start with [docs/README.md](docs/README.md) for the full documentation map.

| Need | Read |
|---|---|
| First local run | [docs/quickstart.md](docs/quickstart.md) |
| API and SDK surface | [docs/reference/api-sdk.md](docs/reference/api-sdk.md) |
| MongoDB adapter rules | [docs/reference/mongodb-compatibility.md](docs/reference/mongodb-compatibility.md) |
| Architecture | [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) |
| Deployment | [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) |
| Operations | [docs/operations/OPERATIONS.md](docs/operations/OPERATIONS.md) |
| Benchmarks | [tools/agentdb-bench](tools/agentdb-bench/README.md) |
| Roadmap | [docs/roadmap.md](docs/roadmap.md) |

## Repository Layout

```text
crates/                 Rust server, runtime, storage, routing, and API crates
clients/python          Python SDK
clients/typescript      TypeScript SDK
adapters/               Framework and tool adapters
docs/                   Architecture, reference, operations, and runbooks
examples/               Agent, MongoDB, compose, and Helm examples
ops/                    Local CI, smoke tests, observability, and systemd units
tools/                  Benchmarks and workload tooling
```

## Development

Common checks:

```bash
cargo test -p agentdb-core -p agentdb-api
python3 -m pytest -q clients/python/tests
(cd clients/typescript && npm test && npm run build)
```

For the full development workflow, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

AgentDB is licensed under the [MIT License](LICENSE). Third-party dependencies
keep their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
