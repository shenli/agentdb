# Python agent examples

Small agent loops that use AgentDB for working memory, coordination,
and durable runtime state.

## Quickstart

```bash
# 1. Start the server (one terminal)
cargo run -p agentdb-server -- serve --dev

# 2. Run from the repo checkout (another terminal)
python examples/agents/python/quickstart.py

# Optional: install the SDK if you want the package in your env
pip install -e clients/python
python examples/agents/python/quickstart.py
```

You should see three turns of output, each showing the rolling working memory
grow by one event, then a fork that inherits the parent's events at fork time.

## Session-first runtime demo

```bash
cargo run -p agentdb-server -- serve --dev
python examples/agents/python/session_first_demo.py

# Optional: install the SDK into your environment instead
pip install -e clients/python
python examples/agents/python/session_first_demo.py
```

This demo treats AgentDB as the runtime database for an agent harness. It
records user input, tool calls, tool results, artifact refs, context provenance,
model output, a simulated harness restart, replay, and resume-from-`seq` in one
ordered session timeline.

## What it shows

- **`session.append(event, fetch_last_n=5)`** — the AgentDB hot-
  path primitive: write one event and read the last N back in the
  same round trip. This is the canonical agent loop shape.
- **`session.fork(child_id)`** — branch a session into an
  alternate timeline. The child sees a frozen snapshot of the
  parent's events at fork time, MVCC-backed.
- **`session.context(...)` + `context_pack_built`** — persist the
  context that shaped a model response and link it back with
  `source_refs`.
- **`put_artifact(...)` + `artifact_refs`** — keep large tool outputs
  out of inline event payloads while preserving provenance.
- **`replay(from_seq=...)` / `subscribe(from_seq=...)`** — recover an
  agent harness after restart and resume without gaps.

## Customize

```bash
# Talk to a non-local server
AGENTDB_URL=http://prod.example.com:7878 python quickstart.py

# Use a different tenant (dev mode only — auth-mode derives it)
AGENTDB_TENANT=t-myapp python quickstart.py
```

## Next steps

- The full SDK reference lives at [`clients/python/README.md`](../../../clients/python/README.md).
