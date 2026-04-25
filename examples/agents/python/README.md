# Quickstart agent (Python)

A 60-line agent loop that uses AgentDB for working memory. Three
turns of "thought / action / observation" plus a session fork
that branches into an alternate timeline.

## Run it

```bash
# 1. Start the server (one terminal)
cargo run -p agentdb-server -- serve --dev

# 2. Install the SDK + run the example (another terminal)
pip install -e clients/python
python examples/agents/python/quickstart.py
```

You should see three turns of output, each showing the rolling
working memory grow by one event, then a fork that inherits the
parent's events at fork time.

## What it shows

- **`session.append(event, fetch_last_n=5)`** — the AgentDB hot-
  path primitive: write one event and read the last N back in the
  same round trip. This is the canonical agent loop shape.
- **`session.fork(child_id)`** — branch a session into an
  alternate timeline. The child sees a frozen snapshot of the
  parent's events at fork time, MVCC-backed.

## Customize

```bash
# Talk to a non-local server
AGENTDB_URL=http://prod.example.com:7878 python quickstart.py

# Use a different tenant (dev mode only — auth-mode derives it)
AGENTDB_TENANT=t-myapp python quickstart.py
```

## Next steps

- The full SDK reference lives at [`clients/python/README.md`](../../../clients/python/README.md).
- Wire-protocol clients (any MongoDB driver) work too — the
  AgentDB Mongo wire port is `:7879` by default. See the main
  [`README.md`](../../../README.md) for examples.
