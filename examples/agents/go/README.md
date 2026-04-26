# Quickstart agent (Go)

Short agent loop using AgentDB for working memory. Three turns
of "thought / action / observation" plus a session fork that
branches into an alternate timeline.

## Run it

```bash
# 1. Start the server (one terminal)
cargo run -p agentdb-server -- serve --dev

# 2. Run the example (another terminal)
cd examples/agents/go
go run .
```

You should see three turns of growing working memory, then a
fork that inherits the parent's events at fork time.

## What it shows

- **`session.Append(ctx, event, agentdb.AppendOptions{FetchLastN: 5})`**
  — write one event and read the last N back in the same round
  trip. The canonical agent loop shape.
- **`session.Fork(ctx, childID)`** — branch into an alternate
  timeline. The child sees a frozen snapshot of the parent's
  events at fork time, MVCC-backed.

## Customize

```bash
AGENTDB_URL=http://prod.example.com:7878 go run .
AGENTDB_TENANT=t-myapp go run .   # dev mode only
```

## Next steps

- Full SDK reference: [`clients/go/README.md`](../../../clients/go/README.md)
- Sister Python: [`examples/agents/python/`](../python/)
- Sister TypeScript: [`examples/agents/typescript/`](../typescript/)
