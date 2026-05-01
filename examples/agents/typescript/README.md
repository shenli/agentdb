# Quickstart agent (TypeScript / Node)

A short agent loop that uses AgentDB for working memory.
Three turns of "thought / action / observation" plus a session
fork that branches into an alternate timeline.

## Run it

```bash
# 1. Start the server (one terminal)
cargo run -p agentdb-server -- serve --dev

# 2. Build the SDK (another terminal)
cd clients/typescript
npm install
npm run build
cd ../..

# 3. Run the example. Node 22.6+ strips TS types directly;
# Node 24 enables it by default.
node --experimental-strip-types \
     examples/agents/typescript/quickstart.ts
```

You should see three turns of output, each showing the rolling
working memory grow by one event, then a fork that inherits the
parent's events at fork time.

## What it shows

- **`agent.session("quickstart-agent-ts")`** — create a session by
  application-facing alias while AgentDB assigns the canonical `ses_*` ID.
- **`session.append(event, { fetchLastN: 5 })`** — the AgentDB
  hot-path primitive: write one event and read the last N back
  in the same round trip. The canonical agent loop shape.
- **`session.fork(childAlias)`** — branch a session into an
  alternate timeline. The child sees a frozen snapshot of the
  parent's events at fork time, MVCC-backed.

## Customize

```bash
# Talk to a non-local server
AGENTDB_URL=http://prod.example.com:7878 \
  node --experimental-strip-types examples/agents/typescript/quickstart.ts

# Use a different workspace alias
AGENTDB_WORKSPACE=t-myapp \
  node --experimental-strip-types examples/agents/typescript/quickstart.ts
```

## Next steps

- Full SDK reference: [`clients/typescript/README.md`](../../../clients/typescript/README.md)
- Sister Python example: [`examples/agents/python/`](../python/)
