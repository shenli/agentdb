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

- **`session.append(event, { fetchLastN: 5 })`** — the AgentDB
  hot-path primitive: write one event and read the last N back
  in the same round trip. The canonical agent loop shape.
- **`session.fork(childId)`** — branch a session into an
  alternate timeline. The child sees a frozen snapshot of the
  parent's events at fork time, MVCC-backed.

## Customize

```bash
# Talk to a non-local server
AGENTDB_URL=http://prod.example.com:7878 \
  node --experimental-strip-types examples/agents/typescript/quickstart.ts

# Use a different tenant (dev mode only — auth-mode derives it)
AGENTDB_TENANT=t-myapp \
  node --experimental-strip-types examples/agents/typescript/quickstart.ts
```

## `broadcast_typing.ts` — typing-indicator demo (Phase 2)

Demonstrates ephemeral pub/sub using Phase 2 broadcast channels. A
publisher emits typing pings every 500ms; a reactor subscribes to
the same channel and prints every event. There is no durability —
if the reactor is disconnected during a publish, that ping is lost
(by design — typing indicators don't need replay).

```bash
# 1. Start the server (one terminal)
cargo run -p agentdb-server -- serve --dev

# 2. Build the SDK once if you haven't already
cd clients/typescript && npm install && npm run build && cd ../..

# 3. Run the demo
node --experimental-strip-types \
     examples/agents/typescript/broadcast_typing.ts
```

Expected output (interleaving may vary):

```
reactor: subscribing to room:42
publisher: sent typing ping 0
reactor: alice → typing
publisher: sent typing ping 1
reactor: alice → typing
...
publisher: done
reactor: alice → stopped_typing
```

## Next steps

- Full SDK reference: [`clients/typescript/README.md`](../../../clients/typescript/README.md)
- Sister Python example: [`examples/agents/python/`](../python/)
- The Mongo wire port (`:7879`) speaks the standard protocol —
  any MongoDB driver works for read-only queries.
