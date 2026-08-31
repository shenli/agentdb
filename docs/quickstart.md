# Quickstart

This is the shortest path from a fresh checkout to an AgentDB-backed agent
session.

## 1. Start AgentDB

Run the in-memory development server:

```bash
cargo run -p agentdb-server -- dev
```

The API listens on `http://127.0.0.1:7878`. The dashboard is available at:

```text
http://127.0.0.1:7878/dashboard/
```

`agentdb dev` stores state in memory and creates a demo workspace/session for
experimentation.

## 2. Run The Python Agent Quickstart

In another terminal:

```bash
pip install -e clients/python
AGENTDB_URL=http://127.0.0.1:7878 python3 examples/agents/python/quickstart.py
```

The example records three turns of agent state:

- user message;
- assistant reasoning;
- tool result.

Each turn uses the AgentDB hot path: append the next event and fetch the recent
session tail in one request. The example then forks the session to demonstrate
branching execution history.

## 3. Try The TypeScript Quickstart

Build the local TypeScript client:

```bash
cd clients/typescript
npm install
npm run build
cd ../..
node --experimental-strip-types examples/agents/typescript/quickstart.ts
```

## 4. Optional Checks

Check readiness:

```bash
curl -i http://127.0.0.1:7878/health/ready
```

Check local tooling:

```bash
cargo run -p agentdb-server -- doctor --dev --dev-tools
```

`doctor --dev-tools` checks optional tools too. Missing Docker or libclang is
only a blocker if you plan to run compose-backed cluster smoke tests or rebuild
RocksDB bindings locally.

## Next Steps

- Read the full [README](../README.md).
- Review the native API and SDK reference in [reference/api-sdk.md](reference/api-sdk.md).
