# Quickstart

AgentDB is a durable, replayable session log for AI agents. This quickstart goes
from nothing installed to **browsing your own coding-agent sessions** — no code
required.

The outcome: your Claude Code / Codex history, imported and searchable, with a
server that keeps running so new sessions show up on their own.

## 1. Install

Prebuilt binaries and a Homebrew tap are coming with the first tagged release.
Until then, install from a checkout with Cargo (needs a Rust toolchain, plus
`clang` and `protobuf` to build the RocksDB bindings):

```bash
cargo install --path bin/agentdb-server   # installs `agentdb`
cargo install --path bin/agentdbctl        # installs `agentdbctl`
```

Both land on your `PATH` (usually `~/.cargo/bin`).

## 2. Start AgentDB

```bash
agentdb serve
```

Persistent writes fsync the RocksDB WAL by default. Add `--changefeed` if you
need the changefeed or exports; the ledger is disabled by default.

The server prints its dashboard URL and data path on startup. State is stored in
a **machine-scoped** directory (`~/.agentdb` by default), so it opens the same
database no matter which shell you launch it from. The API listens on
`http://127.0.0.1:7878` (loopback, no token required).

To keep it running whenever you log in — so imports can stay live — install it as
a background service instead:

```bash
agentdb service install     # launchd on macOS, systemd --user on Linux
```

## 3. Import your agent sessions

Pull in the sessions your coding agents have already recorded. Each JSONL log
becomes one AgentDB session; the repository it ran in becomes the workspace.

```bash
agentdbctl import claude-code    # scans ~/.claude/projects
agentdbctl import codex          # scans ~/.codex/sessions
```

Re-running is safe — nothing is duplicated — and `--watch` keeps importing new
lines as your agents write them:

```bash
agentdbctl import claude-code --watch
```

`agentdbctl import` streams over the API, so it works against a running or
remote server. For a large one-shot bulk load, `agentdb import` writes directly
into the local store instead (faster, no HTTP) — stop the server first, since
the store is single-writer. It can also keep oversized tool outputs as
artifacts rather than skipping them:

```bash
agentdb import codex --large-events artifact
```

## 4. See it

Open the dashboard timeline in a browser:

```text
http://127.0.0.1:7878/dashboard/
```

You can also connect the **MCP server** (`adapters/mcp-agentdb`) to your agent and
ask it about a past session — replaying "what did we do about this last week"
against your own imported history.

## 5. Build on it (optional)

To write an app against AgentDB rather than browse it, use a client SDK. The
Python and TypeScript quickstarts exercise the hot path — append an event, fetch
the recent tail in one call, and fork a session:

```bash
# Python
pip install -e clients/python
AGENTDB_URL=http://127.0.0.1:7878 python3 examples/agents/python/quickstart.py

# TypeScript (Node.js 22.6+ for --experimental-strip-types)
cd clients/typescript && npm install && npm run build && cd ../..
node --experimental-strip-types examples/agents/typescript/quickstart.ts
```

For a throwaway, no-persistence server that also bootstraps a demo
workspace/session, use `agentdb dev` instead of `agentdb serve`.

## Troubleshooting

Check readiness and local tooling:

```bash
curl -i http://127.0.0.1:7878/health/ready
agentdb doctor
```

`agentdb service status` shows whether the background service is installed and
running.

## Next steps

- Read the full [README](../README.md).
- Review the native API and SDK reference in [reference/api-sdk.md](reference/api-sdk.md).
- Turn on [encryption at rest](operations/single-node-production-checklist.md) for
  production single-node deployments.
