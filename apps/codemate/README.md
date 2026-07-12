# CodeMate

CodeMate is a local web app for reviewing AI coding-agent sessions. It imports
local Codex session files, shows a timeline, generates a PR review packet,
identifies common issues, and suggests engineering best-practice updates.

This package is intentionally separate from AgentDB core. AgentDB is the local
runtime store behind imported sessions.

## Run

```bash
cd apps/codemate
npm start
```

or from the repository root:

```bash
node apps/codemate/src/server.js
```

Then open the printed localhost URL.

## Current MVP

- Detects Codex sessions under `$CODEX_HOME/sessions` or `~/.codex/sessions`.
- Parses session timelines without requiring hooks or MCP streaming.
- Defaults review output to events after the latest reset/compaction marker.
- Generates:
  - PR review packet
  - common issues report
  - best-practice suggestions
- Optionally imports selected session activity into a local AgentDB server.

Set `AGENTDB_URL` to override the default `http://127.0.0.1:7878`.
