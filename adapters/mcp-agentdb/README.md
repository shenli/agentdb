# mcp-agentdb

Minimal MCP stdio server for AgentDB.

It exposes AgentDB as tools, resources, and prompts:

- tools: append events, replay sessions, search records, fetch artifacts
- resources: `agentdb://session/{tenant_id}/{session_id}`
- prompts: `agentdb-session-summary`

The server is dependency-light and speaks JSON-RPC over stdio; richer transports
can layer on the same handler.

```bash
AGENTDB_BASE_URL=http://127.0.0.1:7878 agentdb-mcp
```
