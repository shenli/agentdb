# AgentDB Adapters

Framework adapter proof points for the runtime API.

| Adapter | Purpose |
|---|---|
| `langgraph-checkpoint-agentdb` | LangGraph checkpoint saver and namespaced store |
| `openai-agents-agentdb` | OpenAI Agents SDK custom session memory |
| `mcp-agentdb` | MCP stdio server exposing AgentDB tools/resources/prompts |
| `vercel-ai-sdk-agentdb-template` | Vercel AI SDK chat persistence template |

These packages should remain mostly mapping code. If an adapter has to invent
ordering, replay, scoped search, artifact refs, idempotency, or fork lineage,
that is a runtime/API gap to fix in AgentDB.
