# Handoff: LangChain / Deep Agents Integration

## Summary

AgentDB now has a concrete integration path for the LangChain / Deep Agents
stack through LangSmith Agent Server custom backends.

The existing `langgraph-checkpoint-agentdb` adapter still exposes the original
sync mapping classes, and now also exposes async classes and factory functions
that Agent Server can load from `langgraph.json`.

## What Changed

- Added `AgentDBAsyncSaver` for Agent Server checkpointer use.
- Added `AgentDBAsyncStore` for Agent Server store use.
- Added `langgraph_checkpoint_agentdb.langsmith` factory functions:
  - `generate_checkpointer`
  - `generate_store`
- Added thread-to-session mapping records under `["langgraph", "threads"]`.
- Updated package dependency to require `agentdb[async]`.
- Documented `langgraph.json` and environment wiring in the adapter README.
- Added unit tests for async checkpoint/store behavior.

## Main Files

- `adapters/langgraph-checkpoint-agentdb/langgraph_checkpoint_agentdb/checkpoint.py`
- `adapters/langgraph-checkpoint-agentdb/langgraph_checkpoint_agentdb/store.py`
- `adapters/langgraph-checkpoint-agentdb/langgraph_checkpoint_agentdb/langsmith.py`
- `adapters/langgraph-checkpoint-agentdb/README.md`
- `adapters/langgraph-checkpoint-agentdb/tests/test_checkpoint.py`
- `adapters/langgraph-checkpoint-agentdb/tests/test_store.py`

## Configuration Shape

LangSmith / Deep Agents deployments can point `langgraph.json` at:

```json
{
  "checkpointer": {
    "path": "langgraph_checkpoint_agentdb.langsmith:generate_checkpointer"
  },
  "store": {
    "path": "langgraph_checkpoint_agentdb.langsmith:generate_store"
  }
}
```

Required environment:

```text
AGENTDB_BASE_URL=http://127.0.0.1:7878
AGENTDB_TENANT_ID=ten_...
AGENTDB_AGENT_ID=agt_...
```

Optional auth:

```text
AGENTDB_TOKEN=...
AGENTDB_BASIC_USER=...
AGENTDB_BASIC_PASSWORD=...
```

## Validation Run

```bash
PYTHONPATH=clients/python:adapters/langgraph-checkpoint-agentdb \
  python3 -m pytest -q adapters/langgraph-checkpoint-agentdb/tests

python3 -m compileall -q adapters/langgraph-checkpoint-agentdb/langgraph_checkpoint_agentdb
```

Result: `12 passed`.

## Remaining Work

- Install `langgraph` and `langgraph-checkpoint-conformance`, then run the
  official conformance suite against `AgentDBAsyncSaver`.
- If the current LangGraph minor version expects extra method parameters,
  adjust the async method signatures while preserving the tested mapping logic.
- Exercise this in a real `langgraph dev` or Deep Agents deployment with a
  live AgentDB server.
- Add HTTP/SSE transport for `adapters/mcp-agentdb` if AgentDB should be
  exposed as a deployed MCP server in Deep Agents Deploy. The current MCP
  adapter is stdio-only.
- Decide whether thread deletion should remain append-only tombstoning or map
  to AgentDB lifecycle/GC once the runtime deletion semantics are finalized.
