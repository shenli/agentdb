# langgraph-checkpoint-agentdb

LangGraph adapter for AgentDB sessions.

The adapter maps LangGraph checkpoint traffic onto AgentDB primitives:

- checkpoints and pending writes become ordered session events;
- checkpoint lookup/history use AgentDB replay/fetch;
- durable store entries become searchable records under a LangGraph namespace;
- idempotency keys are derived from thread/checkpoint/task identifiers.

The package intentionally keeps framework-specific code thin. If an adapter
needs to invent ordering, fork lineage, artifact references, or scoped search,
that should be treated as an AgentDB runtime gap.

## LangSmith / Deep Agents deployment

LangSmith Agent Server can load custom checkpointer and store backends from
`langgraph.json`. This package exposes async factories for that path:

```json
{
  "dependencies": [
    ".",
    "./adapters/langgraph-checkpoint-agentdb",
    "./clients/python[async]"
  ],
  "graphs": {
    "agent": "./src/agent/graph.py:graph"
  },
  "env": ".env",
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

Optional auth environment:

```text
AGENTDB_TOKEN=...
AGENTDB_BASIC_USER=...
AGENTDB_BASIC_PASSWORD=...
```

The checkpointer accepts normal LangGraph configs with only `thread_id`; it
creates an AgentDB session for the thread and records the thread-to-session
mapping under `["langgraph", "threads"]` so later workers can recover the same
session. Returned configs include `configurable.agentdb_session_id` for callers
that preserve the cursor explicitly.

Before deploying, validate the checkpointer against LangGraph's conformance
suite when `langgraph-checkpoint-conformance` is available. The async surface
implements the required Agent Server methods: `aput`, `aput_writes`,
`aget_tuple`, `alist`, and `adelete_thread`.

Deep Agents Deploy itself also has convention-based `AGENTS.md`, `skills/`,
`user/`, `subagents/`, `mcp.json`, sandbox, auth, and frontend behavior. AgentDB
does not replace those surfaces; it backs the LangGraph checkpoint/store layer
inside the deployment.

## Minimal usage

```python
from agentdb import Client
from langgraph_checkpoint_agentdb import AgentDBSaver, AgentDBStore

client = Client("http://127.0.0.1:7878")
saver = AgentDBSaver(client, tenant_id="ten_...", agent_id="agt_...")
store = AgentDBStore(client, tenant_id="ten_...")

config = {
    "configurable": {
        "thread_id": "incident-42",
        "agentdb_session_id": "ses_...",
    }
}

saver.put(config, {"id": "ckpt-1", "channel_values": {}}, {"source": "input"})
store.put(("incidents",), "incident-42", {"severity": "high"}, tags=["incident"])
```

## Adapter readiness

The saver can validate that a LangGraph thread is using AgentDB's session layer
as intended:

```python
report = saver.validate_readiness(config)
if not report.ready:
    raise AssertionError(report.warnings)
```

`validate_readiness` checks the LangGraph checkpoint timeline for AgentDB
ordering, checkpoint IDs, monotonic sequence, and resume-cursor coverage. It also
exercises the shared SDK adapter-readiness validator with a recovered-status
proof event so the adapter stays aligned with the session-layer contract.

For tests, use `assert_ready`:

```python
saver.assert_ready(config)
```
