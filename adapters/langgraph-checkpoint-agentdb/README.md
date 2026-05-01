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
