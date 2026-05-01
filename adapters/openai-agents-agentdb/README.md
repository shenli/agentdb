# openai-agents-agentdb

OpenAI Agents SDK session adapter backed by AgentDB sessions.

The current Agents SDK custom session protocol uses async methods:

- `get_items(limit=None)`
- `add_items(items)`
- `pop_item()`
- `clear_session()`

This adapter stores every operation as an ordered AgentDB event. `pop_item` and
`clear_session` are represented as tombstone events so conversation history stays
auditable while the effective session view matches the SDK protocol.

```python
from agentdb import AsyncClient
from openai_agents_agentdb import AgentDBSession

client = AsyncClient("http://127.0.0.1:7878")
session = AgentDBSession(
    client,
    tenant_id="ten_...",
    agent_id="agt_...",
    session_id="ses_...",
)

await session.add_items([{"role": "user", "content": "hello"}])
items = await session.get_items()
```
