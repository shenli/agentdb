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

Application code should not pass `tenant_id`. Tenant scope is resolved from the
authenticated principal through AgentDB user-info metadata. For local/dev
servers that do not expose `/api/me` yet, pass a `workspace` alias and the
adapter will resolve or create the backing tenant internally.

```python
import os

from agents import Agent, Runner
from openai_agents_agentdb import AgentDBSessions

sessions = AgentDBSessions(
    api_key=os.environ["AGENTDB_API_KEY"],
    agent="support-agent",
)

session = await sessions.session("ticket-42")

agent = Agent(
    name="Support agent",
    instructions="Help the user with their support request.",
)

result = await Runner.run(agent, "What happened with my order?", session=session)
print(result.final_output)
```

`AgentDBSession` remains available as a low-level protocol implementation when
you already have canonical AgentDB IDs. New integrations should prefer
`AgentDBSessions` so tenant resolution stays an authentication concern.

For local development against the OSS server, pass `workspace="dev"` because
the local server may not expose hosted user-info yet.

From a repo checkout, the fastest local smoke is:

```bash
cargo run -p agentdb-server -- dev
python examples/agents/python/openai_agents_quickstart.py
```

The quickstart defaults to a protocol-only run that does not require an OpenAI
API key. Set `AGENTDB_OPENAI_LIVE=1` and `OPENAI_API_KEY` to run a live Agents
SDK call.
