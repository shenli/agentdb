# agentdb (Python)

Python client for AgentDB V2, the tenant-scoped native `/api/...` runtime
database for agent sessions, artifacts, records, skills, replay, and context.

## Install

```bash
pip install -e clients/python
pip install -e 'clients/python[async]'  # AsyncClient + httpx
```

## Start A Local Server

```bash
cargo run -p agentdb-server -- serve --in-memory --bind 127.0.0.1:8080
```

## Sync Quick Start

```python
from agentdb import Client

client = Client("http://127.0.0.1:8080")

tenant = client.create_tenant("acme")
agent = client.create_agent(tenant["tenant_id"], alias="sales-agent")
session = client.create_session(tenant["tenant_id"], agent["agent_id"])

result = session.append(
    {"role": "user", "text": "hello"},
    fetch_last_n=5,
)
print(result.seq, [event.payload for event in result.recent])
```

## Async Quick Start

```python
import asyncio
from agentdb import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:8080") as client:
        tenant = await client.create_tenant("acme-async")
        agent = await client.create_agent(tenant["tenant_id"])
        session = await client.create_session(tenant["tenant_id"], agent["agent_id"])
        result = await session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
        print(result.seq, [event.payload for event in result.recent])

asyncio.run(main())
```

## Core Surface

- `Client.create_tenant`, `get_tenant`
- `Client.create_agent`, `create_session`, `session`
- `Session.append`, `append_and_fetch`, `fetch`, `fetch_recent`
- `Session.replay`, `fork`, `subscribe`, `context`
- `Client.watch`
- `Client.put_artifact`, `get_artifact`, `get_artifact_blob`
- `Client.write_record`, `search_records`
- `Client.register_skill`, `bind_skill`

IDs are opaque typed strings such as `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`. Mutating SDK calls generate an
`Idempotency-Key` automatically unless one is supplied.

## Harness Recovery

AgentDB sessions are durable state outside the harness process. A restarted
harness should keep the tenant, agent, session, and last-seen sequence IDs, then
rebuild a session handle and continue from durable history:

```python
from agentdb import Client

client = Client("http://127.0.0.1:8080")
session = client.session(tenant_id, session_id, agent_id=agent_id)

history = session.replay(from_seq=0, limit=100)
context = session.context(include=["recent_events", "records", "skills"])

result = session.append(
    {"status": "recovered", "last_seen_seq": last_seen_seq},
    kind="agent_status",
)

for event in session.subscribe(from_seq=last_seen_seq + 1):
    handle(event)
```

Concept mapping:

| Durable-session concept | Python SDK |
|---|---|
| wake/resume handle | `client.session(tenant_id, session_id, agent_id=...)` |
| get events | `session.fetch(...)` |
| append event | `session.append(...)` |
| append and fetch | `session.append_and_fetch(...)` |
| subscribe from sequence | `session.subscribe(from_seq=...)` |
| fork session | `session.fork(...)` |
| build context | `session.context(...)` |
| tenant/session watch | `client.watch(...)` |

Use `from_seq=last_seen_seq + 1` when reconnecting a subscription to avoid
replaying events the harness has already processed. Persisting the last handled
`seq` is the harness's cursor rule.

## Testing

```bash
python3 -m pytest -q clients/python/tests
```

Live smoke tests are skipped when `target/debug/agentdb` is absent.
