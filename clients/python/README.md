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

## Testing

```bash
python3 -m pytest -q clients/python/tests
```

Live smoke tests are skipped when `target/debug/agentdb` is absent.
