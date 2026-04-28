# agentdb (Python)

Python client for [AgentDB](https://github.com/shenli/agentdb) — agent-native
operational database.

## Install

From source (recommended while AgentDB is pre-stable):

```bash
pip install -e clients/python              # sync client only
pip install -e 'clients/python[async]'     # also installs httpx for AsyncClient
```

## Quick start

Start the server with auth enabled:

```bash
AGENTDB_REQUIRE_AUTH=true cargo run -p agentdb-server -- serve --dev
```

In another shell, create a user (dev-mode only):

```bash
curl -s -X POST http://localhost:7878/api/admin/users \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"alice","tenant_external":"my-tenant","password":"hunter2"}'
```

Then in Python:

```python
import time
from agentdb import Client

client = Client(
    base_url="http://localhost:7878",
    auth=("alice", "hunter2"),
)

session = client.session(f"run-{int(time.time())}")
result = session.append({"kind": "plan", "body": "..."}, fetch_last_n=5)
print("new seq:", result.seq)
for ev in result.recent:
    print(ev.seq, ev.payload)
```

Tenant is derived from the `auth` credentials server-side — no `tenant`
argument anywhere in v2.

## Async (`AsyncClient`)

Modern agent code (LangChain, LlamaIndex, the Anthropic / OpenAI
SDKs) is async-first. Install with the `[async]` extra and
import `AsyncClient` instead of `Client`:

```python
pip install 'agentdb[async]'
```

```python
import asyncio
import time
from agentdb import AsyncClient

async def main():
    async with AsyncClient(
        base_url="http://localhost:7878",
        auth=("alice", "hunter2"),
    ) as client:
        session = client.session(f"run-{int(time.time())}")
        result = await session.append(
            {"role": "user", "text": "hello"},
            fetch_last_n=5,
        )
        print(result.seq, [e.payload for e in result.recent])

asyncio.run(main())
```

The real payoff is parallelism — write to many sessions at once via
`asyncio.gather`:

```python
results = await asyncio.gather(
    client.session("agent-a").append({"i": 0}),
    client.session("agent-b").append({"i": 1}),
    client.session("agent-c").append({"i": 2}),
)
```

`AsyncClient` is built on `httpx` and keeps a connection pool;
construct one per app and reuse. `async with` (or an explicit
`await client.aclose()`) drains the pool on shutdown.

## Auth model

```python
# Basic auth — for session/subscribe routes.
# Tenant is derived from the user record server-side.
client = Client(
    base_url="https://prod.example.com",
    auth=("alice", "hunter2"),
)

# JWT Bearer auth — for channel routes (broadcast / subscribe_channel).
# Use `token=` for a static JWT or `get_token=` for a rotating provider.
client = Client(
    base_url="https://prod.example.com",
    token="eyJ...",
)

# Both can be supplied simultaneously:
#   Basic auth covers session/subscribe, JWT covers channel routes.
client = Client(
    base_url="https://prod.example.com",
    auth=("alice", "hunter2"),
    token="eyJ...",
)
```

## Fork

```python
parent = client.session("agent-1")
parent.append({"role": "user", "text": "hello"})
child = parent.fork("agent-1-branch-a")  # returns a Session for the new id
child.append({"role": "tool", "text": "branch off"})
```

The child sees a frozen snapshot of the parent's events at fork time,
MVCC-backed — see the architecture doc for the semantics.

## Broadcast (ephemeral pub/sub)

```python
# Publish to a channel (ephemeral — no replay, no durability).
client.broadcast(channel="my-chan", payload={"event": "typing"})

# Subscribe to a channel.
for msg in client.subscribe_channel(channel="my-chan"):
    print(msg.channel, msg.payload)
```

Tenant is resolved server-side from the JWT Bearer token for channel routes.

## Errors

Every server-surfaced failure raises an `AgentDBError` subclass:

| HTTP | Class           | Typical cause                                  |
|------|-----------------|------------------------------------------------|
| 400  | `BadRequest`    | malformed body, tenant resolution failed       |
| 401  | `Unauthorized`  | missing/invalid `Authorization`                |
| 403  | `Forbidden`     | authed user can't act on this tenant/session   |
| 404  | `NotFound`      | tenant or session doesn't exist                |
| 409  | `Conflict`      | resource already exists / state collision      |
| 421  | `Misdirected`   | multi-node routing drift                        |
| 429  | `RateLimited`   | quota exhausted; carries `retry_after_ms`      |
| 5xx  | `ServerError`   | forwarding failure, fault injection, draining |

Catch `AgentDBError` at the top of an agent loop for blanket
handling, or a subclass when reacting to a specific category.

## Testing the SDK

The smoke tests spawn `agentdb serve --dev` with `AGENTDB_REQUIRE_AUTH=true`
and create a test user via the admin API. Build the server first:

```bash
cargo build -p agentdb-server
pytest clients/python
```

Tests that require a live server are skipped automatically if the binary
hasn't been built.
