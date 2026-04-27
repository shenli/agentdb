"""Python client for AgentDB.

Typical usage in dev mode (no auth):

    from agentdb import Client

    client = Client("http://127.0.0.1:7878")
    session = client.session("agent-1", tenant="demo")
    result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
    print(result.seq, [e.payload for e in result.recent])

The `append + fetch_last_n` pattern is the canonical hot path —
write one event and read the last N back in the same round trip.
For pure reads, point a MongoDB driver at the wire port (default
:7879).

In auth mode the tenant is implicit (derived from credentials):

    client = Client("https://prod.example.com", user="alice", password="hunter2")
    session = client.session("agent-1")  # tenant comes from auth
    session.append({"role": "tool", "text": "did a thing"})

For async code (LangChain, LlamaIndex, the Anthropic / OpenAI
SDKs, etc.) install the optional `[async]` extra and use
`AsyncClient`:

    pip install 'agentdb[async]'

    import asyncio
    from agentdb import AsyncClient

    async def main():
        async with AsyncClient("http://127.0.0.1:7878") as client:
            session = client.session("agent-1", tenant="demo")
            result = await session.append({"role": "user", "text": "hi"},
                                          fetch_last_n=5)
            print(result.seq)

    asyncio.run(main())

See `examples/agents/python/quickstart.py` (sync) and
`examples/agents/python/async_quickstart.py` (async) for
working agent loops.
"""

from ._broadcast import BroadcastMessage
from .client import Client, Session, AppendResult, Event
from .errors import (
    AgentDBError,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    RateLimited,
    ServerError,
    Misdirected,
    SubscriptionClosed,
)

__all__ = [
    "Client",
    "Session",
    "AppendResult",
    "Event",
    "AsyncClient",
    "AsyncSession",
    "BroadcastMessage",
    "AgentDBError",
    "BadRequest",
    "Unauthorized",
    "Forbidden",
    "NotFound",
    "Conflict",
    "RateLimited",
    "ServerError",
    "Misdirected",
    "SubscriptionClosed",
]


def __getattr__(name: str):
    """Lazy import of the async surface.

    Why lazy: `AsyncClient` depends on `httpx`, which is an
    optional extra. A user who does `pip install agentdb` (no
    extra) and `from agentdb import Client` shouldn't trigger
    an import error from never-touched async code. The error
    fires only when someone actually references AsyncClient,
    and the message points at the right install command.
    """
    if name in ("AsyncClient", "AsyncSession"):
        from . import _async_client

        return getattr(_async_client, name)
    raise AttributeError(f"module 'agentdb' has no attribute {name!r}")

__version__ = "0.1.0"
