"""Python client for AgentDB.

Typical usage in auth mode:

    from agentdb import Client

    client = Client(
        base_url="http://127.0.0.1:7878",
        auth=("alice", "hunter2"),
    )
    session = client.session("agent-1")
    result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
    print(result.seq, [e.payload for e in result.recent])

The `append + fetch_last_n` pattern is the canonical hot path —
write one event and read the last N back in the same round trip.
Tenant is derived from the auth credentials server-side; no
`tenant` arg is needed anywhere.

For async code (LangChain, LlamaIndex, the Anthropic / OpenAI
SDKs, etc.) install the optional `[async]` extra and use
`AsyncClient`:

    pip install 'agentdb[async]'

    import asyncio
    from agentdb import AsyncClient

    async def main():
        async with AsyncClient(
            base_url="http://127.0.0.1:7878",
            auth=("alice", "hunter2"),
        ) as client:
            session = client.session("agent-1")
            result = await session.append(
                {"role": "user", "text": "hi"}, fetch_last_n=5
            )
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

__version__ = "1.0.0"
