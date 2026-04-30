"""Python client for AgentDB V2.

Typical usage:

    from agentdb import Client

    client = Client("http://127.0.0.1:8080")
    tenant = client.create_tenant("acme")
    agent = client.create_agent(tenant["tenant_id"])
    session = client.create_session(tenant["tenant_id"], agent["agent_id"])
    result = session.append({"role": "user", "text": "hello"}, fetch_last_n=5)
    print(result.seq, [e.payload for e in result.recent])

The `append + fetch_last_n` pattern is the canonical hot path —
write one event and read the last N back in the same round trip.
For async code (LangChain, LlamaIndex, the Anthropic / OpenAI
SDKs, etc.) install the optional `[async]` extra and use
`AsyncClient`:

    pip install 'agentdb[async]'

    import asyncio
    from agentdb import AsyncClient

    async def main():
        async with AsyncClient("http://127.0.0.1:8080") as client:
            tenant = await client.create_tenant("acme-async")
            agent = await client.create_agent(tenant["tenant_id"])
            session = await client.create_session(tenant["tenant_id"], agent["agent_id"])
            result = await session.append(
                {"role": "user", "text": "hi"}, fetch_last_n=5
            )
            print(result.seq)

    asyncio.run(main())

See `examples/agents/python/quickstart.py` (sync) and
`examples/agents/python/async_quickstart.py` (async) for
working agent loops.
"""

from .client import Client, Session, AppendResult, Event, WatchItem
from .conventions import (
    SessionEventKind,
    agent_status,
    approval_requested,
    approval_result,
    artifact_created,
    context_pack_built,
    model_message,
    model_thought,
    tool_call,
    tool_error,
    tool_result,
    user_message,
)
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
    "WatchItem",
    "SessionEventKind",
    "agent_status",
    "approval_requested",
    "approval_result",
    "artifact_created",
    "context_pack_built",
    "model_message",
    "model_thought",
    "tool_call",
    "tool_error",
    "tool_result",
    "user_message",
    "AsyncClient",
    "AsyncSession",
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

__version__ = "2.0.0"
