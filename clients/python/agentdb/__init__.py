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

See `examples/agents/python/quickstart.py` for a working agent loop.
"""

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
)

__all__ = [
    "Client",
    "Session",
    "AppendResult",
    "Event",
    "AgentDBError",
    "BadRequest",
    "Unauthorized",
    "Forbidden",
    "NotFound",
    "Conflict",
    "RateLimited",
    "ServerError",
    "Misdirected",
]

__version__ = "0.1.0"
