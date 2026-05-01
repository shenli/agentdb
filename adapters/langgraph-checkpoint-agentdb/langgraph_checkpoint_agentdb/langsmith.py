"""Factory helpers for LangSmith Agent Server custom backends."""

from __future__ import annotations

import contextlib
import os
from typing import AsyncIterator, Optional

from .checkpoint import AgentDBAsyncSaver
from .store import AgentDBAsyncStore


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required for the AgentDB LangSmith adapter")
    return value


def _client_from_env():
    from agentdb import AsyncClient

    base_url = os.environ.get("AGENTDB_BASE_URL", "http://127.0.0.1:7878")
    token = os.environ.get("AGENTDB_TOKEN")
    user = os.environ.get("AGENTDB_BASIC_USER")
    password = os.environ.get("AGENTDB_BASIC_PASSWORD")
    auth: Optional[tuple[str, str]] = (user, password) if user and password else None
    return AsyncClient(base_url, token=token, auth=auth)


@contextlib.asynccontextmanager
async def generate_checkpointer() -> AsyncIterator[AgentDBAsyncSaver]:
    """Yield an AgentDB-backed LangGraph checkpointer for Agent Server.

    Environment:
    - `AGENTDB_BASE_URL`, default `http://127.0.0.1:7878`
    - `AGENTDB_TENANT_ID`, required
    - `AGENTDB_AGENT_ID`, required
    - `AGENTDB_TOKEN`, optional bearer token
    - `AGENTDB_BASIC_USER` / `AGENTDB_BASIC_PASSWORD`, optional Basic auth
    """

    client = _client_from_env()
    try:
        yield AgentDBAsyncSaver(
            client,
            tenant_id=_required_env("AGENTDB_TENANT_ID"),
            agent_id=_required_env("AGENTDB_AGENT_ID"),
        )
    finally:
        await client.aclose()


@contextlib.asynccontextmanager
async def generate_store() -> AsyncIterator[AgentDBAsyncStore]:
    """Yield an AgentDB-backed LangGraph store for Agent Server."""

    client = _client_from_env()
    try:
        yield AgentDBAsyncStore(client, tenant_id=_required_env("AGENTDB_TENANT_ID"))
    finally:
        await client.aclose()
