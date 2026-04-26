"""Async AgentDB client.

Mirrors the sync `Client` API on `httpx.AsyncClient` so agent
code that's already async (LangChain, LlamaIndex, the Anthropic
and OpenAI SDKs, Mastra-style frameworks) can use AgentDB
without `run_in_executor` wrapping.

Usage:

    import asyncio
    from agentdb import AsyncClient

    async def main():
        client = AsyncClient("http://127.0.0.1:7878")
        try:
            session = client.session("agent-1", tenant="demo")
            result = await session.append(
                {"role": "user", "text": "hello"},
                fetch_last_n=5,
            )
            print(result.seq, [(e.seq, e.payload) for e in result.recent])
        finally:
            await client.aclose()

    asyncio.run(main())

Or with `async with`:

    async with AsyncClient("http://127.0.0.1:7878") as client:
        ...

Imported lazily from `agentdb` only when the user actually
references `AsyncClient` — that way `pip install agentdb`
without the `[async]` extra still imports cleanly; the helpful
error fires only at first use.
"""

from __future__ import annotations

import dataclasses
from typing import Any, List, Optional

try:
    import httpx
except ImportError as e:
    raise ImportError(
        "AgentDB's async client requires the `httpx` extra. Install with:\n"
        "    pip install 'agentdb[async]'\n"
        "(or `pip install httpx` if you want to manage the dep yourself.)"
    ) from e

from .client import AppendResult, Event
from .errors import AgentDBError, from_response


_SESSION_PATH = "/api/sessions/{sid}/append_and_fetch"
_TENANT_PATH = "/api/tenants/{tid}/sessions/{sid}/append_and_fetch"
_FORK_PATH = "/api/sessions/{sid}/fork"
_HEALTH_PATH = "/health/ready"


class AsyncClient:
    """Async counterpart of [`Client`]. Same constructor shape;
    every I/O method is a coroutine.

    Construct it once per app and reuse — the underlying
    `httpx.AsyncClient` keeps a connection pool. Use
    `async with` (or call `aclose()` explicitly) so the pool
    drains cleanly on shutdown.
    """

    def __init__(
        self,
        base_url: str,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        tenant: Optional[str] = None,
        timeout: float = 30.0,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        if user is not None and password is None:
            raise ValueError("`password` is required when `user` is set")
        if password is not None and user is None:
            raise ValueError("`user` is required when `password` is set")
        self.base_url = base_url.rstrip("/")
        self._auth = (user, password) if user is not None else None
        self._tenant = tenant
        self.timeout = timeout
        # Allow injection for tests / instrumentation. When the
        # caller supplies one, we don't own it — they're
        # responsible for closing.
        self._owns_client = client is None
        self._http: httpx.AsyncClient = client or httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
        )

    @property
    def authenticated(self) -> bool:
        return self._auth is not None

    def session(
        self, session_id: str, *, tenant: Optional[str] = None
    ) -> "AsyncSession":
        return AsyncSession(self, session_id, tenant=tenant or self._tenant)

    async def health_ready(self) -> bool:
        try:
            resp = await self._http.get(_HEALTH_PATH)
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def aclose(self) -> None:
        if self._owns_client:
            await self._http.aclose()

    # --- async context manager support ---

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.aclose()

    # --- internal ---

    async def _request(
        self, method: str, path: str, *, json: Optional[dict] = None
    ) -> dict:
        kwargs: dict = {}
        if self._auth is not None:
            kwargs["auth"] = self._auth
        if json is not None:
            kwargs["json"] = json
        resp = await self._http.request(method, path, **kwargs)
        return _decode(resp)


def _decode(resp: httpx.Response) -> dict:
    body: Any
    try:
        body = resp.json()
    except (ValueError, TypeError):
        body = resp.text
    if 200 <= resp.status_code < 300:
        if isinstance(body, dict):
            return body
        raise AgentDBError(
            f"AgentDB returned 2xx with non-JSON body: {body!r}",
            status_code=resp.status_code,
            body=body,
        )
    raise from_response(resp.status_code, body)


@dataclasses.dataclass
class AsyncSession:
    """Handle to one session under [`AsyncClient`]. Don't construct
    directly — use `client.session(id)`."""

    _client: AsyncClient
    session_id: str
    tenant: Optional[str]

    def __init__(
        self, client: AsyncClient, session_id: str, *, tenant: Optional[str]
    ) -> None:
        self._client = client
        self.session_id = session_id
        self.tenant = tenant

    def __repr__(self) -> str:
        t = f", tenant={self.tenant!r}" if self.tenant else ""
        return f"AsyncSession(id={self.session_id!r}{t})"

    async def append(
        self,
        event: Any,
        *,
        fetch_last_n: int = 0,
        actor_id: Optional[int] = None,
    ) -> AppendResult:
        body: dict = {"event": event, "fetch_last_n": fetch_last_n}
        if actor_id is not None:
            body["actor_id"] = actor_id
        path = self._append_path()
        d = await self._client._request("POST", path, json=body)
        return AppendResult.from_dict(d)

    async def fork(self, child_session_id: str) -> "AsyncSession":
        body: dict = {"new_session_id": child_session_id}
        if self.tenant is not None and not self._client.authenticated:
            body["tenant_id"] = self.tenant
        await self._client._request(
            "POST", _FORK_PATH.format(sid=_quote(self.session_id)), json=body
        )
        return AsyncSession(
            self._client, child_session_id, tenant=self.tenant
        )

    def _append_path(self) -> str:
        if self._client.authenticated and self.tenant is None:
            return _SESSION_PATH.format(sid=_quote(self.session_id))
        if self.tenant is None:
            raise ValueError(
                "`tenant` must be set on the AsyncClient or AsyncSession when "
                "no auth credentials are configured. Pass tenant='demo' (or "
                "your actual tenant id) to AsyncClient(...) or .session(...)."
            )
        return _TENANT_PATH.format(
            tid=_quote(self.tenant), sid=_quote(self.session_id)
        )


def _quote(s: str) -> str:
    from urllib.parse import quote

    return quote(s, safe="-_.~")


# Make the public Event / AppendResult types importable from
# the async submodule too — handy for type hints in async code.
__all__ = ["AsyncClient", "AsyncSession", "AppendResult", "Event"]
