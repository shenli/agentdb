"""Async AgentDB client.

Mirrors the sync `Client` API on `httpx.AsyncClient` so agent
code that's already async (LangChain, LlamaIndex, the Anthropic
and OpenAI SDKs, Mastra-style frameworks) can use AgentDB
without `run_in_executor` wrapping.

Usage:

    import asyncio
    from agentdb import AsyncClient

    async def main():
        async with AsyncClient(
            base_url="http://127.0.0.1:7878",
            auth=("alice", "hunter2"),
        ) as client:
            session = client.session("agent-1")
            result = await session.append({"role": "user", "text": "hello"})
            print(result.seq, [(e.seq, e.payload) for e in result.recent])

    asyncio.run(main())

Tenant is derived from auth credentials server-side. No `tenant`
arg on the client or per-method calls.

Auth model:
  - Session/subscribe routes: HTTP Basic auth via `auth=("user", "password")`.
  - Channel routes: JWT Bearer auth via `token="..."` or `get_token=...`.
  - Both can be supplied simultaneously.

Imported lazily from `agentdb` only when the user actually
references `AsyncClient` — that way `pip install agentdb`
without the `[async]` extra still imports cleanly; the helpful
error fires only at first use.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any, AsyncIterator, Awaitable, Callable, List, Optional, Sequence, Union

try:
    import httpx
except ImportError as e:
    raise ImportError(
        "AgentDB's async client requires the `httpx` extra. Install with:\n"
        "    pip install 'agentdb[async]'\n"
        "(or `pip install httpx` if you want to manage the dep yourself.)"
    ) from e

from ._broadcast import BroadcastMessage
from ._sse import parse_lines_async
from .client import AppendResult, Event
from .errors import AgentDBError, SubscriptionClosed, from_response


_APPEND_PATH = "/api/sessions/{sid}/append_and_fetch"
_FETCH_RECENT_PATH = "/api/sessions/{sid}/fetch_recent"
_FORK_PATH = "/api/sessions/{sid}/fork"
_HEALTH_PATH = "/health/ready"
_TENANT_SUB_PATH = "/api/subscribe"
_SESSION_SUB_PATH = "/api/sessions/{sid}/subscribe"
_CHANNEL_PUBLISH_PATH = "/api/channels/{channel}/publish"
_CHANNEL_SUB_PATH = "/api/channels/{channel}/subscribe"


# A token provider may be sync (returns a string) or async (returns
# a coroutine). The latter fits frameworks where token rotation
# itself requires I/O — e.g. an OAuth refresh against a separate
# auth server.
TokenProvider = Callable[[], Union[str, Awaitable[str]]]


class AsyncClient:
    """Async counterpart of [`Client`]. Same constructor shape;
    every I/O method is a coroutine.

    Construct it once per app and reuse — the underlying
    `httpx.AsyncClient` keeps a connection pool. Use
    `async with` (or call `aclose()` explicitly) so the pool
    drains cleanly on shutdown.

    Auth model:
      - `auth=("user", "password")` — HTTP Basic auth for session/subscribe routes.
      - `token="..."` or `get_token=...` — JWT Bearer for channel routes.
    """

    def __init__(
        self,
        base_url: str = "",
        *,
        auth: Optional[tuple] = None,           # (user, password) for Basic
        token: Optional[str] = None,             # static JWT for channel routes
        get_token: Optional[TokenProvider] = None,  # rotating JWT provider
        timeout: float = 30.0,
        _client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        if token is not None and get_token is not None:
            raise ValueError("pass `token` or `get_token`, not both")
        self.base_url = base_url.rstrip("/")
        self._auth = auth  # tuple (user, password) or None
        self._token = token
        self._get_token = get_token
        self.timeout = timeout
        # Allow injection for tests / instrumentation. When the
        # caller supplies one, we don't own it — they're
        # responsible for closing.
        self._owns_client = _client is None
        self._http: httpx.AsyncClient = _client or httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
        )

    @property
    def authenticated(self) -> bool:
        """True when Basic-auth credentials are configured."""
        return self._auth is not None

    def session(self, session_id: str) -> "AsyncSession":
        """Open a handle to one session. Tenant is derived from auth server-side."""
        return AsyncSession(self, session_id)

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

    async def subscribe(
        self,
        *,
        session: Optional[Sequence[str]] = None,
        kind: Optional[Sequence[str]] = None,
        from_seq: Optional[int] = None,
        from_seq_per_session: Optional[dict] = None,
    ) -> AsyncIterator[Event]:
        """Tenant-wide async event subscription.

        Yields envelopes from `/api/subscribe`. Tenant is derived from
        the Basic-auth user's record server-side.
        Use `async for evt in client.subscribe(...)`.
        """
        path = _TENANT_SUB_PATH
        async for evt in self._subscribe_impl(
            path=path,
            session=list(session) if session else None,
            kind=list(kind) if kind else None,
            from_seq=from_seq,
            from_seq_per_session=from_seq_per_session,
        ):
            yield evt

    async def broadcast(
        self,
        *,
        channel: str,
        payload: Any,
    ) -> None:
        """Async counterpart of [`Client.broadcast`].

        Publish an ephemeral broadcast to a channel — no durability,
        no replay. Use the per-session `append` + `subscribe` pair
        when you need durability. Tenant is resolved server-side via JWT.
        """
        path = _CHANNEL_PUBLISH_PATH.format(channel=_quote(channel))
        kwargs: dict = {"json": payload}
        headers: dict = {}
        token = await self._bearer_token()
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if headers:
            kwargs["headers"] = headers
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        resp = await self._http.request("POST", path, **kwargs)
        if resp.status_code == 204:
            return
        _ = _decode(resp)

    async def subscribe_channel(
        self,
        *,
        channel: str,
        no_echo: bool = False,
    ) -> AsyncIterator["BroadcastMessage"]:
        """Async counterpart of [`Client.subscribe_channel`].

        No JWT auto-reconnect: broadcast is ephemeral by design, so
        on disconnect the iterator ends rather than recovering.
        """
        path = _CHANNEL_SUB_PATH.format(channel=_quote(channel))
        params: List[tuple] = []
        if no_echo:
            params.append(("no_echo", "true"))
        headers: dict = {"Accept": "text/event-stream"}
        token = await self._bearer_token()
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        kwargs: dict = {"params": params, "headers": headers}
        kwargs["timeout"] = httpx.Timeout(self.timeout, read=None)
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        async with self._http.stream("GET", path, **kwargs) as resp:
            if resp.status_code != 200:
                await resp.aread()
                _ = _decode(resp)
            async for frame in parse_lines_async(resp.aiter_lines()):
                if frame.event == "subscribed":
                    continue
                if not frame.data:
                    continue
                env = json.loads(frame.data)
                yield BroadcastMessage.from_envelope(env)

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

    async def _bearer_token(self) -> Optional[str]:
        """Fetch the current bearer token, awaiting `get_token` when
        it returns a coroutine. Called fresh on each reconnect."""
        if self._get_token is None:
            return self._token
        result = self._get_token()
        if hasattr(result, "__await__"):
            return await result  # type: ignore[no-any-return]
        return result  # type: ignore[return-value]

    async def _subscribe_impl(
        self,
        *,
        path: str,
        session: Optional[List[str]] = None,
        kind: Optional[List[str]] = None,
        from_seq: Optional[int] = None,
        from_seq_per_session: Optional[dict] = None,
    ) -> AsyncIterator[Event]:
        """Async connect-and-stream loop with JWT-expiry auto-resume."""
        cursor = from_seq
        per_session_cursor = (
            dict(from_seq_per_session) if from_seq_per_session else None
        )
        while True:
            last_seen: Optional[int] = None
            try:
                async for evt in self._stream_once(
                    path=path,
                    session=session,
                    kind=kind,
                    from_seq=cursor,
                    from_seq_per_session=per_session_cursor,
                ):
                    last_seen = evt.seq
                    if per_session_cursor is not None and evt.session:
                        per_session_cursor[evt.session] = evt.seq
                    yield evt
                return
            except SubscriptionClosed as e:
                if e.error_code == "jwt_expired" and (
                    self._get_token is not None or self._token is not None
                ):
                    if e.last_seq is not None:
                        cursor = e.last_seq + 1
                    elif last_seen is not None:
                        cursor = last_seen + 1
                    continue
                raise

    async def _stream_once(
        self,
        *,
        path: str,
        session: Optional[List[str]],
        kind: Optional[List[str]],
        from_seq: Optional[int],
        from_seq_per_session: Optional[dict],
    ) -> AsyncIterator[Event]:
        params: List[tuple] = []
        if from_seq is not None:
            params.append(("from_seq", str(from_seq)))
        if from_seq_per_session:
            joined = ",".join(
                f"{sid}:{seq}" for sid, seq in from_seq_per_session.items()
            )
            params.append(("from_seq_per_session", joined))
        for s in session or []:
            params.append(("session", s))
        for k in kind or []:
            params.append(("kind", k))
        headers: dict = {"Accept": "text/event-stream"}
        token = await self._bearer_token()
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        kwargs: dict = {"params": params, "headers": headers}
        kwargs["timeout"] = httpx.Timeout(self.timeout, read=None)
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        async with self._http.stream("GET", path, **kwargs) as resp:
            if resp.status_code != 200:
                await resp.aread()
                _ = _decode(resp)
            async for frame in parse_lines_async(resp.aiter_lines()):
                if frame.event == "subscribed":
                    continue
                if frame.event == "error":
                    body = json.loads(frame.data) if frame.data else {}
                    raise SubscriptionClosed(
                        body.get("error", "unknown"),
                        last_seq=body.get("last_seq"),
                    )
                if not frame.data:
                    continue
                yield Event.from_dict(json.loads(frame.data))


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

    def __init__(self, client: AsyncClient, session_id: str) -> None:
        self._client = client
        self.session_id = session_id

    def __repr__(self) -> str:
        return f"AsyncSession(id={self.session_id!r})"

    async def append(
        self,
        payload: Any = None,
        *,
        event: Any = None,
        fetch_last_n: int = 0,
        actor_id: Optional[int] = None,
    ) -> AppendResult:
        """Append one event. `payload` (or `event` for backward compat) is
        any JSON-serializable value."""
        data = payload if payload is not None else event
        body: dict = {"event": data, "fetch_last_n": fetch_last_n}
        if actor_id is not None:
            body["actor_id"] = actor_id
        path = _APPEND_PATH.format(sid=_quote(self.session_id))
        d = await self._client._request("POST", path, json=body)
        return AppendResult.from_dict(d)

    async def fetch_recent(self, *, last_n: int = 10) -> List[Event]:
        """Read the last N events without writing. Pure read."""
        path = _FETCH_RECENT_PATH.format(sid=_quote(self.session_id))
        d = await self._client._request("POST", path, json={"last_n": last_n})
        return [Event.from_dict(e) for e in d.get("events") or []]

    async def subscribe(
        self,
        *,
        kind: Optional[Sequence[str]] = None,
        from_seq: Optional[int] = None,
    ) -> AsyncIterator[Event]:
        """Stream events for this session via SSE (async).

        Mirrors the sync `Session.subscribe`. JWT-expiry auto-
        reconnect, kind filter, and `from_seq` replay all behave
        the same.
        """
        path = _SESSION_SUB_PATH.format(sid=_quote(self.session_id))
        async for evt in self._client._subscribe_impl(
            path=path,
            kind=list(kind) if kind else None,
            from_seq=from_seq,
        ):
            yield evt

    async def fork(self, child_session_id: str) -> "AsyncSession":
        """Fork this session into a child. Returns an `AsyncSession` handle."""
        body: dict = {"new_session_id": child_session_id}
        await self._client._request(
            "POST", _FORK_PATH.format(sid=_quote(self.session_id)), json=body
        )
        return AsyncSession(self._client, child_session_id)

    def _subscribe_path(self) -> str:
        """Return the SSE subscribe URL for this session."""
        return _SESSION_SUB_PATH.format(sid=_quote(self.session_id))


def _quote(s: str) -> str:
    from urllib.parse import quote

    return quote(s, safe="-_.~")


# Make the public Event / AppendResult types importable from
# the async submodule too — handy for type hints in async code.
__all__ = ["AsyncClient", "AsyncSession", "AppendResult", "Event"]
