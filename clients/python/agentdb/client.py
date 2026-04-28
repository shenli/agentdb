"""High-level AgentDB client.

Wraps the REST surface in a small object graph that keeps the
common agent flow short:

    client = Client(base_url="http://127.0.0.1:7878", auth=("alice", "hunter2"))
    session = client.session("agent-1")
    session.append({"role": "user", "text": "..."}, fetch_last_n=5)
    session.fork("agent-1-branch-a")

Tenant is derived from the Basic-auth credentials server-side.
No `tenant` arg on the client or per-method calls.

Auth model:
  - Session/subscribe routes: HTTP Basic auth via `auth=("user", "password")`.
  - Channel routes: JWT Bearer auth via `token="..."` or `get_token=lambda: "..."`.
  - If both are supplied, Basic auth is used for session/subscribe and JWT
    for channel routes.
  - If neither is supplied, requests are unauthenticated (server returns 401).

The client is **synchronous** and uses `requests`. For async agent code
install the `[async]` extra and use `AsyncClient` instead.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any, Callable, Iterable, Iterator, List, Optional, Sequence

import requests

from ._broadcast import BroadcastMessage
from ._sse import parse_lines
from .errors import AgentDBError, SubscriptionClosed, from_response


@dataclasses.dataclass
class Event:
    """A single event read back from AgentDB.

    The shape mirrors the server's `EventDto` for `append_and_fetch`
    and the broader `EventEnvelope` shape for `subscribe()` streams.
    `payload` is the arbitrary JSON the agent appended; the rest is
    server-assigned metadata (sequence number, wall-clock timestamp,
    optional actor id for multi-actor sessions).

    The `tenant` / `session` / `kind` fields populate when the event
    came from `subscribe()` (the SSE stream emits the full envelope
    so a tenant-wide subscriber can demultiplex). For `append`
    read-backs the server only echoes the per-session shape and
    those fields stay `None`.
    """

    seq: int
    ts_ms: int
    payload: Any
    actor_id: Optional[int] = None
    tenant: Optional[str] = None
    session: Optional[str] = None
    kind: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        return cls(
            seq=d["seq"],
            ts_ms=d.get("ts_ms") or d.get("ts") or 0,
            payload=d.get("payload"),
            actor_id=d.get("actor_id"),
            tenant=d.get("tenant"),
            session=d.get("session"),
            kind=d.get("kind"),
        )


@dataclasses.dataclass
class AppendResult:
    """Return shape of `Session.append`. `seq` is the new event's
    sequence number; `recent` is the last-N read-back when
    `fetch_last_n > 0` (empty list otherwise)."""

    seq: int
    recent: List[Event]

    @classmethod
    def from_dict(cls, d: dict) -> "AppendResult":
        return cls(
            seq=d["seq"],
            recent=[Event.from_dict(e) for e in d.get("recent") or []],
        )


class Client:
    """An AgentDB connection.

    `base_url` is the REST endpoint, e.g. `http://127.0.0.1:7878`.

    Auth model:
      - `auth=("user", "password")` — HTTP Basic auth for session/subscribe
        routes. Tenant is derived from the user record server-side.
      - `token="..."` or `get_token=lambda: "..."` — JWT Bearer auth for
        channel routes (broadcast / subscribe_channel). `get_token` is the
        refreshable form: called on every reconnect so a server-side
        `jwt_expired` close recovers transparently.
      - Both can be supplied simultaneously: Basic auth covers
        session/subscribe, JWT covers channel routes.
      - If neither is supplied, requests go unauthenticated (server returns
        401 when auth is required).

    `timeout` defaults to a per-request 30s; tune it down for
    interactive agent work where you'd rather fail fast and retry.
    """

    def __init__(
        self,
        base_url: str = "",
        *,
        auth: Optional[tuple] = None,           # (user, password) for Basic
        token: Optional[str] = None,             # JWT for channel routes
        get_token: Optional[Callable[[], str]] = None,  # rotating JWT provider
        timeout: float = 30.0,
        _http: Optional[requests.Session] = None,
    ) -> None:
        if token is not None and get_token is not None:
            raise ValueError("pass `token` or `get_token`, not both")
        self.base_url = base_url.rstrip("/")
        self._auth = auth  # tuple (user, password) or None
        self._token = token
        self._get_token = get_token
        self.timeout = timeout
        self._http = _http or requests.Session()

    @property
    def authenticated(self) -> bool:
        """True when Basic-auth credentials are configured."""
        return self._auth is not None

    def session(self, session_id: str) -> "Session":
        """Open a handle to one session.

        Tenant is derived from auth credentials server-side — no `tenant`
        arg needed. The v2 routes (`/api/sessions/{id}/...`) carry no
        tenant in the URL; the server resolves it from the `Authorization`
        header.
        """
        return Session(self, session_id)

    def health_ready(self) -> bool:
        """Probe `/health/ready`. Returns True on 200, False
        otherwise. Useful for waiting on a server that's still
        bootstrapping (dev-mode test harnesses, CI smoke tests)."""
        try:
            resp = self._http.get(
                f"{self.base_url}/health/ready", timeout=self.timeout
            )
            return resp.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def subscribe(
        self,
        *,
        session: Optional[Sequence[str]] = None,
        kind: Optional[Sequence[str]] = None,
        from_seq: Optional[int] = None,
        from_seq_per_session: Optional[dict] = None,
    ) -> Iterator["Event"]:
        """Tenant-wide event subscription.

        Yields envelopes from `/api/subscribe` — every appended event in
        the tenant (derived from the Basic-auth user's record), optionally
        filtered by session id (`session=[...]`) and/or kind (`kind=[...]`).

        On JWT expiry the connection is transparently refreshed via
        `get_token` (when configured) and resumed gap-free.

        Use this for fan-out reactors that watch many sessions —
        e.g. an indexer or audit log. For one-session tailing the
        per-session form is cheaper:

            session.subscribe()
        """
        path = "/api/subscribe"
        return self._subscribe_impl(
            path=path,
            session=list(session) if session else None,
            kind=list(kind) if kind else None,
            from_seq=from_seq,
            from_seq_per_session=from_seq_per_session,
        )

    def broadcast(
        self,
        *,
        channel: str,
        payload: Any,
    ) -> None:
        """Publish an ephemeral broadcast message to a channel.

        Phase 2 pub/sub. Broadcast is **ephemeral** — no durability,
        no replay, no per-subscriber acks. Use it for transient
        signals like typing indicators, cursor positions, or
        agent-status pings. If you need durability, use
        `Session.append` + `Session.subscribe()` on a session instead.

        `channel` must match `^[a-zA-Z0-9._:-]{1,256}$` — the server
        rejects malformed names with `channel_name_invalid`.
        `payload` is any JSON-serializable value.

        Tenant resolution is server-side via the JWT claim (Bearer token).
        Returns `None` on success (HTTP 204).
        """
        path = f"/api/channels/{_quote(channel)}/publish"
        kwargs: dict = {"timeout": self.timeout, "json": payload}
        headers: dict = {}
        token = self._bearer_token()
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if headers:
            kwargs["headers"] = headers
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        resp = self._http.request(
            "POST", f"{self.base_url}{path}", **kwargs
        )
        if resp.status_code == 204:
            return
        # Non-204 → decode + raise typed error.
        _ = _decode(resp)

    def subscribe_channel(
        self,
        *,
        channel: str,
        no_echo: bool = False,
    ) -> Iterator["BroadcastMessage"]:
        """Subscribe to a channel; yield [`BroadcastMessage`] objects.

        Counterpart to [`broadcast`]. The iterator runs until the
        caller breaks out of the loop or the server closes the
        connection.

        Unlike per-session [`subscribe`], there is **no JWT
        auto-reconnect on expiry** — broadcast is ephemeral by
        design (spec § 5.5), so loss on disconnect is acceptable.
        Callers that need durability use `Session.subscribe()`
        instead.

        `no_echo=True` asks the server to drop envelopes whose
        `sender` matches this subscriber's JWT `sub` claim. Only
        effective with JWT auth — Basic-auth publishers emit
        `sender: None` and never match.
        """
        path = f"/api/channels/{_quote(channel)}/subscribe"
        params: List[tuple] = []
        if no_echo:
            params.append(("no_echo", "true"))
        headers: dict = {"Accept": "text/event-stream"}
        token = self._bearer_token()
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        kwargs: dict = {
            "params": params,
            "headers": headers,
            "stream": True,
            "timeout": (self.timeout, None),
        }
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        resp = self._http.request(
            "GET", f"{self.base_url}{path}", **kwargs
        )
        try:
            if resp.status_code != 200:
                _ = _decode(resp)
            for frame in parse_lines(resp.iter_lines(decode_unicode=True)):
                if frame.event == "subscribed":
                    continue
                if not frame.data:
                    continue
                env = json.loads(frame.data)
                yield BroadcastMessage.from_envelope(env)
        finally:
            resp.close()

    # --- internal helpers ---

    def _request(
        self, method: str, path: str, *, json: Optional[dict] = None
    ) -> dict:
        """Issue a request and return the parsed JSON body, or
        raise a typed `AgentDBError`. Centralizes auth + error
        handling so handlers stay terse."""
        kwargs: dict = {"timeout": self.timeout}
        if self._auth is not None:
            kwargs["auth"] = self._auth
        if json is not None:
            kwargs["json"] = json
        resp = self._http.request(method, f"{self.base_url}{path}", **kwargs)
        return _decode(resp)

    def _bearer_token(self) -> Optional[str]:
        """Return the current JWT, calling `get_token` if configured.

        Each channel reconnect calls this fresh so a token rotation
        between attempts comes through without restarting the iterator.
        """
        if self._get_token is not None:
            return self._get_token()
        return self._token

    def _subscribe_impl(
        self,
        *,
        path: str,
        session: Optional[List[str]] = None,
        kind: Optional[List[str]] = None,
        from_seq: Optional[int] = None,
        from_seq_per_session: Optional[dict] = None,
    ) -> Iterator["Event"]:
        """Connect-and-stream loop with JWT-expiry auto-resume.

        On a `jwt_expired` close frame, refreshes the token and
        reconnects with `from_seq = last_seen + 1` (or the server-
        reported `last_seq + 1` if it gave one), so the consumer
        never sees a dropped event. Other terminal frames
        (`lag_too_far`, `catchup_failed`) bubble up as
        [`SubscriptionClosed`] for the caller to handle.
        """
        cursor = from_seq
        per_session_cursor = (
            dict(from_seq_per_session) if from_seq_per_session else None
        )
        while True:
            last_seen: Optional[int] = None
            try:
                for evt in self._stream_once(
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
                return  # server closed cleanly
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

    def _stream_once(
        self,
        *,
        path: str,
        session: Optional[List[str]],
        kind: Optional[List[str]],
        from_seq: Optional[int],
        from_seq_per_session: Optional[dict],
    ) -> Iterator["Event"]:
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
        token = self._bearer_token()
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        kwargs: dict = {
            "params": params,
            "headers": headers,
            "stream": True,
            "timeout": (self.timeout, None),
        }
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        resp = self._http.request(
            "GET", f"{self.base_url}{path}", **kwargs
        )
        try:
            if resp.status_code != 200:
                _ = _decode(resp)
            for frame in parse_lines(resp.iter_lines(decode_unicode=True)):
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
        finally:
            resp.close()


def _decode(resp: requests.Response) -> dict:
    body: Any
    try:
        body = resp.json()
    except ValueError:
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


class Session:
    """A single AgentDB session.

    Construct via `Client.session(session_id)`. The session is a thin
    handle — every method makes one round trip to the server.
    No client-side caching of events.

    Tenant is derived from auth credentials server-side; no `tenant`
    field or constructor arg is present in v2.
    """

    def __init__(
        self,
        client: Client,
        session_id: str,
    ) -> None:
        self._client = client
        self.session_id = session_id

    def __repr__(self) -> str:
        return f"Session(id={self.session_id!r})"

    def append(
        self,
        payload: Any = None,
        *,
        event: Any = None,
        fetch_last_n: int = 0,
        actor_id: Optional[int] = None,
    ) -> AppendResult:
        """Append one event and (optionally) read back the last N.

        `payload` (or `event` for backward compatibility) is any
        JSON-serializable value — the server stores it in the event's
        `payload` field unchanged.
        `fetch_last_n=0` skips the read-back and the response's
        `recent` list comes back empty (cheaper for hot writes).
        `actor_id` is an optional small integer for multi-actor
        sessions (e.g. user vs tool); leave `None` if unused.
        """
        # Accept both `payload=` (new) and `event=` (old positional) to ease migration.
        data = payload if payload is not None else event
        body: dict = {"event": data, "fetch_last_n": fetch_last_n}
        if actor_id is not None:
            body["actor_id"] = actor_id
        path = f"/api/sessions/{_quote(self.session_id)}/append_and_fetch"
        d = self._client._request("POST", path, json=body)
        return AppendResult.from_dict(d)

    def fetch_recent(self, *, last_n: int = 10) -> List[Event]:
        """Read the last N events without writing. Pure read.

        Uses `POST /api/sessions/{s}/fetch_recent`. Returns an empty
        list when the session is empty or doesn't exist yet.
        """
        path = f"/api/sessions/{_quote(self.session_id)}/fetch_recent"
        d = self._client._request("POST", path, json={"last_n": last_n})
        return [Event.from_dict(e) for e in d.get("events") or []]

    def subscribe(
        self,
        *,
        kind: Optional[Sequence[str]] = None,
        from_seq: Optional[int] = None,
    ) -> Iterator["Event"]:
        """Stream events for this session via SSE.

        Yields [`Event`] objects as they arrive. With `from_seq` the
        server replays from that sequence number first (catch-up)
        and then transitions to live tail without dropping events.
        With `kind=[...]` the server filters out non-matching
        events before they hit the wire.

        Behaviour:

        - The iterator runs until the caller breaks out of the loop
          or the server closes cleanly.
        - On a JWT expiry close frame the client transparently
          refreshes the token (via `get_token` on the parent
          [`Client`]) and reconnects with `from_seq = last_seen + 1`.
        - Other terminal close frames raise [`SubscriptionClosed`]
          with the server-reported `error_code`.
        """
        path = self._subscribe_path()
        return self._client._subscribe_impl(
            path=path,
            kind=list(kind) if kind else None,
            from_seq=from_seq,
        )

    def fork(self, child_session_id: str) -> "Session":
        """Fork this session into a child that inherits the
        parent's events at fork time. Returns a `Session` handle
        for the new id.

        The server populates the child's locator before
        returning, so subsequent reads/writes against the child
        route correctly without a follow-up touch.
        """
        body = {"new_session_id": child_session_id}
        self._client._request(
            "POST",
            f"/api/sessions/{_quote(self.session_id)}/fork",
            json=body,
        )
        return Session(self._client, child_session_id)

    def _subscribe_path(self) -> str:
        """Return the SSE subscribe URL for this session."""
        return f"/api/sessions/{_quote(self.session_id)}/subscribe"


def _quote(s: str) -> str:
    """Path-segment escape. Keeps ASCII letters/digits/-_ as-is
    (which is the bulk of well-formed session ids) and percent-
    encodes everything else."""
    from urllib.parse import quote

    return quote(s, safe="-_.~")


def append_one(
    base_url: str,
    *,
    session_id: str,
    event: Any = None,
    payload: Any = None,
    auth: Optional[tuple] = None,
    fetch_last_n: int = 0,
) -> AppendResult:
    """One-line helper for the most common operation: append a
    single event. Constructs a `Client`, opens a session, calls
    `append`, and returns the result. Use the `Client` class
    directly when you want to keep a connection pooled across
    many events.
    """
    data = payload if payload is not None else event
    client = Client(base_url, auth=auth)
    return client.session(session_id).append(data, fetch_last_n=fetch_last_n)
