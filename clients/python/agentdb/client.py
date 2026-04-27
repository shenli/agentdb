"""High-level AgentDB client.

Wraps the REST surface in a small object graph that keeps the
common agent flow short:

    client = Client("http://127.0.0.1:7878")
    session = client.session("agent-1", tenant="demo")
    session.append({"role": "user", "text": "..."}, fetch_last_n=5)
    session.fork("agent-1-branch-a")

The client is **synchronous** and uses `requests`. An async
variant is a natural follow-up; agent code that's already async
can call this from a thread executor in the meantime.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any, Callable, Iterable, Iterator, List, Optional, Sequence

import requests

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
    `user` / `password` enable HTTP Basic auth; when both are
    provided, every request carries an `Authorization` header and
    the tenant is auth-derived. `tenant` is required only when
    auth is disabled (dev mode) — it's the legacy path-supplied
    tenant identity.

    `timeout` defaults to a per-request 30s; tune it down for
    interactive agent work where you'd rather fail fast and
    retry.
    """

    def __init__(
        self,
        base_url: str,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        tenant: Optional[str] = None,
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
        jwt: Optional[str] = None,
        get_token: Optional[Callable[[], str]] = None,
    ) -> None:
        if user is not None and password is None:
            raise ValueError("`password` is required when `user` is set")
        if password is not None and user is None:
            raise ValueError("`user` is required when `password` is set")
        if jwt is not None and get_token is not None:
            raise ValueError("pass `jwt` or `get_token`, not both")
        self.base_url = base_url.rstrip("/")
        self._auth = (user, password) if user is not None else None
        self._tenant = tenant
        self.timeout = timeout
        self._http = session or requests.Session()
        # `subscribe()` uses bearer-token auth (JWT) rather than HTTP
        # Basic — that's the spec's stance for the SSE endpoint and
        # what the browser-direct demo needs. `get_token` is the
        # refreshable form: invoked on every reconnect so a server-
        # side `jwt_expired` close can recover transparently.
        self._jwt = jwt
        self._get_token = get_token

    @property
    def authenticated(self) -> bool:
        return self._auth is not None

    def session(self, session_id: str, *, tenant: Optional[str] = None) -> "Session":
        """Open a handle to one session.

        Tenant resolution:

        - If `tenant` is given, it overrides the client default
          and uses the legacy `/api/tenants/{tid}/...` route.
        - Otherwise, if the client was created with auth, the
          server derives the tenant from credentials and the
          new `/api/sessions/{sid}/...` route is used.
        - Otherwise, the client default `tenant` (if set) is
          used; if neither is set, requests will fail with
          `BadRequest("cannot resolve tenant…")` from the
          server. Set one of them to silence that.
        """
        return Session(self, session_id, tenant=tenant or self._tenant)

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
        tenant: Optional[str] = None,
        session: Optional[Sequence[str]] = None,
        kind: Optional[Sequence[str]] = None,
        from_seq: Optional[int] = None,
        from_seq_per_session: Optional[dict] = None,
    ) -> Iterator["Event"]:
        """Tenant-wide event subscription.

        Yields envelopes from `/api/tenants/{t}/subscribe` — every
        appended event in the tenant, optionally filtered by
        session id (`session=[...]`) and/or kind (`kind=[...]`).

        Tenant resolution mirrors `client.session(...)`: pass
        `tenant=` explicitly, otherwise the client default is used.
        On JWT expiry the connection is transparently refreshed via
        `get_token` (when configured) and resumed gap-free.

        Use this for fan-out reactors that watch many sessions —
        e.g. an indexer or audit log. For one-session tailing the
        per-session form is cheaper:

            session.subscribe()
        """
        tid = tenant or self._tenant
        if tid is None:
            raise ValueError(
                "`tenant` must be set on Client(...) or passed to "
                "subscribe(tenant=...) — no auth-derived default for "
                "the tenant-wide subscribe endpoint."
            )
        path = f"/api/tenants/{_quote(tid)}/subscribe"
        return self._subscribe_impl(
            path=path,
            session=list(session) if session else None,
            kind=list(kind) if kind else None,
            from_seq=from_seq,
            from_seq_per_session=from_seq_per_session,
        )

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

        Each `subscribe()` reconnect calls this fresh so a token
        rotation between attempts comes through without restarting
        the iterator.
        """
        if self._get_token is not None:
            return self._get_token()
        return self._jwt

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
                    # For tenant-wide subscriptions, advance per-
                    # session cursors so reconnect-after-expiry
                    # doesn't replay events we already yielded.
                    if per_session_cursor is not None and evt.session:
                        per_session_cursor[evt.session] = evt.seq
                    yield evt
                return  # server closed cleanly
            except SubscriptionClosed as e:
                if e.error_code == "jwt_expired" and (
                    self._get_token is not None or self._jwt is not None
                ):
                    if e.last_seq is not None:
                        cursor = e.last_seq + 1
                    elif last_seen is not None:
                        cursor = last_seen + 1
                    # else: keep prior cursor (nothing seen yet).
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
        # `requests` doesn't have a true streaming-iterator API for
        # SSE on Basic auth, but `stream=True` plus `iter_lines`
        # gives us the same shape — and the SSE endpoint is the
        # only place the SDK uses streaming, so the cost stays
        # localised.
        kwargs: dict = {
            "params": params,
            "headers": headers,
            "stream": True,
            # No timeout on the read side: SSE connections are
            # long-lived; the caller controls cancellation by
            # closing the iterator.
            "timeout": (self.timeout, None),
        }
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        resp = self._http.request(
            "GET", f"{self.base_url}{path}", **kwargs
        )
        try:
            if resp.status_code != 200:
                # Decode + raise a typed error (BadRequest,
                # Unauthorized, …) so the subscribe path matches the
                # rest of the SDK's error contract.
                _ = _decode(resp)
            for frame in parse_lines(resp.iter_lines(decode_unicode=True)):
                if frame.event == "subscribed":
                    # Connection acknowledgement; ignore. (The
                    # `live_seq` in the data is informational —
                    # callers can race-test by inspecting it
                    # themselves once we expose the raw frames.)
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
    """A single AgentDB session, scoped to one tenant.

    Construct via `Client.session(...)`. The session is a thin
    handle — every method makes one round trip to the server.
    No client-side caching of events.
    """

    def __init__(
        self,
        client: Client,
        session_id: str,
        *,
        tenant: Optional[str],
    ) -> None:
        self._client = client
        self.session_id = session_id
        self.tenant = tenant

    def __repr__(self) -> str:
        t = f", tenant={self.tenant!r}" if self.tenant else ""
        return f"Session(id={self.session_id!r}{t})"

    def append(
        self,
        event: Any,
        *,
        fetch_last_n: int = 0,
        actor_id: Optional[int] = None,
    ) -> AppendResult:
        """Append one event and (optionally) read back the last N.

        `event` is any JSON-serializable value — the server
        stores it in the event's `payload` field unchanged.
        `fetch_last_n=0` skips the read-back and the response's
        `recent` list comes back empty (cheaper for hot writes).
        `actor_id` is an optional small integer for multi-actor
        sessions (e.g. user vs tool); leave `None` if unused.
        """
        body: dict = {"event": event, "fetch_last_n": fetch_last_n}
        if actor_id is not None:
            body["actor_id"] = actor_id
        path = self._append_path()
        d = self._client._request("POST", path, json=body)
        return AppendResult.from_dict(d)

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
          or the server closes cleanly. Use a thread (or `asyncio`
          via the async client) for fire-and-forget tailing.
        - On a JWT expiry close frame the client transparently
          refreshes the token (via `get_token` on the parent
          [`Client`]) and reconnects with `from_seq = last_seen +
          1`, so consumers never see a gap.
        - Other terminal close frames raise [`SubscriptionClosed`]
          with the server-reported `error_code` and (when the
          server provided one) `last_seq` so the caller can choose
          to reconnect from a fresher point.

        `tenant` resolution mirrors `append`: if the session was
        opened on an authenticated client without an override,
        the tenant comes from credentials; otherwise the explicit
        `tenant` is used.
        """
        if self.tenant is None and not self._client.authenticated:
            raise ValueError(
                "`tenant` must be set on the Client or Session when no auth "
                "credentials are configured. Pass tenant='demo' (or your "
                "actual tenant id) to Client(...) or Client.session(...)."
            )
        if self.tenant is None:
            # Auth-bound: server derives tenant. There's no
            # session-keyed subscribe route on main; the spec
            # routes pub/sub via the tenant-explicit form. Fall
            # back to the legacy path with the auth-derived
            # tenant unfilled — the server will pull tenant from
            # credentials. (Mirrors how tests exercise the path.)
            raise ValueError(
                "Auth-derived tenants on subscribe() aren't supported "
                "yet — pass an explicit tenant to Client.session() until "
                "the auth-bound subscribe route lands."
            )
        path = (
            f"/api/tenants/{_quote(self.tenant)}"
            f"/sessions/{_quote(self.session_id)}/subscribe"
        )
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
        if self.tenant is not None and not self._client.authenticated:
            # No auth → the server can't derive tenant from
            # credentials. Pass it through the deprecated body
            # field so pre-M26 servers and dev-mode tests still
            # work. M26+ servers reject body-vs-auth mismatch
            # with a 400, which is the right contract.
            body["tenant_id"] = self.tenant
        self._client._request(
            "POST", f"/api/sessions/{_quote(self.session_id)}/fork", json=body
        )
        return Session(self._client, child_session_id, tenant=self.tenant)

    # --- routing helper ---

    def _append_path(self) -> str:
        """Pick between the legacy tenant-explicit path and the
        new session-keyed path based on what the caller has
        configured. The trade-off:

        - Authenticated clients with no `tenant` override → new
          path `/api/sessions/{sid}/append_and_fetch`. Tenant is
          auth-derived.
        - Anything with an explicit tenant → legacy path
          `/api/tenants/{tid}/sessions/{sid}/append_and_fetch`,
          which works in dev mode (no auth) and is what every
          server version since M19 has supported.
        """
        if self._client.authenticated and self.tenant is None:
            return f"/api/sessions/{_quote(self.session_id)}/append_and_fetch"
        if self.tenant is None:
            raise ValueError(
                "`tenant` must be set on the Client or Session when no auth "
                "credentials are configured. Pass tenant='demo' (or your "
                "actual tenant id) to Client(...) or Client.session(...)."
            )
        return (
            f"/api/tenants/{_quote(self.tenant)}"
            f"/sessions/{_quote(self.session_id)}/append_and_fetch"
        )


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
    event: Any,
    tenant: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    fetch_last_n: int = 0,
) -> AppendResult:
    """One-line helper for the most common operation: append a
    single event. Constructs a `Client`, opens a session, calls
    `append`, and returns the result. Use the `Client` class
    directly when you want to keep a connection pooled across
    many events.
    """
    client = Client(base_url, user=user, password=password, tenant=tenant)
    return client.session(session_id).append(event, fetch_last_n=fetch_last_n)
