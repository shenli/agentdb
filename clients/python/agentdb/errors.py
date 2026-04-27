"""Exception hierarchy for the AgentDB Python client.

The server emits structured error bodies of the form

    {"error": {"code": "...", "kind": "...", "message": "..."}}

which the client surfaces as one of the typed subclasses below.
Status codes are also preserved so callers can write code that
keys off either the HTTP layer or the error code, whichever is
more idiomatic for them.
"""

from __future__ import annotations

from typing import Any, Optional


class AgentDBError(Exception):
    """Base class for every AgentDB client-surfaced error.

    Catch this at the top of an agent loop to handle any AgentDB
    failure uniformly; catch a subclass when you want to react to
    a specific category (e.g. retry on `RateLimited`).
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: Optional[str] = None,
        kind: Optional[str] = None,
        body: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.kind = kind
        self.body = body

    def __str__(self) -> str:
        bits = [super().__str__()]
        if self.code:
            bits.append(f"(code={self.code})")
        bits.append(f"[HTTP {self.status_code}]")
        return " ".join(bits)


class BadRequest(AgentDBError):
    """400. Malformed body, missing required field, mismatch."""


class Unauthorized(AgentDBError):
    """401. Auth header missing or credentials invalid."""


class Forbidden(AgentDBError):
    """403. Authenticated user can't act on the target tenant/session."""


class NotFound(AgentDBError):
    """404. Tenant or session doesn't exist."""


class Conflict(AgentDBError):
    """409. Resource already exists or is in a conflicting state."""


class RateLimited(AgentDBError):
    """429. Per-tenant or per-tablet quota exhausted; retry after a beat."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        retry_after_ms: Optional[int] = None,
        code: Optional[str] = None,
        kind: Optional[str] = None,
        body: Optional[Any] = None,
    ) -> None:
        super().__init__(
            message, status_code=status_code, code=code, kind=kind, body=body
        )
        self.retry_after_ms = retry_after_ms


class Misdirected(AgentDBError):
    """421. Routing drift on a forwarded request (multi-node operators)."""


class ServerError(AgentDBError):
    """5xx. Forwarding failed, fault injected, or node draining.

    Almost always transient — sensible to retry once with backoff.
    """


class SubscriptionClosed(AgentDBError):
    """Raised when the server emits a terminal `event: error` frame on
    a `subscribe()` stream.

    The body of those frames carries `{"error": "<code>", "last_seq":
    <int?>}`. The `error` field is what the client uses to decide
    whether to reconnect:

    - `jwt_expired`: the auto-refresh path swallows this and
      reconnects with `from_seq = last_seq + 1`. Callers never see
      it unless they passed no token provider.
    - `lag_too_far` / `catchup_failed`: surfaced as-is. The caller
      can choose to reconnect with a fresher `from_seq` or give up.

    `last_seq`, when present, is the sequence number of the last
    event the server actually delivered before closing — use it to
    pick a gap-free resume point.

    Subscriptions don't carry an HTTP status (the close is mid-
    stream, so the original 200 still stands); we set
    `status_code=0` to make that explicit.
    """

    def __init__(
        self,
        error_code: str,
        *,
        last_seq: Optional[int] = None,
        message: Optional[str] = None,
    ) -> None:
        super().__init__(
            message or f"subscription closed: {error_code}",
            status_code=0,
            code=error_code,
            kind=None,
            body={"error": error_code, "last_seq": last_seq},
        )
        self.error_code = error_code
        self.last_seq = last_seq


_CODE_FOR_STATUS: dict[int, type[AgentDBError]] = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound,
    409: Conflict,
    421: Misdirected,
    429: RateLimited,
}


def from_response(status_code: int, body: Any) -> AgentDBError:
    """Construct the right `AgentDBError` subclass for a status + body.

    `body` is whatever the server returned: typically a parsed
    JSON dict with an `"error"` key, but we tolerate plain text
    (proxies, very early server failures) by falling back to the
    raw string.
    """
    err_body = None
    code = None
    kind = None
    message = None
    retry_after_ms = None
    if isinstance(body, dict):
        err_body = body.get("error") if isinstance(body.get("error"), dict) else None
        if err_body:
            code = err_body.get("code")
            kind = err_body.get("kind")
            message = err_body.get("message")
            retry_after_ms = err_body.get("retry_after_ms")
        else:
            message = body.get("errmsg") or str(body)
    elif body is not None:
        message = str(body)
    if not message:
        message = f"AgentDB returned HTTP {status_code}"
    cls: type[AgentDBError]
    if status_code >= 500:
        cls = ServerError
    else:
        cls = _CODE_FOR_STATUS.get(status_code, AgentDBError)
    if cls is RateLimited:
        return RateLimited(
            message,
            status_code=status_code,
            retry_after_ms=retry_after_ms,
            code=code,
            kind=kind,
            body=err_body or body,
        )
    return cls(
        message,
        status_code=status_code,
        code=code,
        kind=kind,
        body=err_body or body,
    )
