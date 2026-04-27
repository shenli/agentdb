"""Phase 2 broadcast message type + parsing helpers.

Ephemeral pub/sub messages — sent via `Client.broadcast()` and
streamed back via `Client.subscribe_channel()`. Distinct from the
durable [`Event`] type (returned by `append_and_fetch` / per-session
`subscribe`) because broadcast has no `seq` and no replay; the
server fans out to current subscribers and forgets.

Wire envelope (spec § 5.2):

    {"tenant": "...", "channel": "...", "ts_ms": 0,
     "sender": "...", "payload": <any JSON>}

`sender` is the publisher's JWT `sub` claim, or `None` when the
publisher used Basic auth (which has no per-user identity).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class BroadcastMessage:
    """One broadcast envelope received from a channel subscription.

    `payload` is the publisher's arbitrary JSON, passed through
    unchanged. `sender` is `None` when the publisher used Basic
    auth — JWT publishers carry the `sub` claim.
    """

    tenant: str
    channel: str
    ts_ms: int
    sender: Optional[str]
    payload: Any

    @classmethod
    def from_envelope(cls, env: dict) -> "BroadcastMessage":
        return cls(
            tenant=env["tenant"],
            channel=env["channel"],
            ts_ms=env["ts_ms"],
            sender=env.get("sender"),
            payload=env["payload"],
        )
