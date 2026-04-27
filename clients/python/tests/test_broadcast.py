"""Tests for Phase 2 `broadcast()` and `subscribe_channel()`.

Two layers (mirrors `test_subscribe.py`):

1. Pure-Python unit tests (always run on `pytest`). Cover envelope
   parsing + the public-API guards (tenant required, etc.) — no
   network, no server.

2. End-to-end smoke against a live `agentdb serve --dev`, gated on
   `AGENTDB_PYTHON_SMOKE=1`. Reuses `test_subscribe.py`'s
   `subscribe_server` fixture so we don't pay for a second server
   boot when both files run together.
"""

from __future__ import annotations

import threading
import time

import pytest

from agentdb import BroadcastMessage, Client


# --- BroadcastMessage envelope parsing (always run) -------------


def test_broadcast_message_parses_full_envelope():
    msg = BroadcastMessage.from_envelope(
        {
            "tenant": "t-1",
            "channel": "c-1",
            "ts_ms": 100,
            "sender": "u-1",
            "payload": {"k": "v"},
        }
    )
    assert msg.tenant == "t-1"
    assert msg.channel == "c-1"
    assert msg.ts_ms == 100
    assert msg.sender == "u-1"
    assert msg.payload == {"k": "v"}


def test_broadcast_message_handles_missing_sender():
    """Basic-auth publishers emit envelopes without a `sender`
    field. The dataclass defaults that to None so subscribers can
    distinguish them from JWT publishers cleanly."""
    msg = BroadcastMessage.from_envelope(
        {
            "tenant": "t-1",
            "channel": "c-1",
            "ts_ms": 100,
            "payload": {},
        }
    )
    assert msg.sender is None


# --- Public API surface (no network) ----------------------------


def test_broadcast_requires_tenant_when_unset():
    client = Client("http://127.0.0.1:7878")  # no tenant, no auth
    with pytest.raises(ValueError, match="tenant"):
        client.broadcast(channel="c-1", payload={"x": 1})


def test_subscribe_channel_requires_tenant_when_unset():
    client = Client("http://127.0.0.1:7878")
    with pytest.raises(ValueError, match="tenant"):
        # subscribe_channel returns a generator; first .next() runs
        # the validation in the body before any network I/O.
        next(iter(client.subscribe_channel(channel="c-1")))


# --- End-to-end smoke (gated) -----------------------------------
#
# Reuse the module-scoped `subscribe_server` fixture from
# test_subscribe.py — pytest auto-discovers it when both modules
# live in the same package, so the same `agentdb serve --dev`
# subprocess covers both files when `AGENTDB_PYTHON_SMOKE=1`.

# Pytest's `rootdir`-based import puts the tests directory on
# `sys.path`, so a flat `from test_subscribe import ...` reaches
# the sibling module without a package `__init__.py`.
from test_subscribe import SMOKE_SKIP, subscribe_server  # noqa: E402,F401


@SMOKE_SKIP
def test_broadcast_round_trip(subscribe_server):
    """Publish + subscribe round-trip on a single channel.

    Open a subscriber, sleep briefly so it attaches before the
    first publish, then send two payloads and assert both arrive.
    """
    base = subscribe_server
    seen: list = []
    error_box: list = []

    def consume() -> None:
        try:
            client = Client(base, tenant="t-bcast-py")
            for msg in client.subscribe_channel(channel="c-rt"):
                seen.append(msg)
                if len(seen) >= 2:
                    return
        except Exception as e:  # pragma: no cover (smoke debug)
            error_box.append(e)

    consumer = threading.Thread(target=consume, daemon=True)
    consumer.start()
    # Let the subscriber attach; broadcast has no replay, so a
    # publish before the subscribe lands would be silently dropped.
    time.sleep(1.0)

    publisher = Client(base, tenant="t-bcast-py")
    publisher.broadcast(channel="c-rt", payload={"i": 1})
    publisher.broadcast(channel="c-rt", payload={"i": 2})

    consumer.join(timeout=10)
    assert not error_box, f"consumer crashed: {error_box[0]!r}"
    assert len(seen) == 2
    ints = sorted(m.payload["i"] for m in seen)
    assert ints == [1, 2]
    for m in seen:
        assert m.tenant == "t-bcast-py"
        assert m.channel == "c-rt"
        assert m.sender is None  # no JWT, no `sub`
        assert isinstance(m.ts_ms, int)
