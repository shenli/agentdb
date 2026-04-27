"""Tests for `Client.subscribe()` / `Session.subscribe()` and the
internal SSE parser.

Two layers:

1. Pure-Python parser tests (always run on `pytest`). The parser is
   the trickiest piece of the subscribe path — getting the frame
   boundary, multi-line `data:` joins, and the leading-space rule
   right is the difference between "works" and "drops events".

2. End-to-end smoke against a live `agentdb serve --dev` server,
   gated on `AGENTDB_PYTHON_SMOKE=1`. Skipped by default to keep
   `pytest` fast and not require the cargo workspace to be built.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest


# --- SSE parser unit tests (always run) -------------------------


def test_parse_lines_yields_named_event_then_data():
    from agentdb._sse import SseFrame, parse_lines

    lines = iter(["event: subscribed", 'data: {"live_seq": 1}', ""])
    frames = list(parse_lines(lines))
    assert frames == [SseFrame(event="subscribed", data='{"live_seq": 1}')]


def test_parse_lines_skips_comment_lines():
    from agentdb._sse import SseFrame, parse_lines

    lines = iter([": keepalive", "", 'data: {"seq": 1}', ""])
    frames = list(parse_lines(lines))
    assert frames == [SseFrame(event=None, data='{"seq": 1}')]


def test_parse_lines_handles_multi_line_data():
    from agentdb._sse import SseFrame, parse_lines

    lines = iter(["data: line1", "data: line2", ""])
    frames = list(parse_lines(lines))
    assert frames == [SseFrame(event=None, data="line1\nline2")]


def test_parse_lines_strips_data_leading_space():
    """The parser strips leading-space padding from `data:` lines so
    `data: x` and `data:x` produce the same payload — AgentDB's
    server emits both shapes depending on the framing path."""
    from agentdb._sse import SseFrame, parse_lines

    lines = iter(["data: a", "data:b", ""])
    frames = list(parse_lines(lines))
    assert frames == [SseFrame(event=None, data="a\nb")]


def test_parse_lines_strips_trailing_cr_for_crlf_servers():
    from agentdb._sse import SseFrame, parse_lines

    lines = iter(["event: ping\r", 'data: {"hi":true}\r', ""])
    frames = list(parse_lines(lines))
    assert frames == [SseFrame(event="ping", data='{"hi":true}')]


def test_async_parse_lines_handles_multi_line_data():
    """Async variant should match sync semantics. Drives it with a
    trivial async generator and `asyncio.run`."""
    from agentdb._sse import SseFrame, parse_lines_async

    async def src():
        for line in ("data: line1", "data: line2", ""):
            yield line

    async def collect():
        out = []
        async for f in parse_lines_async(src()):
            out.append(f)
        return out

    assert asyncio.run(collect()) == [
        SseFrame(event=None, data="line1\nline2")
    ]


# --- Public API surface (no network) ----------------------------


def test_client_rejects_jwt_and_get_token_together():
    from agentdb import Client

    with pytest.raises(ValueError, match="jwt.*get_token"):
        Client("http://127.0.0.1:7878", jwt="t", get_token=lambda: "t")


def test_client_subscribe_requires_tenant_when_unset():
    from agentdb import Client

    client = Client("http://127.0.0.1:7878")
    with pytest.raises(ValueError, match="tenant"):
        # Wrap in iter()/next() to force the generator to run
        # validation. The check is in the eager path of
        # Client.subscribe so even calling subscribe() should
        # raise; verify both paths trip the same error.
        next(iter(client.subscribe()))


def test_session_subscribe_without_tenant_raises():
    from agentdb import Client

    client = Client("http://127.0.0.1:7878")  # no tenant, no auth
    sess = client.session("agent-1")
    # subscribe() returns a generator; first .next() runs the
    # validation in the body before any network I/O.
    with pytest.raises(ValueError, match="tenant"):
        next(iter(sess.subscribe()))


# --- End-to-end smoke (gated) -----------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_BIN = REPO_ROOT / "target" / "debug" / "agentdb"

SMOKE_REASON = "set AGENTDB_PYTHON_SMOKE=1 to run (requires cargo workspace built)"
SMOKE_SKIP = pytest.mark.skipif(
    os.environ.get("AGENTDB_PYTHON_SMOKE") != "1",
    reason=SMOKE_REASON,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def subscribe_server():
    if os.environ.get("AGENTDB_PYTHON_SMOKE") != "1":
        pytest.skip(SMOKE_REASON)
    if not SERVER_BIN.exists():
        pytest.skip(
            f"server binary not found at {SERVER_BIN}; "
            "run `cargo build -p agentdb-server` first"
        )
    rest_port = _free_port()
    mongo_port = _free_port()
    env = {**os.environ, "RUST_LOG": "warn"}
    proc = subprocess.Popen(
        [
            str(SERVER_BIN),
            "serve",
            "--dev",
            "--addr",
            f"127.0.0.1:{rest_port}",
            "--mongo-addr",
            f"127.0.0.1:{mongo_port}",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{rest_port}"

    from agentdb import Client

    client = Client(base_url, tenant="demo")
    deadline = time.time() + 15
    while time.time() < deadline:
        if client.health_ready():
            break
        if proc.poll() is not None:
            pytest.fail(f"server exited before ready (rc={proc.returncode})")
        time.sleep(0.1)
    else:
        proc.terminate()
        proc.wait(timeout=5)
        pytest.fail("server didn't become ready within 15s")
    try:
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


@SMOKE_SKIP
def test_subscribe_yields_appended_events(subscribe_server):
    """Live-tail: open a per-session subscription, then append three
    events, and watch them come back in order."""
    from agentdb import Client

    base = subscribe_server
    seen: list = []
    error_box: list = []

    def consume():
        try:
            client = Client(base, tenant="t-1")
            sess = client.session("s-sub-py")
            for evt in sess.subscribe():
                seen.append(evt)
                if len(seen) >= 3:
                    return
        except Exception as e:  # pragma: no cover (smoke debug)
            error_box.append(e)

    consumer = threading.Thread(target=consume, daemon=True)
    consumer.start()
    # Give the subscribe a beat to attach before we start writing.
    time.sleep(1.0)

    writer = Client(base, tenant="t-1")
    sess = writer.session("s-sub-py")
    for i in range(3):
        sess.append({"kind": "msg", "n": i})
    consumer.join(timeout=10)
    assert not error_box, f"consumer crashed: {error_box[0]!r}"
    assert len(seen) == 3
    seqs = sorted(e.seq for e in seen)
    assert seqs == [1, 2, 3]


@SMOKE_SKIP
def test_subscribe_replays_from_seq(subscribe_server):
    """Pre-seed events, then `from_seq=1` on a per-session subscribe
    should replay them in order."""
    from agentdb import Client

    base = subscribe_server
    writer = Client(base, tenant="t-1")
    sess = writer.session("s-sub-py-replay")
    for _ in range(3):
        sess.append({"kind": "x"})

    client = Client(base, tenant="t-1")
    reader = client.session("s-sub-py-replay")
    seqs: list = []
    for evt in reader.subscribe(from_seq=1):
        seqs.append(evt.seq)
        if len(seqs) >= 3:
            break
    assert seqs == [1, 2, 3]


@SMOKE_SKIP
def test_tenant_subscribe_fans_out_across_sessions(subscribe_server):
    """Tenant-wide subscribe sees events from any session under
    the tenant."""
    from agentdb import Client

    base = subscribe_server
    seen: list = []
    error_box: list = []

    def consume():
        try:
            client = Client(base, tenant="t-fanout-py")
            for evt in client.subscribe():
                seen.append(evt)
                if len(seen) >= 4:
                    return
        except Exception as e:  # pragma: no cover (smoke debug)
            error_box.append(e)

    consumer = threading.Thread(target=consume, daemon=True)
    consumer.start()
    time.sleep(1.0)

    writer = Client(base, tenant="t-fanout-py")
    writer.session("s-a").append({"kind": "p", "t": 1})
    writer.session("s-b").append({"kind": "p", "t": 2})
    writer.session("s-a").append({"kind": "p", "t": 3})
    writer.session("s-b").append({"kind": "p", "t": 4})
    consumer.join(timeout=10)
    assert not error_box, f"consumer crashed: {error_box[0]!r}"
    assert len(seen) == 4
    sids = {e.session for e in seen}
    assert "s-a" in sids and "s-b" in sids
