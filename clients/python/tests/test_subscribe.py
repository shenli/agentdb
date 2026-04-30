"""Tests for `Client.subscribe()` / `Session.subscribe()` and the
internal SSE parser.

Two layers:

1. Pure-Python parser tests (always run on `pytest`). The parser is
   the trickiest piece of the subscribe path — getting the frame
   boundary, multi-line `data:` joins, and the leading-space rule
   right is the difference between "works" and "drops events".

2. End-to-end smoke against a live `agentdb serve --in-memory` server,
   gated on `AGENTDB_PYTHON_SMOKE=1`. Skipped by default to keep
   `pytest` fast and not require the cargo workspace to be built.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
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
    """Async variant should match sync semantics."""
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


def test_client_rejects_token_and_get_token_together():
    from agentdb import Client

    with pytest.raises(ValueError, match="get_token"):
        Client("http://127.0.0.1:7878", token="t", get_token=lambda: "t")


def test_tenant_watch_path_uses_v2():
    """Tenant-wide watch uses the native tenant-scoped V2 route."""
    from agentdb import Client

    client = Client("http://127.0.0.1:7878", auth=("u", "p"))
    calls = []

    def fake(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return []

    client._request = fake
    client.watch("ten_1", scope="tenant")
    assert calls == [
        (
            "GET",
            "/api/tenants/ten_1/watch",
            {"params": {"scope": "tenant", "limit": "100"}},
        )
    ]


def test_session_subscribe_path_uses_v2():
    """Session.subscribe path must be tenant scoped."""
    from agentdb import Client

    client = Client("http://127.0.0.1:7878", auth=("u", "p"))
    sess = client.session("ten_1", "ses_1")
    gen = sess.subscribe()

    class FakeResponse:
        status_code = 200

        def iter_lines(self, decode_unicode=True):
            return iter(())

        def close(self):
            pass

    calls = []

    def fake(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return FakeResponse()

    client._raw_request = fake
    list(gen)
    assert calls[0][0] == "GET"
    assert calls[0][1] == "/api/tenants/ten_1/sessions/ses_1/subscribe"


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
    env = {**os.environ, "RUST_LOG": "warn"}
    proc = subprocess.Popen(
        [
            str(SERVER_BIN),
            "serve",
            "--in-memory",
            "--bind",
            f"127.0.0.1:{rest_port}",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{rest_port}"

    from agentdb import Client

    client = Client(base_url)
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


def _session(base_url: str, label: str):
    from agentdb import Client

    client = Client(base_url)
    tenant = client.create_tenant(label)
    agent = client.create_agent(tenant["tenant_id"], alias=f"{label}-agent")
    return client, client.create_session(tenant["tenant_id"], agent["agent_id"], alias=label)


@SMOKE_SKIP
def test_subscribe_yields_appended_events(subscribe_server):
    """V2 subscribe replays the session event snapshot as SSE."""
    _, sess = _session(subscribe_server, "s-sub-py")
    for i in range(3):
        sess.append({"kind": "msg", "n": i})
    seen = []
    for evt in sess.subscribe(from_seq=0):
        seen.append(evt)
        if len(seen) >= 3:
            break
    assert [e.seq for e in seen] == [0, 1, 2]


@SMOKE_SKIP
def test_subscribe_replays_from_seq(subscribe_server):
    """Pre-seed events, then `from_seq=1` on a per-session subscribe
    should replay them in order."""
    from agentdb import Client

    _, sess = _session(subscribe_server, "s-sub-py-replay")
    for _ in range(3):
        sess.append({"kind": "x"})

    seqs: list = []
    for evt in sess.subscribe(from_seq=1):
        seqs.append(evt.seq)
        if len(seqs) >= 2:
            break
    assert seqs == [1, 2]


@SMOKE_SKIP
def test_tenant_watch_fans_out_across_sessions(subscribe_server):
    """Tenant-wide watch sees events from any session under the tenant."""
    client, first = _session(subscribe_server, "s-a")
    second = client.create_session(first.tenant_id, first.agent_id, alias="s-b")
    first.append({"kind": "p", "t": 1})
    second.append({"kind": "p", "t": 2})
    first.append({"kind": "p", "t": 3})
    second.append({"kind": "p", "t": 4})
    seen = client.watch(first.tenant_id, scope="tenant", limit=10)
    sids = {item.event.session_id for item in seen}
    assert first.session_id in sids and second.session_id in sids
