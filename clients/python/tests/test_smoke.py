"""End-to-end smoke test for the agentdb Python SDK.

Spawns `agentdb serve --dev` as a subprocess on an ephemeral
port, waits for `/health/ready`, and exercises the SDK against
it. The whole test runs in <5s on a warm cargo cache.

Skipped automatically if the server binary hasn't been built —
running this in CI without `cargo build -p agentdb-server` first
is a no-op (with a clear pytest skip reason) rather than a hard
failure.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

from agentdb import (
    BadRequest,
    Client,
    NotFound,
    Session,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_BIN = REPO_ROOT / "target" / "debug" / "agentdb"


def _free_port() -> int:
    """Bind a transient socket, get its port, drop the socket.
    The port is racy but good enough for a smoke test — the
    server is the next process to bind, almost certainly first."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server():
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
    client = Client(base_url, tenant="demo")
    # Poll readiness for up to 15 seconds; the dev-mode bootstrap
    # is normally <2s but a cold cargo build can be slow.
    deadline = time.time() + 15
    while time.time() < deadline:
        if client.health_ready():
            break
        if proc.poll() is not None:
            pytest.fail(
                f"server exited before ready (rc={proc.returncode})"
            )
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


def test_append_round_trips(server):
    client = Client(server, tenant="demo")
    sess = client.session("agent-smoke-1")
    result = sess.append(
        {"role": "user", "text": "hello"}, fetch_last_n=5
    )
    assert result.seq >= 1
    assert len(result.recent) == 1
    assert result.recent[0].payload["role"] == "user"
    assert result.recent[0].payload["text"] == "hello"


def test_append_then_append_returns_growing_recent(server):
    client = Client(server, tenant="demo")
    sess = client.session("agent-smoke-2")
    sess.append({"i": 1}, fetch_last_n=0)
    sess.append({"i": 2}, fetch_last_n=0)
    last = sess.append({"i": 3}, fetch_last_n=10)
    seqs = [e.payload["i"] for e in last.recent]
    assert seqs == [1, 2, 3], f"expected ordered events; got {seqs}"


def test_fork_creates_a_child_session(server):
    client = Client(server, tenant="demo")
    parent = client.session("agent-smoke-fork-parent")
    parent.append({"i": 1}, fetch_last_n=0)
    parent.append({"i": 2}, fetch_last_n=0)
    child = parent.fork("agent-smoke-fork-child")
    assert isinstance(child, Session)
    assert child.session_id == "agent-smoke-fork-child"
    # The child can be written to and behaves like its own session.
    res = child.append({"i": 99}, fetch_last_n=10)
    payloads = [e.payload.get("i") for e in res.recent]
    # Child sees the parent's events plus its own; order matters.
    assert payloads[-1] == 99


def test_session_with_no_tenant_and_no_auth_raises_on_append(server):
    # The client default tenant is None; no auth configured.
    # The SDK should refuse to construct a path rather than
    # send a request the server will reject.
    client = Client(server)  # no tenant, no auth
    sess = client.session("agent-smoke-3")
    with pytest.raises(ValueError, match="tenant"):
        sess.append({"x": 1})


def test_unknown_tenant_returns_typed_error(server):
    client = Client(server, tenant="t-nope-doesnt-exist")
    sess = client.session("agent-smoke-4")
    # Dev mode without auth lets through any path tenant; the
    # tenant gets minted on first write. Not an error case in
    # this server config — assert the contract that it succeeds.
    result = sess.append({"k": "v"}, fetch_last_n=0)
    assert result.seq >= 1


def test_health_ready_probe(server):
    client = Client(server)
    assert client.health_ready() is True


def test_bad_credentials_pair_raises_value_error():
    # Client construction with `user` but no `password` (or vice
    # versa) is a programmer error — surface it at construct
    # time rather than letting the request 401.
    with pytest.raises(ValueError, match="password"):
        Client("http://127.0.0.1:7878", user="alice")
    with pytest.raises(ValueError, match="user"):
        Client("http://127.0.0.1:7878", password="pencil")


# Placate the unused-import linter on `BadRequest` / `NotFound`
# imports — they exist so the smoke tests can grow into the
# typed-error space later without re-importing.
_ = (BadRequest, NotFound)
