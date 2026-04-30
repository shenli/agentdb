"""End-to-end smoke test for the agentdb Python SDK.

Spawns `agentdb serve --in-memory` as a subprocess on an ephemeral
port, then exercises the V2 SDK against it.

Skipped by default. Set `AGENTDB_PYTHON_SMOKE=1` after building the server
binary to run it.
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

_TENANT_COUNTER = 0


def _free_port() -> int:
    """Bind a transient socket, get its port, drop the socket."""
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
    if os.environ.get("AGENTDB_PYTHON_SMOKE") != "1":
        pytest.skip("set AGENTDB_PYTHON_SMOKE=1 to run live server smoke tests")
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
    # Wait for server ready (no auth needed for /health/ready).
    health_client = Client(base_url)
    deadline = time.time() + 15
    while time.time() < deadline:
        if health_client.health_ready():
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


def _session(base_url, label: str):
    global _TENANT_COUNTER
    _TENANT_COUNTER += 1
    client = Client(base_url)
    tenant = client.create_tenant(f"{label}-{_TENANT_COUNTER}")
    agent = client.create_agent(tenant["tenant_id"], alias=f"{label}-agent")
    return client.create_session(tenant["tenant_id"], agent["agent_id"], alias=label)


def test_append_round_trips(server):
    sess = _session(server, "agent-smoke-1")
    result = sess.append(
        {"role": "user", "text": "hello"}, fetch_last_n=5
    )
    assert result.seq >= 0
    assert len(result.recent) == 1
    assert result.recent[0].payload["role"] == "user"
    assert result.recent[0].payload["text"] == "hello"


def test_append_then_append_returns_growing_recent(server):
    sess = _session(server, "agent-smoke-2")
    sess.append({"i": 1}, fetch_last_n=0)
    sess.append({"i": 2}, fetch_last_n=0)
    last = sess.append({"i": 3}, fetch_last_n=10)
    seqs = [e.payload["i"] for e in last.recent]
    assert seqs == [1, 2, 3], f"expected ordered events; got {seqs}"


def test_fork_creates_a_child_session(server):
    parent = _session(server, "agent-smoke-fork-parent")
    parent.append({"i": 1}, fetch_last_n=0)
    parent.append({"i": 2}, fetch_last_n=0)
    child = parent.fork(at_seq=1, alias="agent-smoke-fork-child")
    assert isinstance(child, Session)
    assert child.session_id.startswith("ses_")
    res = child.append({"i": 99}, fetch_last_n=10)
    payloads = [e.payload.get("i") for e in res.recent]
    assert payloads[-1] == 99


def test_health_ready_probe(server):
    client = Client(server)
    assert client.health_ready() is True


def test_bad_credentials_pair_raises_value_error():
    # `token` and `get_token` together is a programmer error.
    with pytest.raises(ValueError, match="get_token"):
        Client("http://127.0.0.1:7878", token="t", get_token=lambda: "t")


# Placate the unused-import linter on `BadRequest` / `NotFound`
_ = (BadRequest, NotFound)
