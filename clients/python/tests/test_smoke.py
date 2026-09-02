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
    SessionEventKind,
    Session,
    agent_status,
    context_pack_built,
    context_source_refs,
    model_message,
    tool_call,
    tool_result,
    user_message,
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


def test_harness_recovers_session_from_ids(server):
    first_client = Client(server)
    tenant = first_client.create_tenant("agent-smoke-recovery")
    tenant_id = tenant["tenant_id"]
    agent = first_client.create_agent(tenant_id, alias="agent-smoke-recovery-agent")
    agent_id = agent["agent_id"]
    session = first_client.create_session(tenant_id, agent_id, alias="agent-smoke-recovery")

    session.append(
        user_message("summarize the sandbox output"),
        kind=SessionEventKind.USER_MESSAGE.value,
    )
    call = session.append(
        tool_call("call_1", "sandbox.exec", {"cmd": "python report.py"}),
        kind=SessionEventKind.TOOL_CALL.value,
    )
    artifact = first_client.put_artifact(
        tenant_id,
        b"generated report\nline 1\nline 2\n",
        content_type="text/plain",
    )
    session.append(
        tool_result("call_1", "Generated report with two lines."),
        kind=SessionEventKind.TOOL_RESULT.value,
        artifact_refs=[
            {
                "artifact_id": artifact["artifact_id"],
                "content_type": artifact["content_type"],
                "bytes": artifact["bytes"],
                "sha256": artifact["sha256"],
            }
        ],
        source_refs=[{"event_id": call.event_id}],
    )
    context = session.context(include=["recent_events"], max_events=10)
    assert context.get("events"), "context should include recent events"
    context_event = session.append(
        context_pack_built(
            "ctx_recovery_1",
            ["recent_events"],
            max_events=10,
            selected_event_count=len(context.get("events", [])),
        ),
        kind=SessionEventKind.CONTEXT_PACK_BUILT.value,
        source_refs=context_source_refs(context),
    )
    session.append(
        model_message(
            "The report has two generated lines.",
            context_pack_id="ctx_recovery_1",
        ),
        kind=SessionEventKind.MODEL_MESSAGE.value,
        source_refs=[{"event_id": context_event.event_id}],
    )

    last_seq_before_crash = session.fetch(from_seq=0, limit=100)[-1].seq

    # Simulate a fresh harness process: only persisted IDs are carried forward.
    recovery_client = Client(server)
    recovered = recovery_client.session(tenant_id, session.session_id, agent_id=agent_id)
    replayed = recovered.replay(from_seq=0, limit=100)
    assert [event.kind for event in replayed] == [
        "user_message",
        "tool_call",
        "tool_result",
        "context_pack_built",
        "model_message",
    ]

    resumed = recovered.append(
        agent_status("recovered", last_seen_seq=last_seq_before_crash),
        kind=SessionEventKind.AGENT_STATUS.value,
    )
    assert resumed.seq == last_seq_before_crash + 1


def test_health_ready_probe(server):
    client = Client(server)
    assert client.health_ready() is True


def test_bad_credentials_pair_raises_value_error():
    # `token` and `get_token` together is a programmer error.
    with pytest.raises(ValueError, match="get_token"):
        Client("http://127.0.0.1:7878", token="t", get_token=lambda: "t")


# Placate the unused-import linter on `BadRequest` / `NotFound`
_ = (BadRequest, NotFound)
