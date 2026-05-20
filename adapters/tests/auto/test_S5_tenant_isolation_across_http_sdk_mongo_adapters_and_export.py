# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S5 tenant_isolation_across_http_sdk_mongo_adapters_and_export
# Falsifies: C7, C11, C14, C20, C21
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_BIN = REPO_ROOT / "target" / "debug" / "agentdb"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _json(method: str, url: str, body: dict | None = None) -> tuple[int, dict | list]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = resp.read()
            return resp.status, json.loads(payload.decode("utf-8")) if payload else {}
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        return exc.code, json.loads(payload.decode("utf-8")) if payload else {}


@pytest.fixture()
def live_server():
    if os.environ.get("AGENTDB_AUTHOR_LIVE") != "1":
        pytest.skip("set AGENTDB_AUTHOR_LIVE=1 to run this author-mode live HTTP test")
    if not SERVER_BIN.exists():
        pytest.skip(f"server binary not found at {SERVER_BIN}; run cargo build -p agentdb-server")
    port = _free_port()
    proc = subprocess.Popen(
        [str(SERVER_BIN), "serve", "--in-memory", "--bind", f"127.0.0.1:{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "RUST_LOG": "warn"},
    )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{base_url}/health/ready", timeout=1).close()
            break
        except Exception:
            if proc.poll() is not None:
                pytest.fail(f"server exited before ready with rc={proc.returncode}")
            time.sleep(0.1)
    else:
        proc.terminate()
        proc.wait(timeout=5)
        pytest.fail("server did not become ready")
    try:
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _seed(base_url: str, key: str) -> tuple[str, str, str]:
    _, tenant = _json("POST", f"{base_url}/api/tenants", {"external_key": key, "policy": {}})
    tid = tenant["tenant_id"]
    _, agent = _json("POST", f"{base_url}/api/tenants/{tid}/agents", {"alias": f"{key}-agent", "config": {}})
    aid = agent["agent_id"]
    _, session = _json("POST", f"{base_url}/api/tenants/{tid}/agents/{aid}/sessions", {"alias": f"{key}-session"})
    return tid, aid, session["session_id"]


def test_tenant_isolation_across_http_surfaces(live_server: str):
    tenant_a, agent_a, session_a = _seed(live_server, "tenant-a-auto")
    tenant_b, agent_b, session_b = _seed(live_server, "tenant-b-auto")

    for tenant_id, agent_id, session_id, label in [
        (tenant_a, agent_a, session_a, "a-only"),
        (tenant_b, agent_b, session_b, "b-only"),
    ]:
        status, _ = _json(
            "POST",
            f"{live_server}/api/tenants/{tenant_id}/sessions/{session_id}/events",
            {
                "agent_id": agent_id,
                "kind": "message",
                "payload": {"label": label},
                "artifact_refs": [],
                "source_refs": [],
            },
        )
        assert status == 200

    status, wrong_fetch = _json(
        "GET",
        f"{live_server}/api/tenants/{tenant_a}/sessions/{session_b}/events?from_seq=0&limit=10",
    )
    assert status in {400, 404}
    assert "b-only" not in json.dumps(wrong_fetch)

    status, watch_a = _json("GET", f"{live_server}/api/tenants/{tenant_a}/watch?scope=tenant")
    assert status == 200
    encoded = json.dumps(watch_a)
    assert "a-only" in encoded
    assert "b-only" not in encoded
