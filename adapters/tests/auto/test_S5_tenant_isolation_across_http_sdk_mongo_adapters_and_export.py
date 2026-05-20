# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S5 tenant_isolation_across_http_sdk_mongo_adapters_and_export
# Falsifies: C7, C11, C14, C20, C21
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

from __future__ import annotations

import dataclasses
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_BIN = REPO_ROOT / "target" / "debug" / "agentdb"
PYTHON_CLIENT_ROOT = REPO_ROOT / "clients" / "python"
sys.path.insert(0, str(PYTHON_CLIENT_ROOT))

from agentdb import AgentDB, AgentDBError, Client  # noqa: E402


@dataclasses.dataclass(frozen=True)
class LiveServer:
    base_url: str
    mongo_uri: str


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
    mongo_port = _free_port()
    proc = subprocess.Popen(
        [
            str(SERVER_BIN),
            "serve",
            "--in-memory",
            "--bind",
            f"127.0.0.1:{port}",
            "--mongo-bind",
            f"127.0.0.1:{mongo_port}",
        ],
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
        yield LiveServer(
            base_url=base_url,
            mongo_uri=f"mongodb://127.0.0.1:{mongo_port}/?directConnection=true",
        )
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
    _, agent = _json(
        "POST",
        f"{base_url}/api/tenants/{tid}/agents",
        {"alias": f"{key}-agent", "config": {}},
    )
    aid = agent["agent_id"]
    _, session = _json(
        "POST",
        f"{base_url}/api/tenants/{tid}/agents/{aid}/sessions",
        {"alias": f"{key}-session"},
    )
    return tid, aid, session["session_id"]


def test_tenant_isolation_across_http_sdk_and_export_surfaces(live_server: LiveServer):
    tenant_a, agent_a, session_a = _seed(live_server.base_url, "tenant-a-auto")
    tenant_b, agent_b, session_b = _seed(live_server.base_url, "tenant-b-auto")

    for tenant_id, agent_id, session_id, label in [
        (tenant_a, agent_a, session_a, "a-only"),
        (tenant_b, agent_b, session_b, "b-only"),
    ]:
        status, _ = _json(
            "POST",
            f"{live_server.base_url}/api/tenants/{tenant_id}/sessions/{session_id}/events",
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
        (
            f"{live_server.base_url}/api/tenants/{tenant_a}/sessions/"
            f"{session_b}/events?from_seq=0&limit=10"
        ),
    )
    assert status in {400, 404}
    assert "b-only" not in json.dumps(wrong_fetch)

    status, watch_a = _json(
        "GET", f"{live_server.base_url}/api/tenants/{tenant_a}/watch?scope=tenant"
    )
    assert status == 200
    encoded = json.dumps(watch_a)
    assert "a-only" in encoded
    assert "b-only" not in encoded

    sdk = AgentDB(live_server.base_url)
    sdk_workspace_a = sdk.workspace("tenant-a-sdk-auto")
    sdk_workspace_b = sdk.workspace("tenant-b-sdk-auto")
    sdk_session_a = sdk_workspace_a.agent(alias="tenant-a-sdk-agent").session(
        alias="tenant-a-sdk-session"
    )
    sdk_session_b = sdk_workspace_b.agent(alias="tenant-b-sdk-agent").session(
        alias="tenant-b-sdk-session"
    )
    sdk_session_a.append({"label": "sdk-a-only"})
    sdk_session_b.append({"label": "sdk-b-only"})

    wrong_session = Client(live_server.base_url).session(sdk_workspace_a.id, sdk_session_b.id)
    with pytest.raises(AgentDBError) as exc:
        wrong_session.fetch(from_seq=0, limit=10)
    assert exc.value.status_code in {400, 404}
    assert "sdk-b-only" not in json.dumps(exc.value.body)

    watched = sdk_workspace_a.watch(scope="tenant", limit=100)
    assert "sdk-a-only" in json.dumps([item.event.payload for item in watched])
    assert "sdk-b-only" not in json.dumps([item.event.payload for item in watched])

    status, export_a = _json(
        "GET",
        f"{live_server.base_url}/api/tenants/{tenant_a}/exports/watch?include=events&limit=100",
    )
    assert status == 200
    encoded_export = json.dumps(export_a)
    assert "a-only" in encoded_export
    assert "b-only" not in encoded_export

    status, export_sdk_a = _json(
        "GET",
        (
            f"{live_server.base_url}/api/tenants/{sdk_workspace_a.id}/exports/"
            "watch?include=events&limit=100"
        ),
    )
    assert status == 200
    encoded_sdk_export = json.dumps(export_sdk_a)
    assert "sdk-a-only" in encoded_sdk_export
    assert "sdk-b-only" not in encoded_sdk_export


def test_tenant_isolation_across_mongo_database_mapping(live_server: LiveServer):
    pymongo = pytest.importorskip("pymongo")

    key_a = "tenant-a-mongo-auto"
    key_b = "tenant-b-mongo-auto"
    marker_a = "mongo-a-only"
    marker_b = "mongo-b-only"

    mongo_client = pymongo.MongoClient(live_server.mongo_uri, serverSelectionTimeoutMS=2000)
    try:
        mongo_client[key_a].events.insert_one(
            {
                "session_alias": "tenant-a-mongo-session",
                "kind": "message",
                "payload": {"label": marker_a},
            }
        )
        mongo_client[key_b].events.insert_one(
            {
                "session_alias": "tenant-b-mongo-session",
                "kind": "message",
                "payload": {"label": marker_b},
            }
        )

        events_a = list(mongo_client[key_a].events.find({"kind": "message"}))
        encoded_events_a = json.dumps(events_a, default=str)
        assert marker_a in encoded_events_a
        assert marker_b not in encoded_events_a

        events_b = list(mongo_client[key_b].events.find({"kind": "message"}))
        encoded_events_b = json.dumps(events_b, default=str)
        assert marker_b in encoded_events_b
        assert marker_a not in encoded_events_b
    finally:
        mongo_client.close()

    status, tenants = _json("GET", f"{live_server.base_url}/api/tenants")
    assert status == 200
    tenants_by_key = {tenant["external_key"]: tenant["tenant_id"] for tenant in tenants["tenants"]}
    tenant_a = tenants_by_key[key_a]
    tenant_b = tenants_by_key[key_b]

    status, watch_a = _json(
        "GET", f"{live_server.base_url}/api/tenants/{tenant_a}/watch?scope=tenant"
    )
    assert status == 200
    encoded_watch_a = json.dumps(watch_a)
    assert marker_a in encoded_watch_a
    assert marker_b not in encoded_watch_a

    status, export_a = _json(
        "GET",
        f"{live_server.base_url}/api/tenants/{tenant_a}/exports/watch?include=events&limit=100",
    )
    assert status == 200
    encoded_export_a = json.dumps(export_a)
    assert marker_a in encoded_export_a
    assert marker_b not in encoded_export_a

    status, export_b = _json(
        "GET",
        f"{live_server.base_url}/api/tenants/{tenant_b}/exports/watch?include=events&limit=100",
    )
    assert status == 200
    encoded_export_b = json.dumps(export_b)
    assert marker_b in encoded_export_b
    assert marker_a not in encoded_export_b
