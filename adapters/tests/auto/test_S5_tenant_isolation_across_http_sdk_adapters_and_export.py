# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S5 tenant_isolation_across_http_sdk_adapters_and_export
# Falsifies: C7, C11, C14, C20, C21
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

from __future__ import annotations

import dataclasses
import json
import os
import shutil
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
TYPESCRIPT_CLIENT_ENTRY = REPO_ROOT / "clients" / "typescript" / "dist" / "index.js"
sys.path.insert(0, str(PYTHON_CLIENT_ROOT))

from agentdb import AgentDB, AgentDBError, Client  # noqa: E402


@dataclasses.dataclass(frozen=True)
class LiveServer:
    base_url: str


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
        pytest.skip(
            f"server binary not found at {SERVER_BIN}; run cargo build -p agentdb-server"
        )
    port = _free_port()
    proc = subprocess.Popen(
        [
            str(SERVER_BIN),
            "serve",
            "--in-memory",
            "--bind",
            f"127.0.0.1:{port}",
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
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _seed(base_url: str, key: str) -> tuple[str, str, str]:
    _, tenant = _json(
        "POST", f"{base_url}/api/tenants", {"external_key": key, "policy": {}}
    )
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


def _run_typescript_sdk_isolation(base_url: str) -> dict:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required for the TypeScript SDK isolation check")
    if not TYPESCRIPT_CLIENT_ENTRY.exists():
        pytest.skip(
            f"TypeScript SDK build output missing at {TYPESCRIPT_CLIENT_ENTRY}; "
            "run npm test in clients/typescript"
        )

    script = r"""
import { pathToFileURL } from "node:url";

const [clientPath, baseUrl] = process.argv.slice(2);
const { AgentDB, Client, AgentDBError } = await import(pathToFileURL(clientPath).href);

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const db = new AgentDB({ baseUrl });
const workspaceA = await db.workspace("tenant-a-ts-auto");
const workspaceB = await db.workspace("tenant-b-ts-auto");
const agentA = await workspaceA.agent("tenant-a-ts-agent");
const agentB = await workspaceB.agent("tenant-b-ts-agent");
const sessionA = await agentA.session("tenant-a-ts-session");
const sessionB = await agentB.session("tenant-b-ts-session");

const appendedA = await sessionA.append({ label: "ts-a-only" });
const appendedB = await sessionB.append({ label: "ts-b-only" });
assert(appendedA.seq === 0, `expected tenant A first seq 0, got ${appendedA.seq}`);
assert(appendedB.seq === 0, `expected tenant B first seq 0, got ${appendedB.seq}`);

const wrongSession = new Client({ baseUrl }).session(workspaceA.id, sessionB.id);
try {
  await wrongSession.fetch({ fromSeq: 0, limit: 10 });
  throw new Error("cross-tenant TypeScript fetch unexpectedly succeeded");
} catch (err) {
  if (!(err instanceof AgentDBError)) throw err;
  assert([400, 404].includes(err.statusCode), `unexpected cross-tenant status ${err.statusCode}`);
  assert(!JSON.stringify(err.body ?? {}).includes("ts-b-only"), "error body leaked tenant B marker");
}

const watchedA = await workspaceA.watch({ scope: "tenant", limit: 100 });
const watchedPayloadsA = JSON.stringify(watchedA.map((item) => item.event.payload));
assert(watchedPayloadsA.includes("ts-a-only"), "TypeScript tenant A watch missed own marker");
assert(!watchedPayloadsA.includes("ts-b-only"), "TypeScript tenant A watch leaked tenant B marker");

const exportResp = await fetch(
  `${baseUrl}/api/tenants/${encodeURIComponent(workspaceA.id)}/exports/watch?include=events&limit=100`
);
assert(exportResp.ok, `tenant A export failed HTTP ${exportResp.status}`);
const exportBody = await exportResp.json();
const encodedExport = JSON.stringify(exportBody);
assert(encodedExport.includes("ts-a-only"), "tenant A export missed TypeScript marker");
assert(!encodedExport.includes("ts-b-only"), "tenant A export leaked tenant B TypeScript marker");

console.log(JSON.stringify({
  tenantA: workspaceA.id,
  tenantB: workspaceB.id,
  sessionA: sessionA.id,
  sessionB: sessionB.id,
  watchedA: watchedA.length,
}));
"""
    proc = subprocess.run(
        [node, "--input-type=module", "-", str(TYPESCRIPT_CLIENT_ENTRY), base_url],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=20,
        check=False,
    )
    if proc.returncode != 0:
        pytest.fail(
            "TypeScript SDK isolation script failed\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return json.loads(proc.stdout)


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

    wrong_session = Client(live_server.base_url).session(
        sdk_workspace_a.id, sdk_session_b.id
    )
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


def test_tenant_isolation_across_typescript_sdk_and_export_surfaces(
    live_server: LiveServer,
):
    result = _run_typescript_sdk_isolation(live_server.base_url)
    assert result["tenantA"] != result["tenantB"]
    assert result["sessionA"] != result["sessionB"]
    assert result["watchedA"] >= 1


