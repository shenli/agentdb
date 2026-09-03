#!/usr/bin/env bash
# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S12 single_node_upgrade_preserves_runtime_state_and_client_compatibility
# Falsifies: C4, C6, C12, C16, C20, C21
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts"

if [[ -z "${AGENTDB_OLD_SERVER_BIN:-}" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_OLD_SERVER_BIN to an older agentdb binary to run upgrade compatibility"
  exit 0
fi

if [[ ! -x "${AGENTDB_OLD_SERVER_BIN}" ]]; then
  echo "INCONCLUSIVE: AGENTDB_OLD_SERVER_BIN is not executable: ${AGENTDB_OLD_SERVER_BIN}"
  exit 0
fi

NEW_SERVER_BIN="${AGENTDB_NEW_SERVER_BIN:-${ROOT}/target/debug/agentdb}"
if [[ ! -x "${NEW_SERVER_BIN}" ]]; then
  echo "INCONCLUSIVE: AGENTDB_NEW_SERVER_BIN is not executable: ${NEW_SERVER_BIN}"
  exit 0
fi

BIND="${AGENTDB_S12_BIND:-127.0.0.1:32120}"
BASE_URL="http://${BIND}"
WORK_DIR="${AGENTDB_S12_WORK_DIR:-${SESSION_DIR}/artifacts/S12-upgrade}"
DATA_DIR="${WORK_DIR}/data"
CONTENT_DIR="${WORK_DIR}/content"
STATE_FILE="${WORK_DIR}/state.json"
METRICS_FILE="${WORK_DIR}/metrics.json"
OLD_LOG="${SESSION_DIR}/logs/S12-old-server.log"
NEW_LOG="${SESSION_DIR}/logs/S12-new-server.log"
CLIENT_LOG="${SESSION_DIR}/logs/S12-upgrade-client.log"
PID=""

case "${WORK_DIR}" in
  *S12* | *s12* | *upgrade*) rm -rf "${WORK_DIR}" ;;
  *)
    echo "Refusing to delete AGENTDB_S12_WORK_DIR without S12/upgrade marker: ${WORK_DIR}" >&2
    exit 2
    ;;
esac
mkdir -p "${WORK_DIR}" "${DATA_DIR}" "${CONTENT_DIR}"
: >"${CLIENT_LOG}"

stop_server() {
  if [[ -n "${PID}" ]] && kill -0 "${PID}" 2>/dev/null; then
    kill "${PID}" 2>/dev/null || true
    wait "${PID}" 2>/dev/null || true
  fi
  PID=""
}
trap stop_server EXIT

wait_ready() {
  local base_url="$1"
  python3 - "${base_url}" <<'PY'
import sys
import time
import urllib.error
import urllib.request

base = sys.argv[1]
deadline = time.time() + 30
last = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"{base}/health/ready", timeout=1.0) as resp:
            if resp.status == 200:
                sys.exit(0)
            last = f"HTTP {resp.status}"
    except Exception as exc:
        last = repr(exc)
    time.sleep(0.25)
raise SystemExit(f"server did not become ready at {base}: {last}")
PY
}

run_client_phase() {
  local phase="$1"
  python3 - "${BASE_URL}" "${STATE_FILE}" "${METRICS_FILE}" "${phase}" "${ROOT}" <<'PY' 2>&1 | tee -a "${CLIENT_LOG}"
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

base, state_path, metrics_path, phase, root = sys.argv[1:6]
sys.path.insert(0, str(pathlib.Path(root) / "clients" / "python"))

def request(method, path, body=None, headers=None):
    data = None
    req_headers = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        f"{base}{path}",
        data=data,
        headers=req_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise AssertionError(f"{method} {path} failed HTTP {exc.code}: {detail}") from exc

def append_event(tenant_id, agent_id, session_id, seq, marker):
    event = request(
        "POST",
        f"/api/tenants/{tenant_id}/sessions/{session_id}/events",
        {
            "agent_id": agent_id,
            "kind": "message",
            "payload": {"seq": seq, "marker": marker},
            "artifact_refs": [],
            "source_refs": [],
        },
        {"Idempotency-Key": f"s12-upgrade-{session_id}-{seq}"},
    )
    if event.get("seq") != seq:
        raise AssertionError(f"append expected seq {seq}, got {event}")
    return event

def fetch_events(tenant_id, session_id):
    return request(
        "GET",
        f"/api/tenants/{tenant_id}/sessions/{session_id}/events?from_seq=0&limit=100",
    )

def assert_events(tenant_id, session_id, expected):
    events = fetch_events(tenant_id, session_id)
    seqs = [event.get("seq") for event in events]
    if seqs != expected:
        raise AssertionError(f"expected fetched seqs {expected}, got {seqs}: {events}")
    return events

def item_seq(item):
    if isinstance(item, dict):
        if isinstance(item.get("seq"), int):
            return item["seq"]
        event = item.get("event")
        if isinstance(event, dict) and isinstance(event.get("seq"), int):
            return event["seq"]
    return None

def assert_watch_contains(tenant_id, session_id, expected):
    items = request(
        "GET",
        f"/api/tenants/{tenant_id}/watch?scope=session&session_id={session_id}&limit=100",
    )
    seqs = {item_seq(item) for item in items}
    missing = [seq for seq in expected if seq not in seqs]
    if missing:
        raise AssertionError(f"watch missing seqs {missing}; response={items}")
    return items

def assert_export_contains(tenant_id, session_id, expected):
    page = request(
        "GET",
        f"/api/tenants/{tenant_id}/changes?scope=session&session_id={session_id}&include=events&limit=100",
    )
    items = page.get("items", [])
    seqs = {
        item.get("seq")
        for item in items
        if item.get("object_type") == "event" and item.get("op") == "insert"
    }
    missing = [seq for seq in expected if seq not in seqs]
    if missing:
        raise AssertionError(f"export watch missing seqs {missing}; response={page}")
    return page

def write_metrics(state, phase_result):
    metrics = {}
    try:
        with open(metrics_path, "r", encoding="utf-8") as handle:
            metrics = json.load(handle)
    except FileNotFoundError:
        pass
    metrics[phase] = phase_result
    metrics["state"] = state
    with open(metrics_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")

def assert_python_sdk_compat(state, expected_before):
    from agentdb import Client

    client = Client(base)
    session = client.session(
        state["tenant_id"],
        state["session_id"],
        agent_id=state["agent_id"],
    )
    before = session.fetch(from_seq=0, limit=100)
    before_seqs = [event.seq for event in before]
    if before_seqs != expected_before:
        raise AssertionError(f"Python SDK expected seqs {expected_before}, got {before_seqs}")
    result = session.append(
        {"seq": 3, "marker": "new-python-sdk"},
        idempotency_key=f"s12-python-sdk-{state['session_id']}-3",
    )
    if result.seq != 3:
        raise AssertionError(f"Python SDK append expected seq 3, got {result.seq}")
    after = session.fetch(from_seq=0, limit=100)
    after_seqs = [event.seq for event in after]
    if after_seqs != [0, 1, 2, 3]:
        raise AssertionError(f"Python SDK expected seqs [0,1,2,3], got {after_seqs}")
    return {"before": before_seqs, "after": after_seqs}


if phase == "old-write":
    tenant_external_key = f"s12-upgrade-{int(time.time())}"
    tenant = request("POST", "/api/tenants", {"external_key": tenant_external_key, "policy": {}})
    tenant_id = tenant["tenant_id"]
    agent = request("POST", f"/api/tenants/{tenant_id}/agents", {"alias": "s12-agent", "config": {}})
    agent_id = agent["agent_id"]
    session = request(
        "POST",
        f"/api/tenants/{tenant_id}/agents/{agent_id}/sessions",
        {"alias": "s12-session"},
    )
    session_id = session["session_id"]
    append_event(tenant_id, agent_id, session_id, 0, "old")
    append_event(tenant_id, agent_id, session_id, 1, "old")
    events = assert_events(tenant_id, session_id, [0, 1])
    assert_watch_contains(tenant_id, session_id, [0, 1])
    assert_export_contains(tenant_id, session_id, [0, 1])
    state = {
        "tenant_external_key": tenant_external_key,
        "tenant_id": tenant_id,
        "agent_id": agent_id,
        "session_id": session_id,
    }
    with open(state_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")
    write_metrics(state, {"fetched": len(events), "verified_seqs": [0, 1]})
    print(f"old-write verified tenant={tenant_id} session={session_id} seqs=[0,1]")
elif phase == "new-verify":
    with open(state_path, "r", encoding="utf-8") as handle:
        state = json.load(handle)
    tenant_id = state["tenant_id"]
    agent_id = state["agent_id"]
    session_id = state["session_id"]
    before = assert_events(tenant_id, session_id, [0, 1])
    assert_watch_contains(tenant_id, session_id, [0, 1])
    assert_export_contains(tenant_id, session_id, [0, 1])
    append_event(tenant_id, agent_id, session_id, 2, "new")
    after = assert_events(tenant_id, session_id, [0, 1, 2])
    assert_watch_contains(tenant_id, session_id, [0, 1, 2])
    assert_export_contains(tenant_id, session_id, [0, 1, 2])
    sdk_result = assert_python_sdk_compat(state, [0, 1, 2])
    assert_events(tenant_id, session_id, [0, 1, 2, 3])
    assert_watch_contains(tenant_id, session_id, [0, 1, 2, 3])
    assert_export_contains(tenant_id, session_id, [0, 1, 2, 3])
    final_expected = [0, 1, 2, 3]
    final_events = assert_events(tenant_id, session_id, final_expected)
    assert_watch_contains(tenant_id, session_id, final_expected)
    assert_export_contains(tenant_id, session_id, final_expected)
    write_metrics(
        state,
        {
            "before": len(before),
            "after": len(after),
            "final": len(final_events),
            "verified_seqs": final_expected,
            "python_sdk": sdk_result,
        },
    )
    print(f"new-verify verified tenant={tenant_id} session={session_id} seqs={final_expected}")
else:
    raise SystemExit(f"unknown phase {phase}")
PY
}

echo "S12: starting old server ${AGENTDB_OLD_SERVER_BIN} on ${BIND}" | tee -a "${CLIENT_LOG}"
"${AGENTDB_OLD_SERVER_BIN}" serve \
  --bind "${BIND}" \
  --data-dir "${DATA_DIR}" \
  --content-dir "${CONTENT_DIR}" \
  >"${OLD_LOG}" 2>&1 &
PID="$!"
wait_ready "${BASE_URL}"
run_client_phase old-write
stop_server

echo "S12: starting new server ${NEW_SERVER_BIN} on ${BIND}" | tee -a "${CLIENT_LOG}"
new_server_args=(
  serve
  --bind "${BIND}" \
  --data-dir "${DATA_DIR}" \
  --content-dir "${CONTENT_DIR}"
)
"${NEW_SERVER_BIN}" "${new_server_args[@]}" >"${NEW_LOG}" 2>&1 &
PID="$!"
wait_ready "${BASE_URL}"
run_client_phase new-verify
stop_server

echo "PASS: single-node upgrade preserved fetch/watch/export state and accepted post-upgrade Python SDK appends" | tee -a "${CLIENT_LOG}"
