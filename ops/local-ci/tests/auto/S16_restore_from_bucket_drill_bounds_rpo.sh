#!/usr/bin/env bash
# S16: the object store is a working recovery root — after total loss of the
# data directory, `agentdb restore-from-bucket` rebuilds the node and every
# event acknowledged BEFORE the last successful recovery-manifest backup is
# readable. This is the recovery ladder's cell-loss rung, run as a drill:
# an untested restore path is not a recovery root.
#
# RPO contract checked: loss is bounded by the manifest backup — acked events
# covered by the newest manifest MUST survive; events acked after it MAY be
# lost (that gap is what `recovery_lag_ms` reports).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "${ROOT}"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agentdb-s16.XXXXXX")"
cleanup() {
  status=$?
  kill "${SERVER_PID:-0}" 2>/dev/null || true
  if [[ ${status} -ne 0 ]]; then
    echo "FAILED: work dir preserved for inspection: ${WORK_DIR}" >&2
  else
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
BASE="http://127.0.0.1:${PORT}"

cat > "${WORK_DIR}/agentdb.toml" <<EOF
[server]
bind = "127.0.0.1:${PORT}"

[storage]
data_dir = "${WORK_DIR}/data"

[storage.object_store]
backend = "localfs"
local_root = "${WORK_DIR}/bucket"

[recovery]
manifest_backup_interval_ms = 1000
EOF

cargo build -q -p agentdb-server
BIN="${ROOT}/target/debug/agentdb"

start_server() {
  "${BIN}" serve --config "${WORK_DIR}/agentdb.toml" \
    > "${WORK_DIR}/server-$1.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if curl -sf "${BASE}/health/ready" > /dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "server did not become ready; log tail:" >&2
  tail -20 "${WORK_DIR}/server-$1.log" >&2
  exit 1
}

start_server seed

python3 - "${BASE}" "${WORK_DIR}" <<'PY'
import json
import os
import sys
import time
import urllib.request

base, work_dir = sys.argv[1], sys.argv[2]

bearer = None

def call(path, body=None):
    headers = {"content-type": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())

tenant = call("/api/tenants", {"external_key": "s16"})
agent = call(f"/api/tenants/{tenant['tenant_id']}/agents", {"alias": "s16-agent"})
session = call(
    f"/api/tenants/{tenant['tenant_id']}/agents/{agent['agent_id']}/sessions",
    {"alias": "s16-session"},
)
tid, sid = tenant["tenant_id"], session["session_id"]

# Phase 1: events that must survive (acked before the manifest backup).
for n in range(50):
    call(
        f"/api/tenants/{tid}/sessions/{sid}/events",
        {"agent_id": agent["agent_id"], "kind": "message", "payload": {"n": n}},
    )

# S16-M (metadata-restore): mint a bearer token BEFORE the covered point.
# Tokens are system-tenant records now — they must ride the same recovery
# manifests as data and verify after a bucket-only restore.
minted = call("/api/admin/tokens", {"description": "s16-drill", "scope": "admin"})
with open(os.path.join(work_dir, "token.json"), "w") as f:
    json.dump({"token": minted["token"], "token_id": minted["token_id"]}, f)
# Minting the first token closes the instance; the rest of the drill
# authenticates with it (which also exercises verify-under-traffic).
bearer = minted["token"]

# Wait for a manifest backup covering phase 1.
deadline = time.time() + 15
covered_at = None
while time.time() < deadline:
    status = call("/api/admin/recovery")
    backup = status.get("manifest_backup") or {}
    if backup.get("last_success_ms"):
        covered_at = backup["last_success_ms"]
        break
    time.sleep(0.3)
if covered_at is None:
    print("no manifest backup within 15s", file=sys.stderr)
    sys.exit(1)
# One more interval so the newest manifest definitely includes phase 1.
time.sleep(1.5)
status = call("/api/admin/recovery")
print("recovery status before loss:", json.dumps(status["manifest_backup"]))

# Phase 2: events acked after the covered point — allowed to be lost.
for n in range(50, 60):
    call(
        f"/api/tenants/{tid}/sessions/{sid}/events",
        {"agent_id": agent["agent_id"], "kind": "message", "payload": {"n": n}},
    )

with open(os.path.join(work_dir, "drill.json"), "w") as f:
    json.dump(
        {"tenant_id": tid, "session_id": sid, "must_survive": 50, "total_acked": 60},
        f,
    )
PY

kill -9 "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true

# Total loss of the data directory: the bucket is the only survivor.
rm -rf "${WORK_DIR}/data"

"${BIN}" restore-from-bucket --config "${WORK_DIR}/agentdb.toml" \
  | tee "${WORK_DIR}/restore.log"

start_server verify

python3 - "${BASE}" "${WORK_DIR}" <<'PY'
import json
import sys
import urllib.request

base, work_dir = sys.argv[1], sys.argv[2]
with open(f"{work_dir}/drill.json") as f:
    drill = json.load(f)
tid, sid = drill["tenant_id"], drill["session_id"]
# The token record survived the restore, so the instance is closed:
# authenticate with the pre-loss bearer (itself part of what the drill
# proves).
with open(f"{work_dir}/token.json") as f:
    bearer = json.load(f)["token"]

present = set()
from_seq = 0
while True:
    url = f"{base}/api/tenants/{tid}/sessions/{sid}/events?from_seq={from_seq}&limit=1000"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {bearer}"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        payload = json.loads(resp.read())
    events = payload if isinstance(payload, list) else payload.get("events", payload)
    if not events:
        break
    for e in events:
        present.add(e["seq"])
    from_seq = max(e["seq"] for e in events) + 1

missing_covered = [seq for seq in range(drill["must_survive"]) if seq not in present]
if missing_covered:
    print(
        f"FAIL: {len(missing_covered)} events covered by the recovery manifest "
        f"lost after restore-from-bucket: {missing_covered[:10]}",
        file=sys.stderr,
    )
    sys.exit(1)

lost_uncovered = [
    seq for seq in range(drill["must_survive"], drill["total_acked"]) if seq not in present
]
print(
    f"restore drill: {len(present)} events recovered; all {drill['must_survive']} "
    f"manifest-covered events survived; {len(lost_uncovered)} post-manifest events "
    f"lost (within the recovery_lag_ms RPO contract)"
)
PY

# S16-M: the pre-loss token must verify against the restored node (the
# instance is closed post-restore because the token record survived, so an
# unauthenticated probe must get 401 and the token must get 200).
python3 - "${BASE}" "${WORK_DIR}" <<'PY'
import json, os, sys, urllib.request

base, work_dir = sys.argv[1], sys.argv[2]
with open(os.path.join(work_dir, "token.json")) as f:
    token = json.load(f)["token"]

def status_of(path, bearer=None):
    req = urllib.request.Request(base + path)
    if bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code

unauth = status_of("/api/tenants")
if unauth != 401:
    print(f"restored node should be closed (token exists) but got {unauth}", file=sys.stderr)
    sys.exit(1)
authed = status_of("/api/tenants", bearer=token)
if authed != 200:
    print(f"pre-loss token failed to verify after restore: {authed}", file=sys.stderr)
    sys.exit(1)
print("S16-M: pre-loss bearer token survived the bucket-only restore")
PY

kill "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true


echo "PASS: restore-from-bucket rebuilt the node with manifest-bounded RPO"
