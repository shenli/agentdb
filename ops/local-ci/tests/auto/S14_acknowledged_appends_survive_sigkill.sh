#!/usr/bin/env bash
# S14: an acknowledged append survives SIGKILL at any instant, on default
# configuration.
#
# Falsifies: (a) the ack-ordering claim — an ack is issued only after the
# event's WAL write reached the kernel; (b) WAL recovery — a cold reopen from
# a torn mid-stream crash state reconstructs every acknowledged event. The
# client appends as fast as the server acks, recording every acked seq; the
# server is SIGKILLed mid-stream; after a cold restart from the same data
# directory every acked seq must be present. Unacked seqs MAY be present
# (the write can land before the ack is sent) — only acked-and-missing fails.
#
# Known limitation: SIGKILL does not drop the OS page cache, so this harness
# cannot distinguish fsync-on from fsync-off — the fsync default protects
# against power loss / host crash, which requires a VM-kill or dm-flakey rig
# (Linux) to falsify. This is the strongest crash test available on a dev
# laptop and in portable CI.
#
# Runs AGENTDB_S14_ITERATIONS crash cycles (default 3) against one data dir.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "${ROOT}"

ITERATIONS="${AGENTDB_S14_ITERATIONS:-3}"
KILL_AFTER_MS="${AGENTDB_S14_KILL_AFTER_MS:-1500}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agentdb-s14.XXXXXX")"
trap 'kill "${SERVER_PID:-0}" 2>/dev/null || true; rm -rf "${WORK_DIR}"' EXIT

PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
BASE="http://127.0.0.1:${PORT}"

cat > "${WORK_DIR}/agentdb.toml" <<EOF
[server]
bind = "127.0.0.1:${PORT}"

[storage]
data_dir = "${WORK_DIR}/data"
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

for i in $(seq 1 "${ITERATIONS}"); do
  start_server "${i}"

  python3 - "${BASE}" "${WORK_DIR}" "${i}" "${SERVER_PID}" "${KILL_AFTER_MS}" <<'PY'
import json
import os
import signal
import sys
import threading
import time
import urllib.request
import urllib.error

base, work_dir, iteration, server_pid, kill_after_ms = (
    sys.argv[1],
    sys.argv[2],
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
)

def post(path, body):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())

state_path = os.path.join(work_dir, "state.json")
if iteration == 1:
    tenant = post("/api/tenants", {"external_key": "s14"})
    agent = post(
        f"/api/tenants/{tenant['tenant_id']}/agents", {"alias": "s14-agent"}
    )
    state = {"tenant_id": tenant["tenant_id"], "agent_id": agent["agent_id"]}
    with open(state_path, "w") as f:
        json.dump(state, f)
else:
    with open(state_path) as f:
        state = json.load(f)

session = post(
    f"/api/tenants/{state['tenant_id']}/agents/{state['agent_id']}/sessions",
    {"alias": f"s14-run-{iteration}"},
)
session_id = session["session_id"]
with open(os.path.join(work_dir, f"session-{iteration}"), "w") as f:
    f.write(session_id)

# The killer fires while appends are in flight, so the crash lands at an
# arbitrary point in the ack stream.
def kill_server():
    time.sleep(kill_after_ms / 1000.0)
    os.kill(server_pid, signal.SIGKILL)

threading.Thread(target=kill_server, daemon=True).start()

acked_path = os.path.join(work_dir, f"acked-{iteration}")
acked = 0
with open(acked_path, "w") as f:
    n = 0
    while True:
        try:
            resp = post(
                f"/api/tenants/{state['tenant_id']}/sessions/{session_id}/events",
                {
                    "agent_id": state["agent_id"],
                    "kind": "message",
                    "payload": {"n": n},
                },
            )
        except Exception:
            break
        # Record the ack durably before issuing the next append: this file is
        # the ground truth the survival check compares against.
        f.write(f"{resp['seq']}\n")
        f.flush()
        os.fsync(f.fileno())
        acked += 1
        n += 1

print(f"iteration {iteration}: {acked} acked appends before SIGKILL")
if acked == 0:
    print("iteration produced no acked appends; raise AGENTDB_S14_KILL_AFTER_MS", file=sys.stderr)
    sys.exit(1)
PY

  wait "${SERVER_PID}" 2>/dev/null || true

  start_server "${i}-verify"

  python3 - "${BASE}" "${WORK_DIR}" "${i}" <<'PY'
import json
import os
import sys
import urllib.request

base, work_dir, iteration = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(os.path.join(work_dir, "state.json")) as f:
    state = json.load(f)
with open(os.path.join(work_dir, f"session-{iteration}")) as f:
    session_id = f.read().strip()
with open(os.path.join(work_dir, f"acked-{iteration}")) as f:
    acked = [int(line) for line in f.read().split()]

present = set()
from_seq = 0
while True:
    url = (
        f"{base}/api/tenants/{state['tenant_id']}/sessions/{session_id}/events"
        f"?from_seq={from_seq}&limit=1000"
    )
    with urllib.request.urlopen(url, timeout=5) as resp:
        payload = json.loads(resp.read())
    events = payload if isinstance(payload, list) else payload.get("events", payload)
    if not events:
        break
    for e in events:
        present.add(e["seq"])
    from_seq = max(e["seq"] for e in events) + 1

missing = [seq for seq in acked if seq not in present]
if missing:
    print(
        f"FAIL iteration {iteration}: {len(missing)} acknowledged appends lost "
        f"after SIGKILL: seqs {missing[:10]}{'...' if len(missing) > 10 else ''}",
        file=sys.stderr,
    )
    sys.exit(1)
extra = len(present) - len(acked)
print(
    f"iteration {iteration}: all {len(acked)} acked appends survived "
    f"({extra} unacked write(s) also landed)"
)
PY

  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
done

echo "PASS: acknowledged appends survived SIGKILL across ${ITERATIONS} crash cycles"
