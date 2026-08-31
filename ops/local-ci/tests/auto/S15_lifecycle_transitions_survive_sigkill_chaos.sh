#!/usr/bin/env bash
# S15: tiered-storage lifecycle chaos — warm/cold/archive/restore transitions
# interleaved with appends survive SIGKILL at arbitrary points.
#
# Falsifies: (a) seal-to-cold and compact-to-archive move history without
# losing acknowledged events; (b) a crash mid-transition leaves the session
# readable and append-able after cold restart; (c) reopened warm heads keep
# the sequence contiguous across crashes. Each iteration runs a driver that
# appends and cycles lifecycle states as fast as the server acks, records
# every acked seq durably, and gets SIGKILLed mid-stream; after restart the
# full merged event stream (archive + segments + warm head) must contain
# every acked seq, and the session must accept a fresh append.
#
# Same limitation as S14: SIGKILL cannot falsify fsync (page cache survives);
# this is torn-state recovery + ack-ordering coverage for the tiering path.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "${ROOT}"

ITERATIONS="${AGENTDB_S15_ITERATIONS:-3}"
KILL_AFTER_MS="${AGENTDB_S15_KILL_AFTER_MS:-1500}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agentdb-s15.XXXXXX")"
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
import random
import signal
import sys
import threading
import time
import urllib.request

base, work_dir, iteration, server_pid, kill_after_ms = (
    sys.argv[1],
    sys.argv[2],
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
)
rng = random.Random(1000 + iteration)

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
    tenant = post("/api/tenants", {"external_key": "s15"})
    agent = post(f"/api/tenants/{tenant['tenant_id']}/agents", {"alias": "s15-agent"})
    state = {"tenant_id": tenant["tenant_id"], "agent_id": agent["agent_id"]}
    with open(state_path, "w") as f:
        json.dump(state, f)
else:
    with open(state_path) as f:
        state = json.load(f)

session = post(
    f"/api/tenants/{state['tenant_id']}/agents/{state['agent_id']}/sessions",
    {"alias": f"s15-run-{iteration}"},
)
session_id = session["session_id"]
with open(os.path.join(work_dir, f"session-{iteration}"), "w") as f:
    f.write(session_id)

def kill_server():
    time.sleep(kill_after_ms / 1000.0)
    os.kill(server_pid, signal.SIGKILL)

threading.Thread(target=kill_server, daemon=True).start()

# Chaos driver: mostly appends, with lifecycle transitions sprinkled in so
# the SIGKILL lands around seal/compact/restore boundaries. Only *acked*
# operations go into the ground-truth file.
acked_path = os.path.join(work_dir, f"acked-{iteration}")
tid = state["tenant_id"]
transitions = 0
with open(acked_path, "w") as f:
    n = 0
    while True:
        try:
            roll = rng.random()
            if roll < 0.85 or n == 0:
                resp = post(
                    f"/api/tenants/{tid}/sessions/{session_id}/events",
                    {
                        "agent_id": state["agent_id"],
                        "kind": "message",
                        "payload": {"n": n},
                    },
                )
                f.write(f"{resp['seq']}\n")
                f.flush()
                os.fsync(f.fileno())
                n += 1
            elif roll < 0.93:
                resp = post(
                    f"/api/tenants/{tid}/sessions/{session_id}/lifecycle",
                    {"target": "cold"},
                )
                f.write(f"# cold -> {json.dumps(resp)}\n")
                f.flush()
                os.fsync(f.fileno())
                transitions += 1
            elif roll < 0.97:
                resp = post(
                    f"/api/tenants/{tid}/sessions/{session_id}/lifecycle",
                    {"target": "archive"},
                )
                f.write(f"# archive -> {json.dumps(resp)}\n")
                f.flush()
                os.fsync(f.fileno())
                transitions += 1
            else:
                resp = post(
                    f"/api/tenants/{tid}/sessions/{session_id}/lifecycle",
                    {"target": "warm"},
                )
                f.write(f"# warm -> {json.dumps(resp)}\n")
                f.flush()
                os.fsync(f.fileno())
                transitions += 1
        except Exception:
            break

print(f"iteration {iteration}: {n} acked appends, {transitions} lifecycle transitions before SIGKILL")
if n == 0:
    print("no acked appends; raise AGENTDB_S15_KILL_AFTER_MS", file=sys.stderr)
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
    acked = [
        int(line)
        for line in f.read().splitlines()
        if line.strip().isdigit()
    ]

tid = state["tenant_id"]

def get(path):
    with urllib.request.urlopen(base + path, timeout=10) as resp:
        return json.loads(resp.read())

# The merged logical stream (archive bundle + sealed segments + warm head)
# must contain every acked seq with the right payload, regardless of which
# physical tier the crash left each range in.
present = {}
from_seq = 0
while True:
    payload = get(
        f"/api/tenants/{tid}/sessions/{session_id}/events?from_seq={from_seq}&limit=1000"
    )
    events = payload if isinstance(payload, list) else payload.get("events", payload)
    if not events:
        break
    for e in events:
        present[e["seq"]] = e.get("payload", {})
    from_seq = max(e["seq"] for e in events) + 1

missing = [seq for seq in acked if seq not in present]
if missing:
    print(
        f"FAIL iteration {iteration}: {len(missing)} acked events unreadable after "
        f"lifecycle-chaos crash: {missing[:10]}",
        file=sys.stderr,
    )
    sys.exit(1)

# Green-but-broken audit: payloads must match the append order, not merely
# exist. Every acked seq n was written with payload {"n": n}.
corrupt = [seq for seq in acked if present[seq].get("n") != seq]
if corrupt:
    print(f"FAIL iteration {iteration}: payload mismatch at seqs {corrupt[:10]}", file=sys.stderr)
    sys.exit(1)

# The session must accept a fresh append after crash recovery (warm-head
# reopen over whatever tier state the crash left behind).
def post(path, body):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())

resp = post(
    f"/api/tenants/{tid}/sessions/{session_id}/events",
    {"agent_id": state["agent_id"], "kind": "message", "payload": {"post": "recovery"}},
)
expected_next = max(present) + 1 if present else 0
if resp["seq"] != expected_next:
    print(
        f"FAIL iteration {iteration}: post-recovery append got seq {resp['seq']}, "
        f"expected contiguous {expected_next}",
        file=sys.stderr,
    )
    sys.exit(1)

print(
    f"iteration {iteration}: all {len(acked)} acked events readable across tiers; "
    f"post-recovery append landed at seq {resp['seq']}"
)
PY

  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
done

echo "PASS: lifecycle transitions survived SIGKILL chaos across ${ITERATIONS} cycles"
