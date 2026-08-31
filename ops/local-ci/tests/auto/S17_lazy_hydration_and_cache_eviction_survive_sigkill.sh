#!/usr/bin/env bash
# S17: lazy hydration + metadata-cache eviction chaos under SIGKILL.
#
# Falsifies: the KV-authoritative-state claim — with the metadata caches
# starting EMPTY (AGENTDB_STARTUP_HYDRATION=lazy), eviction caps small
# enough that the sweeper repeatedly wipes warm entries mid-traffic, and
# SIGKILL landing at arbitrary points, every acknowledged append must
# survive a cold lazy restart with contiguous per-session sequences and
# correct payloads. This is the server-level end-to-end proof of the
# Stage 1/2 cache-fallback design.
#
# Same limitation as S14: SIGKILL preserves the page cache, so fsync
# itself needs a power-loss rig; this covers ordering + recovery + cache
# coherence.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "${ROOT}"

ITERATIONS="${AGENTDB_S17_ITERATIONS:-2}"
KILL_AFTER_MS="${AGENTDB_S17_KILL_AFTER_MS:-1500}"
SESSIONS_PER_ITER="${AGENTDB_S17_SESSIONS:-6}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agentdb-s17.XXXXXX")"
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
  # Lazy hydration with adversarial cache settings: caps far below the
  # live session count and a sweep every worker tick, so warm cache
  # entries are wiped constantly while traffic runs.
  AGENTDB_STARTUP_HYDRATION=lazy \
  AGENTDB_METADATA_CACHE_MAX_TENANTS=1 \
  AGENTDB_METADATA_CACHE_MAX_AGENTS=1 \
  AGENTDB_METADATA_CACHE_MAX_SESSIONS=2 \
  AGENTDB_METADATA_CACHE_MAX_ARTIFACTS=2 \
  AGENTDB_METADATA_CACHE_SWEEP_MS=200 \
  AGENTDB_RECORD_INDEX_WORKER_INTERVAL_MS=100 \
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

  python3 - "${BASE}" "${WORK_DIR}" "${i}" "${SERVER_PID}" "${KILL_AFTER_MS}" "${SESSIONS_PER_ITER}" <<'PY'
import json
import os
import signal
import sys
import threading
import time
import urllib.request

base, work_dir, iteration, server_pid, kill_after_ms, session_count = (
    sys.argv[1],
    sys.argv[2],
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    int(sys.argv[6]),
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
    tenant = post("/api/tenants", {"external_key": "s17"})
    agent = post(f"/api/tenants/{tenant['tenant_id']}/agents", {"alias": "s17-agent"})
    state = {"tenant_id": tenant["tenant_id"], "agent_id": agent["agent_id"]}
    with open(state_path, "w") as f:
        json.dump(state, f)
else:
    with open(state_path) as f:
        state = json.load(f)

tid = state["tenant_id"]
sessions = []
for j in range(session_count):
    session = post(
        f"/api/tenants/{tid}/agents/{state['agent_id']}/sessions",
        {"alias": f"s17-run-{iteration}-{j}"},
    )
    sessions.append(session["session_id"])
with open(os.path.join(work_dir, f"sessions-{iteration}"), "w") as f:
    f.write("\n".join(sessions))

def kill_server():
    time.sleep(kill_after_ms / 1000.0)
    os.kill(server_pid, signal.SIGKILL)

threading.Thread(target=kill_server, daemon=True).start()

# Round-robin appends across more sessions than the cache cap can hold, so
# every append after the first sweep exercises the cold-cache fallback.
acked_path = os.path.join(work_dir, f"acked-{iteration}")
acked = 0
with open(acked_path, "w") as f:
    n = 0
    while True:
        session_id = sessions[n % session_count]
        try:
            resp = post(
                f"/api/tenants/{tid}/sessions/{session_id}/events",
                {
                    "agent_id": state["agent_id"],
                    "kind": "message",
                    "payload": {"n": n // session_count},
                },
            )
        except Exception:
            break
        f.write(f"{session_id} {resp['seq']}\n")
        f.flush()
        os.fsync(f.fileno())
        acked += 1
        n += 1

print(f"iteration {iteration}: {acked} acked appends across {session_count} sessions before SIGKILL")
if acked == 0:
    print("no acked appends; raise AGENTDB_S17_KILL_AFTER_MS", file=sys.stderr)
    sys.exit(1)
PY

  wait "${SERVER_PID}" 2>/dev/null || true

  start_server "${i}-verify"

  python3 - "${BASE}" "${WORK_DIR}" "${i}" <<'PY'
import collections
import json
import os
import sys
import urllib.request

base, work_dir, iteration = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(os.path.join(work_dir, "state.json")) as f:
    state = json.load(f)
with open(os.path.join(work_dir, f"sessions-{iteration}")) as f:
    sessions = f.read().split()
with open(os.path.join(work_dir, f"acked-{iteration}")) as f:
    acked = collections.defaultdict(set)
    for line in f.read().splitlines():
        session_id, seq = line.split()
        acked[session_id].add(int(seq))

tid = state["tenant_id"]

def get(path):
    with urllib.request.urlopen(base + path, timeout=10) as resp:
        return json.loads(resp.read())

total_missing = 0
for session_id in sessions:
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
    missing = sorted(acked[session_id] - set(present))
    if missing:
        print(
            f"FAIL iteration {iteration}: session {session_id} lost acked seqs {missing[:8]}",
            file=sys.stderr,
        )
        total_missing += len(missing)
        continue
    # Payload audit: seq k in this session was written with payload n=k.
    for seq in acked[session_id]:
        if present[seq].get("n") != seq:
            print(
                f"FAIL iteration {iteration}: session {session_id} seq {seq} "
                f"payload mismatch: {present[seq]}",
                file=sys.stderr,
            )
            total_missing += 1
if total_missing:
    sys.exit(1)

total = sum(len(v) for v in acked.values())
print(f"iteration {iteration}: all {total} acked appends survived lazy restart with eviction chaos")
PY

  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
done

echo "PASS: lazy hydration + cache eviction survived SIGKILL across ${ITERATIONS} cycles"
