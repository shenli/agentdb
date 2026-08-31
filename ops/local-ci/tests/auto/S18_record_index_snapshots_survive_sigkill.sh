#!/usr/bin/env bash
# S18: record-index snapshots + delta replay under SIGKILL.
#
# Falsifies: the snapshot-restore claim — with snapshots written on nearly
# every worker tick (min advance 1, 100ms interval) while record traffic
# (creates, updates, deletes) is running, and SIGKILL landing at arbitrary
# points including mid-snapshot-write, a restart must restore the indexes
# from whatever snapshot generation survived and delta-replay the rest:
# every acked record write is searchable with its latest text, every acked
# delete stays gone, and external-id dedupe still holds.
#
# Same limitation as S14/S17: SIGKILL preserves the page cache, so fsync
# itself needs a power-loss rig; this covers snapshot atomicity + replay.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "${ROOT}"

ITERATIONS="${AGENTDB_S18_ITERATIONS:-2}"
KILL_AFTER_MS="${AGENTDB_S18_KILL_AFTER_MS:-1500}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agentdb-s18.XXXXXX")"
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
  # Snapshot on almost every tick so the SIGKILL window reliably overlaps
  # snapshot writes; lazy hydration so restarts actually exercise the
  # snapshot-restore path instead of masking it with resident rows.
  AGENTDB_STARTUP_HYDRATION=lazy \
  AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE=1 \
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

  python3 - "${BASE}" "${WORK_DIR}" "${i}" "${SERVER_PID}" "${KILL_AFTER_MS}" <<'PY'
import json
import os
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

def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path,
        data=data,
        headers={"content-type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None

state_path = os.path.join(work_dir, "state.json")
if iteration == 1:
    tenant = call("POST", "/api/tenants", {"external_key": "s18"})
    state = {"tenant_id": tenant["tenant_id"]}
    with open(state_path, "w") as f:
        json.dump(state, f)
else:
    with open(state_path) as f:
        state = json.load(f)
tid = state["tenant_id"]

def kill_server():
    time.sleep(kill_after_ms / 1000.0)
    os.kill(server_pid, signal.SIGKILL)

threading.Thread(target=kill_server, daemon=True).start()

# Mixed record traffic journal: "up <ext> <gen> <record_id>" for an acked
# upsert (text embeds ext+gen), "del <ext>" for an acked delete.
# Round-robins through a keyspace larger than one snapshot interval can
# settle, so every snapshot generation is mid-churn when SIGKILL lands.
acked_path = os.path.join(work_dir, f"acked-{iteration}")
records_base = f"/api/tenants/{tid}/records"
last_id = {}
ops = 0
with open(acked_path, "w") as f:
    n = 0
    while True:
        ext = f"s18i{iteration}n{n % 24}"
        gen = n // 24
        try:
            if n % 6 == 5 and ext in last_id:
                call("DELETE", f"{records_base}/{last_id.pop(ext)}")
                f.write(f"del {ext}\n")
            else:
                written = call(
                    "POST",
                    f"{records_base}/upsert",
                    {
                        "external_id": ext,
                        "namespace": ["s18"],
                        "text": f"payload {ext} generation {gen}",
                        "metadata": {"gen": gen},
                        "policy": {"mode": "summary_only", "vector_allowed": False},
                    },
                )
                last_id[ext] = written["record_id"]
                f.write(f"up {ext} {gen} {written['record_id']}\n")
        except Exception:
            break
        f.flush()
        os.fsync(f.fileno())
        ops += 1
        n += 1

print(f"iteration {iteration}: {ops} acked record ops before SIGKILL")
if ops == 0:
    print("no acked record ops; raise AGENTDB_S18_KILL_AFTER_MS", file=sys.stderr)
    sys.exit(1)
PY

  wait "${SERVER_PID}" 2>/dev/null || true

  start_server "${i}-verify"

  python3 - "${BASE}" "${WORK_DIR}" "${i}" <<'PY'
import json
import os
import sys
import urllib.request
import urllib.parse

base, work_dir, iteration = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(os.path.join(work_dir, "state.json")) as f:
    state = json.load(f)
tid = state["tenant_id"]

# Replay the journal to the expected final state per external id.
expected = {}
with open(os.path.join(work_dir, f"acked-{iteration}")) as f:
    for line in f.read().splitlines():
        parts = line.split()
        if parts[0] == "up":
            expected[parts[1]] = int(parts[2])
        else:
            expected[parts[1]] = None

def get(path):
    try:
        with urllib.request.urlopen(base + path, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} on {path}: {e.read().decode()}", file=sys.stderr)
        raise

# No-text search = brute-force enumeration of live records in the namespace.
listing = get(f"/api/tenants/{tid}/records/search?namespace=s18&limit=100")
live_by_ext = {}
for m in listing["matches"]:
    r = m["record"]
    ext = r.get("external_id")
    if ext and ext.startswith(f"s18i{iteration}n"):
        live_by_ext.setdefault(ext, []).append(r)

# The traffic loop is sequential and deterministic: exactly one op (the
# n-th, where n = number of journal lines) can have committed without an
# ack. Skip strict checks for that one external id.
acked_ops = sum(1 for _ in open(os.path.join(work_dir, f"acked-{iteration}")))
inflight_ext = f"s18i{iteration}n{acked_ops % 24}"

failures = 0
for ext, gen in sorted(expected.items()):
    rows = live_by_ext.get(ext, [])
    if ext == inflight_ext:
        continue
    if gen is None:
        if rows:
            print(f"FAIL: {ext} acked-deleted but still live", file=sys.stderr)
            failures += 1
        continue
    if len(rows) != 1:
        print(
            f"FAIL: {ext} expected exactly one live record, got {len(rows)}",
            file=sys.stderr,
        )
        failures += 1
        continue
    got = rows[0].get("metadata", {}).get("gen")
    # The journal only records ACKED ops; a crash can land newer un-acked
    # generations, so the live generation must be >= the acked one.
    if got is None or got < gen:
        print(
            f"FAIL: {ext} acked generation {gen} but live row has {got}",
            file=sys.stderr,
        )
        failures += 1
        continue
    # Text search must find this record by its external id token.
    q = urllib.parse.quote(ext)
    hits = get(f"/api/tenants/{tid}/records/search?text={q}&namespace=s18&limit=10")
    if not any(m["record"].get("external_id") == ext for m in hits["matches"]):
        print(f"FAIL: {ext} live but not searchable after restart", file=sys.stderr)
        failures += 1

if failures:
    sys.exit(1)
print(
    f"iteration {iteration}: {len(expected)} externals verified "
    f"(live text-searchable, deletes stayed gone) after snapshot restore"
)
PY

  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
done

echo "PASS: record-index snapshots + delta replay survived SIGKILL across ${ITERATIONS} cycles"
