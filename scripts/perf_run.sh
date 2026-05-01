#!/usr/bin/env bash
# Perf harness: start agentdb-server + Go workload, sample admin API
# every second for DURATION seconds, write a CSV.
#
# Usage: scripts/perf_run.sh <label> <duration_sec>
#
# Env vars that influence tuning:
#   AGENTDB_T_COLD_SECS (default 60) — tier demotion threshold
#   AGENTDB_TICK_SECS   (default 2)  — lifecycle scan interval
set -euo pipefail

LABEL="${1:-run}"
DURATION="${2:-60}"

PORT_HTTP=${PORT_HTTP:-7878}
DATA_DIR=$(mktemp -d -t agentdb-perf-XXXXXX)
OBJ_DIR=$(mktemp -d -t agentdb-perf-obj-XXXXXX)
OUT_DIR="perf-out"
mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/${LABEL}.csv"

cleanup() {
    for pid in "${WORKLOAD_PID:-}" "${SERVER_PID:-}"; do
        [[ -n "$pid" ]] || continue
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$DATA_DIR" "$OBJ_DIR"
}
trap cleanup EXIT

echo "label=$LABEL duration=${DURATION}s data=$DATA_DIR obj=$OBJ_DIR"

AGENTDB_DATA="$DATA_DIR" \
AGENTDB_OBJECTS="$OBJ_DIR" \
AGENTDB_ADDR="127.0.0.1:$PORT_HTTP" \
AGENTDB_T_COLD_SECS="${AGENTDB_T_COLD_SECS:-60}" \
AGENTDB_TICK_SECS="${AGENTDB_TICK_SECS:-2}" \
./target/release/agentdb serve > "$OUT_DIR/${LABEL}.server.log" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$PORT_HTTP/api/admin/boxes"; then
        break
    fi
    sleep 0.3
done

pushd workload > /dev/null
./agentdb-workload -config config.yaml > "../$OUT_DIR/${LABEL}.workload.log" 2>&1 &
WORKLOAD_PID=$!
popd > /dev/null

# CSV header
echo "ts,box,tenants,appends,forks,inserts,queries_find,queries_aggregate,warm,cold,tier_transitions,quota_hits,faults,fault_active" > "$CSV"

START=$(date +%s)
END=$((START + DURATION))
while [[ $(date +%s) -lt $END ]]; do
    now=$(date +%s)
    resp=$(curl -fsS "http://127.0.0.1:$PORT_HTTP/api/admin/boxes" 2>/dev/null || true)
    if [[ -z "$resp" ]]; then
        sleep 1
        continue
    fi
    printf '%s' "$resp" | python3 -c "
import json, sys
data = json.load(sys.stdin)
ts = sys.argv[1]
for b in data:
    m = b['metrics']; g = m['gauges']
    print(f\"{ts},{b['id']},{b['num_tenants']},{m['appends']},{m['forks']},\" \
          f\"{m['inserts']},{m['queries_find']},{m['queries_aggregate']},\" \
          f\"{g['warm_sessions']},{g['cold_sessions']},{m['tier_transitions']},\" \
          f\"{m['quota_hits']},{m['faults_injected']},{str(b['fault_active']).lower()}\")
" "$now" >> "$CSV"
    sleep 1
done

echo "done — csv=$CSV server log=$OUT_DIR/${LABEL}.server.log"
