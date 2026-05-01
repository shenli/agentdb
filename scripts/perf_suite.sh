#!/usr/bin/env bash
# Perf suite: start agentdb-server with fresh data dirs, run a series of
# perf-probe invocations against it, and capture JSON reports.
set -euo pipefail

PORT_HTTP=${PORT_HTTP:-7878}
DATA_DIR=$(mktemp -d -t agentdb-perf-XXXXXX)
OBJ_DIR=$(mktemp -d -t agentdb-perf-obj-XXXXXX)
OUT_DIR="perf-out"
mkdir -p "$OUT_DIR"

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR" "$OBJ_DIR"
}
trap cleanup EXIT

AGENTDB_DATA="$DATA_DIR" \
AGENTDB_OBJECTS="$OBJ_DIR" \
AGENTDB_ADDR="127.0.0.1:$PORT_HTTP" \
AGENTDB_T_COLD_SECS="${AGENTDB_T_COLD_SECS:-60}" \
AGENTDB_TICK_SECS="${AGENTDB_TICK_SECS:-2}" \
./target/release/agentdb serve > "$OUT_DIR/suite.server.log" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$PORT_HTTP/api/admin/boxes" 2>/dev/null; then
        break
    fi
    sleep 0.3
done
echo "server ready, pid=$SERVER_PID"

DURATION=${DURATION:-20}
for C in 1 4 16 64; do
    echo "--- c=$C duration=${DURATION}s ---"
    workload/perf-probe \
        -base "http://127.0.0.1:$PORT_HTTP" \
        -c "$C" \
        -tenants 8 \
        -duration "$DURATION" \
        -fetch 5 \
        -payload 256 \
        | tee "$OUT_DIR/probe-c${C}.json"
done

# Admin snapshot after all runs
curl -fsS "http://127.0.0.1:$PORT_HTTP/api/admin/boxes" \
    | python3 -m json.tool > "$OUT_DIR/final-boxes.json"

echo "done — outputs in $OUT_DIR/"
