#!/usr/bin/env bash
# Perf suite: start agentdb-server with a fresh data dir, run a series of
# perf-probe invocations against it, and capture JSON reports.
set -euo pipefail

PORT_HTTP=${PORT_HTTP:-7878}
WORKLOAD_DIR="tools/workload"
DATA_DIR=$(mktemp -d -t agentdb-perf-XXXXXX)
OUT_DIR=${OUT_DIR:-perf-out}
mkdir -p "$OUT_DIR"

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# AGENTDB_ROCKSDB_SYNC_WRITES is honored from the environment (default: durable
# fsync on). Set it to 0 before invoking this script to measure the opt-out.
./target/release/agentdb serve --dev \
    --bind "127.0.0.1:$PORT_HTTP" \
    --data-dir "$DATA_DIR" \
    > "$OUT_DIR/suite.server.log" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 60); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$PORT_HTTP/health/ready" 2>/dev/null; then
        break
    fi
    sleep 0.3
done
echo "server ready, pid=$SERVER_PID (sync_writes=${AGENTDB_ROCKSDB_SYNC_WRITES:-on})"

DURATION=${DURATION:-20}
CONCURRENCY=${CONCURRENCY:-"1 4 16 64"}
for C in $CONCURRENCY; do
    echo "--- c=$C duration=${DURATION}s ---"
    "$WORKLOAD_DIR/perf-probe" \
        -base "http://127.0.0.1:$PORT_HTTP" \
        -c "$C" \
        -tenants 8 \
        -duration "$DURATION" \
        -fetch 5 \
        -payload 256 \
        | tee "$OUT_DIR/probe-c${C}.json"
done

echo "done — outputs in $OUT_DIR/"
