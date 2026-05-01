#!/usr/bin/env bash
set -euo pipefail

# Minimal end-to-end smoke: starts the DB, fires off a handful of operations
# via HTTP, checks the dashboard is served, and tears down.

PORT_HTTP=${PORT_HTTP:-7878}
DATA_DIR=$(mktemp -d -t agentdb-smoke-XXXXXX)
OBJ_DIR=$(mktemp -d -t agentdb-smoke-obj-XXXXXX)

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
cargo run -p agentdb-server &
SERVER_PID=$!

# Wait for the HTTP port
for i in $(seq 1 30); do
    if curl -s -o /dev/null "http://127.0.0.1:$PORT_HTTP/api/admin/boxes"; then
        break
    fi
    sleep 1
done

# Poke the endpoints
curl -s -X POST "http://127.0.0.1:$PORT_HTTP/api/tenants/t-smoke/sessions/s/append_and_fetch" \
    -H 'Content-Type: application/json' \
    -d '{"event":{"text":"hi"},"fetch_last_n":0}' > /dev/null

# Dashboard root
status=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT_HTTP/dashboard/")
if [[ "$status" != "200" ]]; then
    echo "dashboard returned $status"
    exit 1
fi

echo "smoke ok"
