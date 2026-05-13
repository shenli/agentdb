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
cargo run -p agentdb-server -- serve &
SERVER_PID=$!

# Wait for the HTTP port. Cold source-checkout runs may spend most of this
# budget compiling before the server process starts.
for i in $(seq 1 180); do
    if curl -s -o /dev/null "http://127.0.0.1:$PORT_HTTP/health/ready"; then
        break
    fi
    sleep 1
done
if ! curl -s -o /dev/null "http://127.0.0.1:$PORT_HTTP/health/ready"; then
    echo "server did not become ready within 180s"
    exit 1
fi

# Poke the endpoints
python3 - "$PORT_HTTP" <<'PY'
import json
import sys
import urllib.request

base = f"http://127.0.0.1:{sys.argv[1]}"

def post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        base + path,
        data=data,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.load(resp)

tenant = post("/api/tenants", {"external_key": "smoke", "policy": {}})
agent = post(f"/api/tenants/{tenant['tenant_id']}/agents", {"alias": "smoke-agent", "config": {}})
session = post(
    f"/api/tenants/{tenant['tenant_id']}/agents/{agent['agent_id']}/sessions",
    {"alias": "smoke-session"},
)
post(
    f"/api/tenants/{tenant['tenant_id']}/sessions/{session['session_id']}/append-and-fetch",
    {
        "agent_id": agent["agent_id"],
        "kind": "message",
        "payload": {"text": "hi"},
        "artifact_refs": [],
        "source_refs": [],
        "last": 1,
        "max_bytes": 1000000,
    },
)
PY

# Dashboard root
status=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT_HTTP/dashboard/")
if [[ "$status" != "200" ]]; then
    echo "dashboard returned $status"
    exit 1
fi

echo "smoke ok"
