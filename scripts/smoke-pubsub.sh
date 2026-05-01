#!/usr/bin/env bash
# End-to-end smoke for AgentDB Pub/Sub (Phase 1).
#
# Boots `agentdb serve --dev` on a free port, appends events,
# subscribes via SSE with from_seq=1, and asserts that catch-up
# delivers all events (plus the `subscribed` control frame).
#
# Run with:
#   bash scripts/smoke-pubsub.sh
#
# Cold builds take a few minutes. Subsequent runs reuse the
# cargo cache and finish in seconds.
set -euo pipefail

PORT=$((10000 + RANDOM % 50000))
BASE="http://127.0.0.1:${PORT}"

echo "==> Booting agentdb-server on ${BASE}"
AGENTDB_ADDR="127.0.0.1:${PORT}" \
cargo run --quiet -p agentdb-server -- serve --dev &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null || true; wait $SERVER_PID 2>/dev/null || true" EXIT

# Wait for /health/ready (up to ~180s). Cold source-checkout runs may spend
# most of this budget compiling before the server process starts.
echo "==> Waiting for server to be ready"
for _ in $(seq 1 360); do
    if curl -fsS "${BASE}/health/ready" > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
if ! curl -fsS "${BASE}/health/ready" > /dev/null 2>&1; then
    echo "FAIL: server did not become ready within 180s"
    exit 1
fi

echo "==> Creating a tenant, agent, and session"
BOOTSTRAP=$(python3 - "$PORT" <<'PY'
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

tenant = post("/api/tenants", {"external_key": "pubsub-smoke", "policy": {}})
agent = post(f"/api/tenants/{tenant['tenant_id']}/agents", {"alias": "smoke-agent", "config": {}})
session = post(
    f"/api/tenants/{tenant['tenant_id']}/agents/{agent['agent_id']}/sessions",
    {"alias": "smoke-session"},
)
print(json.dumps({
    "tenant_id": tenant["tenant_id"],
    "agent_id": agent["agent_id"],
    "session_id": session["session_id"],
}))
PY
)
TENANT_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["tenant_id"])' <<< "$BOOTSTRAP")
AGENT_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["agent_id"])' <<< "$BOOTSTRAP")
SESSION_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])' <<< "$BOOTSTRAP")

echo "==> Appending 3 events to ${TENANT_ID}/sessions/${SESSION_ID}"
for i in 1 2 3; do
    curl -fsS -X POST "${BASE}/api/tenants/${TENANT_ID}/sessions/${SESSION_ID}/append-and-fetch" \
        -H 'content-type: application/json' \
        -d "{\"agent_id\":\"${AGENT_ID}\",\"kind\":\"smoke\",\"payload\":{\"n\":${i}},\"artifact_refs\":[],\"source_refs\":[],\"last\":0,\"max_bytes\":1000000}" > /dev/null
done

echo "==> Subscribing with from_seq=1; expecting subscribed + 3 catch-up frames"
# 5-second budget; count `data:` lines.
DATA_LINES=$(curl -sN --max-time 5 \
    "${BASE}/api/tenants/${TENANT_ID}/sessions/${SESSION_ID}/subscribe?from_seq=1" \
    | grep -c '^data:' || true)

# Expect at least 4: 1 `subscribed` control frame + 3 catch-up event frames.
if [ "${DATA_LINES}" -lt 4 ]; then
    echo "FAIL: expected >= 4 data: lines, got ${DATA_LINES}"
    exit 1
fi

echo "==> Subscribing with from_seq above live_seq; expecting just subscribed"
DATA_LINES=$(curl -sN --max-time 3 \
    "${BASE}/api/tenants/${TENANT_ID}/sessions/${SESSION_ID}/subscribe?from_seq=999" \
    | grep -c '^data:' || true)
# Expect at least 1: the subscribed control event.
if [ "${DATA_LINES}" -lt 1 ]; then
    echo "FAIL: expected at least 1 (subscribed) data line, got ${DATA_LINES}"
    exit 1
fi

echo "==> smoke-pubsub OK"
