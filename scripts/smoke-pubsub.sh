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
MONGO_PORT=$((PORT + 1))
BASE="http://127.0.0.1:${PORT}"

echo "==> Booting agentdb-server on ${BASE} (mongo on :${MONGO_PORT})"
AGENTDB_ADDR="127.0.0.1:${PORT}" \
AGENTDB_MONGO_ADDR="127.0.0.1:${MONGO_PORT}" \
cargo run --quiet -p agentdb-server -- serve --dev &
SERVER_PID=$!
trap "kill $SERVER_PID 2>/dev/null || true; wait $SERVER_PID 2>/dev/null || true" EXIT

# Wait for /health/ready (up to ~30s).
echo "==> Waiting for server to be ready"
for _ in $(seq 1 60); do
    if curl -fsS "${BASE}/health/ready" > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
if ! curl -fsS "${BASE}/health/ready" > /dev/null 2>&1; then
    echo "FAIL: server did not become ready within 30s"
    exit 1
fi

echo "==> Appending 3 events to t-1/sessions/s-1"
for i in 1 2 3; do
    curl -fsS -X POST "${BASE}/api/tenants/t-1/sessions/s-1/append_and_fetch" \
        -H 'content-type: application/json' \
        -d "{\"event\":{\"kind\":\"smoke\",\"n\":${i}},\"fetch_last_n\":0}" > /dev/null
done

echo "==> Subscribing with from_seq=1; expecting subscribed + 3 catch-up frames"
# 5-second budget; count `data:` lines.
DATA_LINES=$(curl -sN --max-time 5 \
    "${BASE}/api/tenants/t-1/sessions/s-1/subscribe?from_seq=1" \
    | grep -c '^data:' || true)

# Expect at least 4: 1 `subscribed` control frame + 3 catch-up event frames.
if [ "${DATA_LINES}" -lt 4 ]; then
    echo "FAIL: expected >= 4 data: lines, got ${DATA_LINES}"
    exit 1
fi

echo "==> Subscribing with from_seq above live_seq; expecting just subscribed"
DATA_LINES=$(curl -sN --max-time 3 \
    "${BASE}/api/tenants/t-1/sessions/s-1/subscribe?from_seq=999" \
    | grep -c '^data:' || true)
# Expect at least 1: the subscribed control event.
if [ "${DATA_LINES}" -lt 1 ]; then
    echo "FAIL: expected at least 1 (subscribed) data line, got ${DATA_LINES}"
    exit 1
fi

echo "==> smoke-pubsub OK"
