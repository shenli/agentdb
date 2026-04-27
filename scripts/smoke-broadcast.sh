#!/usr/bin/env bash
# End-to-end smoke for Phase 2 ephemeral broadcast.
#
# Boots agentdb-server in --dev mode, subscribes to a channel via curl,
# publishes 3 messages, asserts at least 3 SSE data frames arrive.
#
# Run with:
#   bash scripts/smoke-broadcast.sh
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

echo "==> Subscribing to t-1/channels/c-smoke in background"
TMP=$(mktemp)
curl -sN --max-time 5 \
    "${BASE}/api/tenants/t-1/channels/c-smoke/subscribe" > "$TMP" &
CURL_PID=$!

# Give the subscriber 500ms to attach before we start publishing.
sleep 0.5

echo "==> Publishing 3 messages to c-smoke"
for i in 1 2 3; do
    curl -fsS -X POST "${BASE}/api/tenants/t-1/channels/c-smoke/publish" \
        -H 'content-type: application/json' \
        -d "{\"i\":${i}}" > /dev/null
done

# Wait for the curl subscriber to time out (--max-time 5).
wait $CURL_PID || true

DATA_LINES=$(grep -c '^data:' "$TMP" || true)
if [ "${DATA_LINES}" -lt 3 ]; then
    echo "FAIL: expected >= 3 data: lines, got ${DATA_LINES}"
    cat "$TMP"
    exit 1
fi
rm -f "$TMP"

echo "==> smoke-broadcast OK"
