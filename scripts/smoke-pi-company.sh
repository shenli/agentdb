#!/usr/bin/env bash
# End-to-end smoke for the pi-company sample app.
# Skips if AGENTDB_DEMO_API_KEY is unset.

set -euo pipefail

if [ -z "${AGENTDB_DEMO_API_KEY:-}" ]; then
    echo "SKIP: AGENTDB_DEMO_API_KEY not set"
    exit 0
fi

cd "$(dirname "$0")/../examples/agents/typescript/pi-company"

# Pre-build the AgentDB server for fast first-run.
echo "==> Pre-building agentdb-server (cargo build)..."
(cd "$(git rev-parse --show-toplevel)" && cargo build --quiet -p agentdb-server) || {
    echo "FAIL: cargo build failed"
    exit 1
}

# Install + build the sample.
echo "==> Installing pi-company deps..."
npm install --silent 2>&1 | tail -3

echo "==> Type-checking..."
npm run build --silent 2>&1 | tail -3

# Run the launch scenario, capture output.
TMP=$(mktemp)
trap "rm -f $TMP" EXIT

echo "==> Running launch scenario (this takes ~3-5 minutes)..."
node --experimental-strip-types src/index.ts --scenario=launch > "$TMP" 2>&1 || {
    echo "FAIL: scenario exited non-zero"
    cat "$TMP" | tail -50
    exit 1
}

# Assertions.
grep -q "AgentDB ready" "$TMP" || { echo "FAIL: server didn't start"; tail -50 "$TMP"; exit 1; }
grep -q "Running scripted scenario 'launch'" "$TMP" || { echo "FAIL: scenario didn't start"; tail -50 "$TMP"; exit 1; }
grep -qi "contractor" "$TMP" || { echo "FAIL: no contractor was hired"; tail -50 "$TMP"; exit 1; }

echo "==> smoke-pi-company OK"
