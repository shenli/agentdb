#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Publication-candidate defaults for the local single-node value proposition.
# Override any variable at the call site for smoke runs or heavier cells.
export MATRIX_ID="${MATRIX_ID:-longrun-value-$(date +%Y%m%d-%H%M%S)}"
export BACKENDS="${BACKENDS:-agentdb,mongodb,postgres}"
export WORKLOADS="${WORKLOADS:-mixed-agent-loop,append-only,append-fetch-hot}"
export PROFILE="${PROFILE:-default-agent-runtime}"
export TENANTS="${TENANTS:-10}"
export AGENTS_PER_TENANT="${AGENTS_PER_TENANT:-2}"
export SESSIONS_PER_AGENT="${SESSIONS_PER_AGENT:-50}"
export EVENTS_PER_SESSION="${EVENTS_PER_SESSION:-50}"
export RECORDS_PER_SESSION="${RECORDS_PER_SESSION:-10}"
export PAYLOAD_BYTES="${PAYLOAD_BYTES:-2048}"
export ARTIFACT_RATE="${ARTIFACT_RATE:-0.05}"
export DURABILITY_PROFILE="${DURABILITY_PROFILE:-single-node-wal}"
export CONCURRENCY="${CONCURRENCY:-32}"
export WARMUP="${WARMUP:-30s}"
export DURATION="${DURATION:-10m}"
export REPEATS="${REPEATS:-3}"
export SEEDS="${SEEDS:-43,44,45}"
export SESSION_SELECTION="${SESSION_SELECTION:-zipf}"
export ZIPF_EXPONENT="${ZIPF_EXPONENT:-1.2}"
export FETCH_LAST_N="${FETCH_LAST_N:-20}"
export NO_RAW_JSONL="${NO_RAW_JSONL:-1}"
export MIN_FREE_GB="${MIN_FREE_GB:-20}"

echo "long-run value matrix:"
echo "  matrix_id=${MATRIX_ID}"
echo "  backends=${BACKENDS}"
echo "  workloads=${WORKLOADS}"
echo "  repeats=${REPEATS} seeds=${SEEDS}"
echo "  warmup=${WARMUP} duration=${DURATION} concurrency=${CONCURRENCY}"
echo "  shape=${TENANTS} tenants x ${AGENTS_PER_TENANT} agents x ${SESSIONS_PER_AGENT} sessions"
echo "  output=${OUT:-${ROOT}/bench-out}"

"${ROOT}/scripts/agentdb-bench-value-matrix.sh"
