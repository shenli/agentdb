#!/usr/bin/env bash
# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S13 benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors
# Falsifies: C7, C19
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
OUT="${AGENTDB_AUTHOR_BENCH_OUT:-${HOME}/work/agentdb-author-bench}"
mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts" "${OUT}"

if [[ "${AGENTDB_RUN_AUTHOR_BENCH:-0}" != "1" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_RUN_AUTHOR_BENCH=1 to run benchmark smoke/repeat cells"
  exit 0
fi

RUN_ID="author-s13-$(date +%Y%m%d-%H%M%S)"
OUT="${OUT}" RUN_ID="${RUN_ID}" BUILD_PROFILE="${BUILD_PROFILE:-debug}" PROFILE="${PROFILE:-smoke}" \
  WORKLOAD="${WORKLOAD:-mixed-agent-loop}" CONCURRENCY="${CONCURRENCY:-1,4}" \
  WARMUP="${WARMUP:-1s}" DURATION="${DURATION:-5s}" \
  "${ROOT}/tools/agentdb-bench/scripts/agentdb-bench-local.sh" \
  2>&1 | tee "${SESSION_DIR}/logs/S13-agentdb-bench-local.log"

REPORT_DIR="${OUT}/runs/${RUN_ID}"
REPORT="$(find "${REPORT_DIR}" -maxdepth 1 -type f -name 'report*.json' | sort | head -1)"
if [[ -z "${REPORT}" || ! -f "${REPORT}" ]]; then
  echo "FAIL: benchmark report not found under ${REPORT_DIR}"
  exit 1
fi

echo "PASS: benchmark smoke produced ${REPORT}; release-grade repeat/noisy-neighbor thresholds still require explicit profile"
