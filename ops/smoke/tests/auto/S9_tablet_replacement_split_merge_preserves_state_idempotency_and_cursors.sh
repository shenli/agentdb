#!/usr/bin/env bash
# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S9 tablet_replacement_split_merge_preserves_state_idempotency_and_cursors
# Falsifies: C1, C3, C11, C15, C16, C17
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
WORK_DIR="${AGENTDB_CLUSTER_SMOKE_WORK_DIR:-${HOME}/work/agentdb-author-s9}"

mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts"

if [[ "${AGENTDB_RUN_AUTHOR_HARDENING:-0}" != "1" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_RUN_AUTHOR_HARDENING=1 to run full hardening replacement/split/merge smoke"
  exit 0
fi

export AGENTDB_CLUSTER_SMOKE_WORK_DIR="${WORK_DIR}"
"${ROOT}/ops/smoke/scripts/real-cluster-smoke-compose.sh" \
  --nodes 6 \
  --two-cell \
  --full-hardening \
  2>&1 | tee "${SESSION_DIR}/logs/S9-full-hardening.log"

if ! grep -q "real cluster smoke passed" "${SESSION_DIR}/logs/S9-full-hardening.log"; then
  echo "FAIL: full hardening smoke did not report pass"
  exit 1
fi

echo "PASS: hardening smoke reported pass; inspect ${WORK_DIR}/diagnostics and smoke logs for movement/cursor evidence"
