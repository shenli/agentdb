#!/usr/bin/env bash
# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S8 raft_quorum_preserves_acknowledged_tablet_writes_under_partition
# Falsifies: C1, C15, C17, C18
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
WORK_DIR="${AGENTDB_CLUSTER_SMOKE_WORK_DIR:-${HOME}/work/agentdb-author-s8}"

mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts"

if [[ "${AGENTDB_RUN_AUTHOR_CLUSTER:-0}" != "1" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_RUN_AUTHOR_CLUSTER=1 to run the real cluster quorum smoke"
  exit 0
fi

export AGENTDB_CLUSTER_SMOKE_WORK_DIR="${WORK_DIR}"
"${ROOT}/ops/smoke/scripts/real-cluster-smoke-compose.sh" \
  --nodes 6 \
  --two-cell \
  --failover \
  --consistency \
  --record-index \
  --changefeed \
  2>&1 | tee "${SESSION_DIR}/logs/S8-real-cluster-smoke.log"

if ! grep -q "real cluster smoke passed" "${SESSION_DIR}/logs/S8-real-cluster-smoke.log"; then
  echo "FAIL: cluster smoke did not report pass"
  exit 1
fi

echo "PASS: real cluster smoke reported pass; inspect ${WORK_DIR}/diagnostics for readiness and Raft evidence"
