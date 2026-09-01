#!/usr/bin/env bash
# AUTO-GENERATED from test plan: docs/design/distributed-safety-hardening.md §2
# Scenario: S22 transport_fault_injection_isolates_live_leader
#
# Deterministic transport fault injection (hardening P0-1). Isolates a
# LIVE raft leader from its followers via the fault-control API — a
# partition shape the process-kill scenarios cannot produce — and asserts:
#   - the fault mechanism actually fired (matched counters advance),
#   - the isolated leader loses quorum and cannot commit a write,
#   - the surviving majority elects a new leader at a higher term,
#   - healing the partition reconverges every replica.
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
WORK_DIR="${AGENTDB_CLUSTER_SMOKE_WORK_DIR:-${HOME}/work/agentdb-author-s22}"

mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts"

if [[ "${AGENTDB_RUN_AUTHOR_CLUSTER:-0}" != "1" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_RUN_AUTHOR_CLUSTER=1 to run the transport fault-injection smoke"
  exit 0
fi

export AGENTDB_CLUSTER_SMOKE_WORK_DIR="${WORK_DIR}"
"${ROOT}/ops/smoke/scripts/real-cluster-smoke-compose.sh" \
  --nodes 3 \
  --fault-injection \
  2>&1 | tee "${SESSION_DIR}/logs/S22-transport-fault-injection.log"

LOG="${SESSION_DIR}/logs/S22-transport-fault-injection.log"
if ! grep -q "real cluster smoke passed" "${LOG}"; then
  echo "FAIL: cluster smoke did not report pass"
  exit 1
fi
if ! grep -q "fault-injection: PASS" "${LOG}"; then
  echo "FAIL: fault-injection scenario did not reach its PASS marker"
  exit 1
fi
if ! grep -q "isolation rules matched" "${LOG}"; then
  echo "FAIL: fault-injection mechanism never fired (no matched messages)"
  exit 1
fi
echo "PASS: transport fault injection isolated a live leader; quorum, election, and heal all held"
