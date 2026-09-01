#!/usr/bin/env bash
# AUTO-GENERATED from test plan: docs/design/distributed-safety-hardening.md §3
# Scenario: S23 store_incarnation_admission_refuses_retired_disk
#
# Retires a node's store incarnation via the control plane, restarts that
# node against the same data directory, and asserts it FAILS to boot — a
# retired disk must not rejoin. Exercises the control-plane admission half
# of the durable store-identity check.
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
WORK_DIR="${AGENTDB_CLUSTER_SMOKE_WORK_DIR:-${HOME}/work/agentdb-author-s23}"

mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts"

if [[ "${AGENTDB_RUN_AUTHOR_CLUSTER:-0}" != "1" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_RUN_AUTHOR_CLUSTER=1 to run the store-admission smoke"
  exit 0
fi

export AGENTDB_CLUSTER_SMOKE_WORK_DIR="${WORK_DIR}"
"${ROOT}/ops/smoke/scripts/real-cluster-smoke-compose.sh" \
  --nodes 3 \
  --store-admission \
  2>&1 | tee "${SESSION_DIR}/logs/S23-store-admission.log"

LOG="${SESSION_DIR}/logs/S23-store-admission.log"
if ! grep -q "real cluster smoke passed" "${LOG}"; then
  echo "FAIL: cluster smoke did not report pass"
  exit 1
fi
if ! grep -q "store-admission: PASS" "${LOG}"; then
  echo "FAIL: store-admission scenario did not reach its PASS marker"
  exit 1
fi
echo "PASS: a retired store incarnation was refused readmission by the control plane"
