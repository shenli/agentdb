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
# Statement-budget tripwire for the "hot loop one await from SQL" defect
# class (metadata self-hosting stage 1): log every sqlx statement on all
# nodes and assert the steady-state volume stays at heartbeat scale.
export RUST_LOG="${RUST_LOG:-info},sqlx::query=debug"
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

# SQL statement budget: with observed state in node_operational summaries
# and tokens/locators as system-tenant records, control-plane SQL should be
# refresher-scale — this smoke configures a 500ms route refresh, which puts
# a healthy 6-node fleet around 150k statements for the whole run. A
# per-message/per-request/per-tick SQL leak or a notification storm shows
# up as many multiples of that (the first calibration run measured 1.26M
# from exactly those defects); the budget sits ~2.5x above healthy baseline
# so only the defect CLASS trips it, not normal variance.
total_statements=0
for node_log in "${WORK_DIR}"/logs/node-*.log; do
  [[ -f "${node_log}" ]] || continue
  count=$(grep -c 'sqlx::query' "${node_log}" || true)
  echo "sql-statements $(basename "${node_log}"): ${count}"
  total_statements=$((total_statements + count))
done
budget=400000
if [[ ${total_statements} -gt ${budget} ]]; then
  echo "FAIL: ${total_statements} SQL statements across nodes exceeds budget ${budget};"
  echo "      something re-attached a hot loop to the metadata database"
  exit 1
fi
echo "sql-statement budget ok: ${total_statements} <= ${budget}"

echo "PASS: real cluster smoke reported pass; inspect ${WORK_DIR}/diagnostics for readiness and Raft evidence"
