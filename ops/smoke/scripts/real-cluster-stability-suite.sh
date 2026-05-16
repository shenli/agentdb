#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BASE_WORK_DIR="${AGENTDB_STABILITY_SUITE_WORK_DIR:-$HOME/work/agentdb-stability-suite}"
CONTINUE_ON_FAILURE="${AGENTDB_STABILITY_CONTINUE_ON_FAILURE:-0}"

run_case() {
  local name="$1"
  shift
  local work_dir="$BASE_WORK_DIR/$name"
  echo "== stability case: $name =="
  echo "work_dir=$work_dir"
  if AGENTDB_CLUSTER_SMOKE_WORK_DIR="$work_dir" "$ROOT/ops/smoke/scripts/real-cluster-smoke-compose.sh" "$@"; then
    echo "== stability case passed: $name =="
    return 0
  fi
  local status=$?
  echo "== stability case failed: $name status=$status diagnostics=$work_dir/diagnostics ==" >&2
  if [[ "$CONTINUE_ON_FAILURE" == "1" ]]; then
    return 0
  fi
  return "$status"
}

run_case deterministic-surface \
  --nodes 6 \
  --two-cell \
  --consistency \
  --record-index \
  --changefeed

run_case deterministic-hardening \
  --nodes 6 \
  --two-cell \
  --full-hardening

run_case seeded-chaos \
  --nodes 3 \
  --chaos \
  --chaos-seed "${AGENTDB_STABILITY_CHAOS_SEED:-424242}" \
  --chaos-iterations "${AGENTDB_STABILITY_CHAOS_ITERATIONS:-12}"
