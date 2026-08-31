#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROFILE="${AGENTDB_DISTRIBUTED_PROFILE:-nightly}"
BRANCH="${AGENTDB_DISTRIBUTED_BRANCH:-main}"
RUN_ROOT="${AGENTDB_DISTRIBUTED_RUN_ROOT:-$HOME/agentdb-distributed-test-runs}"
SKIP_PULL="${AGENTDB_DISTRIBUTED_SKIP_PULL:-0}"

usage() {
  cat <<'EOF'
Usage: ops/local-ci/scripts/distributed-test-runner.sh [OPTIONS]

Runs the distributed test plan gates intended for li-dev:
  quick    - authored Rust auto suites
  nightly  - quick + S14-S18 crash/chaos + agent-experience evals + S8 + S9 + S13 smoke
  release  - quick + S14-S18 crash/chaos + agent-experience evals + S8 + S9 + S13 release gate

Options:
  --profile NAME     quick, nightly, or release. Defaults to nightly.
  --branch NAME      Branch to fast-forward before running. Defaults to main.
  --no-pull          Do not fetch/checkout/pull before running.
  --help             Show this help.

Environment:
  AGENTDB_DISTRIBUTED_RUN_ROOT   Defaults to $HOME/agentdb-distributed-test-runs.
  AGENTDB_DISTRIBUTED_BRANCH     Defaults to main.
  AGENTDB_DISTRIBUTED_SKIP_PULL  Set to 1 to skip git synchronization.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      shift
      PROFILE="${1:-}"
      ;;
    --branch)
      shift
      BRANCH="${1:-}"
      ;;
    --no-pull)
      SKIP_PULL=1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

case "$PROFILE" in
  quick|nightly|release) ;;
  *)
    echo "profile must be quick, nightly, or release; got $PROFILE" >&2
    exit 2
    ;;
esac

cd "$ROOT"

export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-short}"
export RUST_LOG="${RUST_LOG:-warn}"
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"
export CARGO_PROFILE_TEST_DEBUG="${CARGO_PROFILE_TEST_DEBUG:-0}"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"

if [[ -z "${LIBCLANG_PATH:-}" ]]; then
  for candidate in \
    /usr/lib/llvm-14/lib \
    "$HOME/.local/libclang/usr/lib/llvm-14/lib" \
    /usr/lib/llvm-15/lib \
    /usr/lib/llvm-16/lib
  do
    if [[ -e "$candidate/libclang.so" || -n "$(find "$candidate" -maxdepth 1 -name 'libclang-*.so*' 2>/dev/null | head -n 1)" ]]; then
      export LIBCLANG_PATH="$candidate"
      break
    fi
  done
fi

if [[ "$SKIP_PULL" != "1" ]]; then
  if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "tracked working tree is dirty; refusing to switch/pull" >&2
    git status --short >&2
    exit 1
  fi
  git fetch --prune origin "$BRANCH"
  if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git checkout "$BRANCH"
  else
    git checkout -B "$BRANCH" "origin/$BRANCH"
  fi
  git pull --ff-only origin "$BRANCH"
fi

RUN_ID="${AGENTDB_DISTRIBUTED_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$PROFILE}"
LOG_DIR="$RUN_ROOT/runs/$RUN_ID"
mkdir -p "$LOG_DIR/logs" "$LOG_DIR/artifacts" "$LOG_DIR/work"
ln -sfn "$LOG_DIR" "$RUN_ROOT/current"

START_EPOCH="$(date +%s)"
CURRENT_STAGE="bootstrap"
STAGES=()

write_status() {
  local status="$1"
  local exit_code="${2:-}"
  local end_epoch="${3:-}"
  local duration_secs=""
  if [[ -n "$end_epoch" ]]; then
    duration_secs="$((end_epoch - START_EPOCH))"
  fi
  {
    printf 'status=%q\n' "$status"
    printf 'exit_code=%q\n' "$exit_code"
    printf 'profile=%q\n' "$PROFILE"
    printf 'branch=%q\n' "$(git branch --show-current)"
    printf 'git_sha=%q\n' "$(git rev-parse --short HEAD)"
    printf 'run_id=%q\n' "$RUN_ID"
    printf 'run_root=%q\n' "$RUN_ROOT"
    printf 'log_dir=%q\n' "$LOG_DIR"
    printf 'start_epoch=%q\n' "$START_EPOCH"
    printf 'end_epoch=%q\n' "$end_epoch"
    printf 'duration_secs=%q\n' "$duration_secs"
    printf 'last_stage=%q\n' "$CURRENT_STAGE"
  } >"$LOG_DIR/status.env"
}

write_report() {
  local status="$1"
  {
    echo "# AgentDB Distributed Tests"
    echo
    echo "- Status: \`$status\`"
    echo "- Profile: \`$PROFILE\`"
    echo "- Branch: \`$(git branch --show-current)\`"
    echo "- Commit: \`$(git rev-parse --short HEAD)\`"
    echo "- Run ID: \`$RUN_ID\`"
    echo "- Started epoch: \`$START_EPOCH\`"
    echo "- Duration seconds: \`$(($(date +%s) - START_EPOCH))\`"
    echo "- Run directory: \`$LOG_DIR\`"
    echo
    echo "## Stages"
    echo
    for stage in "${STAGES[@]}"; do
      echo "- \`$stage\`: \`logs/$stage.log\`"
    done
  } >"$LOG_DIR/report.md"
  ln -sfn "$LOG_DIR" "$RUN_ROOT/latest"
}

finish() {
  local exit_code=$?
  local end_epoch
  end_epoch="$(date +%s)"
  if [[ "$exit_code" == "0" ]]; then
    write_status passed 0 "$end_epoch"
    write_report passed
  else
    write_status failed "$exit_code" "$end_epoch"
    write_report failed
  fi
  rm -f "$RUN_ROOT/current"
  exit "$exit_code"
}
trap 'exit 130' INT
trap 'exit 143' TERM
trap finish EXIT

run_stage() {
  local name="$1"
  shift
  CURRENT_STAGE="$name"
  STAGES+=("$name")
  write_status running "" ""
  echo "==> $name"
  set +e
  "$@" 2>&1 | tee "$LOG_DIR/logs/$name.log"
  local statuses=("${PIPESTATUS[@]}")
  set -e
  if [[ "${statuses[0]}" != "0" ]]; then
    return "${statuses[0]}"
  fi
  if [[ "${statuses[1]}" != "0" ]]; then
    return "${statuses[1]}"
  fi
}

run_quick() {
  run_stage core-auto cargo test -q -p agentdb-core --test auto
  run_stage api-auto cargo test -q -p agentdb-api --test auto
  # Lazy-hydration parity: the same scenario suites must pass with the
  # metadata caches starting empty (KV-fallback reads carry everything).
  run_stage core-auto-lazy env AGENTDB_STARTUP_HYDRATION=lazy cargo test -q -p agentdb-core --test auto --test kv_authoritative_reads
}

run_s8() {
  local session="$LOG_DIR/artifacts/s8"
  mkdir -p "$session"
  run_stage s8-cluster-failover env \
    AGENTDB_RUN_AUTHOR_CLUSTER=1 \
    AGENTDB_DIST_SESSION_DIR="$session" \
    AGENTDB_CLUSTER_SMOKE_WORK_DIR="$LOG_DIR/work/s8" \
    bash ops/smoke/tests/auto/S8_raft_quorum_preserves_acknowledged_tablet_writes_under_partition.sh
}

run_s9() {
  local session="$LOG_DIR/artifacts/s9"
  mkdir -p "$session"
  run_stage s9-cluster-hardening env \
    AGENTDB_RUN_AUTHOR_HARDENING=1 \
    AGENTDB_DIST_SESSION_DIR="$session" \
    AGENTDB_CLUSTER_SMOKE_WORK_DIR="$LOG_DIR/work/s9" \
    bash ops/smoke/tests/auto/S9_tablet_replacement_split_merge_preserves_state_idempotency_and_cursors.sh
}

run_s13() {
  local grade="smoke"
  if [[ "$PROFILE" == "release" ]]; then
    grade="release"
  fi
  local session="$LOG_DIR/artifacts/s13"
  local out="$LOG_DIR/work/s13-bench"
  mkdir -p "$session" "$out"
  run_stage s13-benchmark env \
    AGENTDB_RUN_AUTHOR_BENCH=1 \
    AGENTDB_AUTHOR_BENCH_GRADE="$grade" \
    AGENTDB_DIST_SESSION_DIR="$session" \
    AGENTDB_AUTHOR_BENCH_OUT="$out" \
    bash tools/agentdb-bench/tests/auto/S13_benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors.sh
}

run_s14() {
  run_stage s14-sigkill-durability bash ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh
}

run_s15() {
  run_stage s15-lifecycle-chaos bash ops/local-ci/tests/auto/S15_lifecycle_transitions_survive_sigkill_chaos.sh
}

run_s16() {
  run_stage s16-restore-drill bash ops/local-ci/tests/auto/S16_restore_from_bucket_drill_bounds_rpo.sh
}

run_s17() {
  run_stage s17-lazy-eviction-chaos bash ops/local-ci/tests/auto/S17_lazy_hydration_and_cache_eviction_survive_sigkill.sh
}

run_s18() {
  run_stage s18-index-snapshot-chaos bash ops/local-ci/tests/auto/S18_record_index_snapshots_survive_sigkill.sh
}

run_agent_evals() {
  run_stage agent-experience-evals bash ops/evals/agent-experience-eval.sh
}

run_quick

if [[ "$PROFILE" == "nightly" || "$PROFILE" == "release" ]]; then
  run_s14
  run_s15
  run_s16
  run_s17
  run_s18
  run_agent_evals
  run_s8
  run_s9
  run_s13
fi
