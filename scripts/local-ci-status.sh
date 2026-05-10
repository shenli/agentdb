#!/usr/bin/env bash
set -euo pipefail

RUN_ROOT="${LOCAL_CI_RUN_ROOT:-/tmp/agentdb-local-ci}"
RUN_REF="latest"
SHOW_LOGS=0
SHOW_SUMMARY=0

usage() {
  cat <<'EOF'
Usage: scripts/local-ci-status.sh [OPTIONS]

Shows the most recent local CI run status.

Options:
  --root DIR       Local CI run root. Defaults to LOCAL_CI_RUN_ROOT or /tmp/agentdb-local-ci.
  --run RUN_ID     Show a specific run under ROOT/runs.
  --current        Show the currently running run.
  --logs           Also list per-stage log files.
  --summary        Show latest, last successful, and last failed run.
  --help           Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      shift
      RUN_ROOT="${1:-}"
      if [[ -z "$RUN_ROOT" ]]; then
        echo "--root requires a value" >&2
        exit 2
      fi
      ;;
    --run)
      shift
      RUN_REF="runs/${1:-}"
      if [[ "$RUN_REF" == "runs/" ]]; then
        echo "--run requires a value" >&2
        exit 2
      fi
      ;;
    --current)
      RUN_REF="current"
      ;;
    --logs)
      SHOW_LOGS=1
      ;;
    --summary)
      SHOW_SUMMARY=1
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

if [[ "$SHOW_SUMMARY" == "1" ]]; then
  exec "$(dirname "${BASH_SOURCE[0]}")/local-ci-summary.sh" --root "$RUN_ROOT"
fi

RUN_DIR="$RUN_ROOT/$RUN_REF"
if [[ ! -e "$RUN_DIR/status.env" ]]; then
  echo "no local CI status found at $RUN_DIR/status.env" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$RUN_DIR/status.env"

echo "status: ${status:-unknown}"
echo "exit_code: ${exit_code:-}"
echo "run_id: ${run_id:-}"
echo "branch: ${git_branch:-}"
echo "git_sha: ${git_sha:-}"
echo "last_stage: ${last_stage:-}"
echo "duration_secs: ${duration_secs:-}"
echo "log_dir: ${log_dir:-$RUN_DIR}"
echo "report: ${log_dir:-$RUN_DIR}/report.html"

if [[ "$SHOW_LOGS" == "1" ]]; then
  find "${log_dir:-$RUN_DIR}" -maxdepth 1 -type f -name '*.log' -print | sort
fi
