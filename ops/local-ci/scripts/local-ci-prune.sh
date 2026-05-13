#!/usr/bin/env bash
set -euo pipefail

RUN_ROOT="${LOCAL_CI_RUN_ROOT:-/tmp/agentdb-local-ci}"
KEEP_RUNS="${LOCAL_CI_KEEP_RUNS:-30}"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: ops/local-ci/scripts/local-ci-prune.sh [OPTIONS]

Deletes old local CI run directories, keeping the newest N runs.

Options:
  --root DIR   Local CI run root. Defaults to LOCAL_CI_RUN_ROOT or /tmp/agentdb-local-ci.
  --keep N     Number of newest runs to keep. Defaults to LOCAL_CI_KEEP_RUNS or 30.
  --dry-run    Print what would be deleted without deleting.
  --help       Show this help.
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
    --keep)
      shift
      KEEP_RUNS="${1:-}"
      if [[ -z "$KEEP_RUNS" ]]; then
        echo "--keep requires a value" >&2
        exit 2
      fi
      ;;
    --dry-run)
      DRY_RUN=1
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

if ! [[ "$KEEP_RUNS" =~ ^[0-9]+$ ]]; then
  echo "--keep must be a non-negative integer" >&2
  exit 2
fi
if ((KEEP_RUNS < 1)); then
  echo "--keep must be at least 1" >&2
  exit 2
fi

RUNS_DIR="$RUN_ROOT/runs"
if [[ ! -d "$RUNS_DIR" ]]; then
  exit 0
fi

runs=()
while IFS= read -r run; do
  runs+=("$run")
done < <(find "$RUNS_DIR" -mindepth 1 -maxdepth 1 -type d -print | sort)
delete_count=$((${#runs[@]} - KEEP_RUNS))
if ((delete_count <= 0)); then
  exit 0
fi

for ((idx = 0; idx < delete_count; idx++)); do
  run="${runs[$idx]}"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "would delete $run"
  else
    rm -rf -- "$run"
    echo "deleted $run"
  fi
done
