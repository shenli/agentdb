#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

RUN_ROOT="${LOCAL_CI_RUN_ROOT:-$HOME/agentdb-ci-runs}"
CHAOS_ITERATIONS="${LOCAL_CI_NIGHTLY_CHAOS_ITERATIONS:-48}"
SKIP_PULL="${LOCAL_CI_SKIP_PULL:-0}"

usage() {
  cat <<'EOF'
Usage: ops/local-ci/scripts/local-ci-nightly.sh [OPTIONS]

Runs the nightly local CI profile: cargo, SDKs, failover smoke, deterministic
cluster hardening smoke, and bounded random cluster chaos.

Options:
  --no-pull              Do not git pull before running.
  --chaos-iterations N   Chaos kill/restart iterations. Defaults to LOCAL_CI_NIGHTLY_CHAOS_ITERATIONS or 48.
  --help                 Show this help.

Environment:
  LOCAL_CI_RUN_ROOT      Run root. Defaults to $HOME/agentdb-ci-runs.
  LOCAL_CI_SKIP_PULL     Set to 1 to skip git pull.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-pull)
      SKIP_PULL=1
      ;;
    --chaos-iterations)
      shift
      CHAOS_ITERATIONS="${1:-}"
      if [[ -z "$CHAOS_ITERATIONS" ]]; then
        echo "--chaos-iterations requires a value" >&2
        exit 2
      fi
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

cd "$ROOT"

if [[ "$SKIP_PULL" != "1" ]]; then
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "working tree is dirty; refusing nightly pull. Commit/stash changes or rerun with --no-pull." >&2
    exit 1
  fi
  git pull --ff-only
fi

export LOCAL_CI_RUN_ROOT="$RUN_ROOT"
exec ops/local-ci/scripts/local-ci.sh --hardening --chaos --chaos-iterations "$CHAOS_ITERATIONS"
