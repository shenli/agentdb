#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUN_CARGO=1
RUN_SDK=1
RUN_CLUSTER=1
RUN_CLIPPY=1

usage() {
  cat <<'EOF'
Usage: scripts/local-ci.sh [OPTIONS]

Runs the local CI pipeline:
  1. cargo fmt --check
  2. cargo clippy --workspace --tests -- -D warnings
  3. cargo test --workspace --all-targets
  4. Python + TypeScript SDK smoke tests
  5. compose-backed real cluster smoke with failover

Options:
  --cargo-only       Run only fmt/clippy/cargo tests.
  --no-clippy        Skip clippy.
  --no-sdk           Skip Python/TypeScript SDK tests.
  --no-cluster       Skip the real cluster failover smoke.
  --help             Show this help.

Environment:
  LIBCLANG_PATH      Used by librocksdb-sys bindgen. Auto-detected on li-dev.
  LOCAL_CI_LOG_DIR   Directory for per-stage logs. Defaults to /tmp/agentdb-local-ci.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cargo-only)
      RUN_SDK=0
      RUN_CLUSTER=0
      ;;
    --no-clippy)
      RUN_CLIPPY=0
      ;;
    --no-sdk)
      RUN_SDK=0
      ;;
    --no-cluster)
      RUN_CLUSTER=0
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

LOG_DIR="${LOCAL_CI_LOG_DIR:-/tmp/agentdb-local-ci}"
mkdir -p "$LOG_DIR"

run_stage() {
  local name="$1"
  shift
  local log="$LOG_DIR/${name}.log"
  echo "==> $name"
  (
    cd "$ROOT"
    "$@"
  ) 2>&1 | tee "$log"
}

run_shell_stage() {
  local name="$1"
  shift
  local log="$LOG_DIR/${name}.log"
  echo "==> $name"
  (
    cd "$ROOT"
    bash -lc "$*"
  ) 2>&1 | tee "$log"
}

if [[ "$RUN_CARGO" == "1" ]]; then
  run_stage cargo-fmt cargo fmt --check
  if [[ "$RUN_CLIPPY" == "1" ]]; then
    run_stage cargo-clippy cargo clippy --workspace --tests -- -D warnings
  fi
  run_stage cargo-test cargo test --workspace --all-targets
fi

if [[ "$RUN_SDK" == "1" ]]; then
  run_shell_stage sdk-python "python3 -m pip install -e 'clients/python[async]' pytest && pytest clients/python -q"
  run_shell_stage sdk-typescript "cd clients/typescript && npm ci && npm run build && npm test"
fi

if [[ "$RUN_CLUSTER" == "1" ]]; then
  run_stage real-cluster-smoke scripts/real-cluster-smoke-compose.sh --failover
fi

echo "local ci passed; logs: $LOG_DIR"
