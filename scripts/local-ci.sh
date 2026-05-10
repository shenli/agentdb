#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUN_CARGO=1
RUN_SDK=1
RUN_CLUSTER=1
RUN_CLIPPY=1
RUN_FMT=1
RUN_CHAOS=0
CHAOS_ITERATIONS="${LOCAL_CI_CHAOS_ITERATIONS:-24}"
CHAOS_SEED="${LOCAL_CI_CHAOS_SEED:-}"

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
  --cluster-only     Run only cluster smoke stages.
  --no-fmt           Skip cargo fmt --check.
  --no-clippy        Skip clippy.
  --no-sdk           Skip Python/TypeScript SDK tests.
  --no-cluster       Skip the real cluster failover smoke.
  --chaos            Also run the bounded random cluster chaos smoke.
  --chaos-iterations N
                     Chaos kill/restart iterations. Defaults to LOCAL_CI_CHAOS_ITERATIONS or 24.
  --chaos-seed N     Deterministic chaos seed. Defaults to LOCAL_CI_CHAOS_SEED or current time.
  --help             Show this help.

Environment:
  LIBCLANG_PATH      Used by librocksdb-sys bindgen. Auto-detected on li-dev.
  LOCAL_CI_RUN_ROOT  Root directory for CI runs. Defaults to /tmp/agentdb-local-ci.
  LOCAL_CI_RUN_ID    Run identifier. Defaults to an UTC timestamp plus pid.
  LOCAL_CI_LOG_DIR   Exact run directory for logs/status. Overrides LOCAL_CI_RUN_ROOT.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cargo-only)
      RUN_SDK=0
      RUN_CLUSTER=0
      ;;
    --cluster-only)
      RUN_CARGO=0
      RUN_SDK=0
      RUN_CLUSTER=1
      ;;
    --no-clippy)
      RUN_CLIPPY=0
      ;;
    --no-fmt)
      RUN_FMT=0
      ;;
    --no-sdk)
      RUN_SDK=0
      ;;
    --no-cluster)
      RUN_CLUSTER=0
      ;;
    --chaos)
      RUN_CHAOS=1
      ;;
    --chaos-iterations)
      shift
      CHAOS_ITERATIONS="${1:-}"
      if [[ -z "$CHAOS_ITERATIONS" ]]; then
        echo "--chaos-iterations requires a value" >&2
        exit 2
      fi
      ;;
    --chaos-seed)
      shift
      CHAOS_SEED="${1:-}"
      if [[ -z "$CHAOS_SEED" ]]; then
        echo "--chaos-seed requires a value" >&2
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

RUN_ROOT="${LOCAL_CI_RUN_ROOT:-/tmp/agentdb-local-ci}"
RUN_ID="${LOCAL_CI_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
LOG_DIR="${LOCAL_CI_LOG_DIR:-$RUN_ROOT/runs/$RUN_ID}"
mkdir -p "$LOG_DIR"
mkdir -p "$RUN_ROOT"

START_EPOCH="$(date +%s)"
CURRENT_STAGE="bootstrap"
EXIT_CODE=0
STAGE_NAMES=()

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
    printf 'run_id=%q\n' "$RUN_ID"
    printf 'run_root=%q\n' "$RUN_ROOT"
    printf 'log_dir=%q\n' "$LOG_DIR"
    printf 'start_epoch=%q\n' "$START_EPOCH"
    printf 'end_epoch=%q\n' "$end_epoch"
    printf 'duration_secs=%q\n' "$duration_secs"
    printf 'last_stage=%q\n' "$CURRENT_STAGE"
    printf 'git_sha=%q\n' "$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || true)"
    printf 'git_branch=%q\n' "$(git -C "$ROOT" branch --show-current 2>/dev/null || true)"
  } > "$LOG_DIR/status.env"
}

write_report() {
  local status="$1"
  local title="AgentDB Local CI - $status"
  local report="$LOG_DIR/report.html"
  local root_index="$RUN_ROOT/index.html"
  {
    cat <<EOF
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>$title</title>
  <style>
    :root { color-scheme: light dark; --ok: #117733; --bad: #b00020; --muted: #667085; }
    body { font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 2rem; line-height: 1.45; }
    main { max-width: 980px; }
    .status { display: inline-block; padding: .25rem .6rem; border-radius: 999px; font-weight: 700; }
    .passed { color: white; background: var(--ok); }
    .failed { color: white; background: var(--bad); }
    .running { color: black; background: #facc15; }
    dl { display: grid; grid-template-columns: 12rem 1fr; gap: .35rem 1rem; }
    dt { color: var(--muted); }
    dd { margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { border-bottom: 1px solid color-mix(in srgb, currentColor 18%, transparent); padding: .55rem; text-align: left; }
    a { color: inherit; font-weight: 650; }
    .muted { color: var(--muted); }
  </style>
</head>
<body>
<main>
  <h1>AgentDB Local CI</h1>
  <p><span class="status $status">$status</span></p>
  <dl>
    <dt>Run ID</dt><dd>$RUN_ID</dd>
    <dt>Branch</dt><dd>$(git -C "$ROOT" branch --show-current 2>/dev/null || true)</dd>
    <dt>Commit</dt><dd>$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || true)</dd>
    <dt>Last Stage</dt><dd>$CURRENT_STAGE</dd>
    <dt>Started Epoch</dt><dd>$START_EPOCH</dd>
    <dt>Duration Seconds</dt><dd>$(( $(date +%s) - START_EPOCH ))</dd>
    <dt>Run Directory</dt><dd>$LOG_DIR</dd>
  </dl>
  <h2>Stage Logs</h2>
  <table>
    <thead><tr><th>Stage</th><th>Log</th><th>Tail</th></tr></thead>
    <tbody>
EOF
    if ((${#STAGE_NAMES[@]} > 0)); then
      for stage in "${STAGE_NAMES[@]}"; do
        local log="$stage.log"
        echo "      <tr><td>$stage</td><td><a href=\"$log\">$log</a></td><td><pre>"
        if [[ -f "$LOG_DIR/$log" ]]; then
          tail -20 "$LOG_DIR/$log" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g'
        fi
        echo "</pre></td></tr>"
      done
    fi
    cat <<EOF
    </tbody>
  </table>
  <p class="muted">Generated by scripts/local-ci.sh. Refresh while a run is active.</p>
</main>
</body>
</html>
EOF
  } > "$report"

  cat > "$root_index" <<EOF
<!doctype html>
<meta charset="utf-8">
<title>AgentDB Local CI</title>
<h1>AgentDB Local CI</h1>
<ul>
  <li><a href="latest/report.html">Latest completed run</a></li>
  <li><a href="current/report.html">Current run</a></li>
</ul>
EOF
}

finish() {
  EXIT_CODE=$?
  local end_epoch
  end_epoch="$(date +%s)"
  ln -sfn "$LOG_DIR" "$RUN_ROOT/latest"
  if [[ "$EXIT_CODE" == "0" ]]; then
    write_status passed 0 "$end_epoch"
    write_report passed
  else
    write_status failed "$EXIT_CODE" "$end_epoch"
    write_report failed
  fi
  if [[ -x "$ROOT/scripts/local-ci-prune.sh" ]]; then
    "$ROOT/scripts/local-ci-prune.sh" --root "$RUN_ROOT" --keep "${LOCAL_CI_KEEP_RUNS:-30}" >/dev/null || true
  fi
  if [[ -x "$ROOT/scripts/local-ci-summary.sh" ]]; then
    "$ROOT/scripts/local-ci-summary.sh" --root "$RUN_ROOT" --quiet || true
  fi
  echo "local ci status: $LOG_DIR/status.env"
  echo "local ci summary: $RUN_ROOT/summary.html"
  exit "$EXIT_CODE"
}

trap finish EXIT
write_status running "" ""
ln -sfn "$LOG_DIR" "$RUN_ROOT/current"

run_stage() {
  local name="$1"
  shift
  CURRENT_STAGE="$name"
  STAGE_NAMES+=("$name")
  write_status running "" ""
  write_report running
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
  CURRENT_STAGE="$name"
  STAGE_NAMES+=("$name")
  write_status running "" ""
  write_report running
  local log="$LOG_DIR/${name}.log"
  echo "==> $name"
  (
    cd "$ROOT"
    bash -lc "$*"
  ) 2>&1 | tee "$log"
}

if [[ "$RUN_CARGO" == "1" ]]; then
  if [[ "$RUN_FMT" == "1" ]]; then
    run_stage cargo-fmt cargo fmt --check
  fi
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

if [[ "$RUN_CHAOS" == "1" ]]; then
  chaos_args=(--chaos --chaos-iterations "$CHAOS_ITERATIONS")
  if [[ -n "$CHAOS_SEED" ]]; then
    chaos_args+=(--chaos-seed "$CHAOS_SEED")
  fi
  run_stage real-cluster-chaos scripts/real-cluster-smoke-compose.sh "${chaos_args[@]}"
fi

echo "local ci passed; logs: $LOG_DIR"
