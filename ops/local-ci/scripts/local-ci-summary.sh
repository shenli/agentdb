#!/usr/bin/env bash
set -euo pipefail

RUN_ROOT="${LOCAL_CI_RUN_ROOT:-/tmp/agentdb-local-ci}"
LIMIT="${LOCAL_CI_SUMMARY_LIMIT:-20}"
QUIET=0

usage() {
  cat <<'EOF'
Usage: ops/local-ci/scripts/local-ci-summary.sh [OPTIONS]

Builds and prints a local CI summary across recent runs.

Options:
  --root DIR    Local CI run root. Defaults to LOCAL_CI_RUN_ROOT or /tmp/agentdb-local-ci.
  --limit N     Number of recent runs to show. Defaults to LOCAL_CI_SUMMARY_LIMIT or 20.
  --quiet       Write summary files without printing text output.
  --help        Show this help.
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
    --limit)
      shift
      LIMIT="${1:-}"
      if [[ -z "$LIMIT" ]]; then
        echo "--limit requires a value" >&2
        exit 2
      fi
      ;;
    --quiet)
      QUIET=1
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

RUNS_DIR="$RUN_ROOT/runs"
mkdir -p "$RUN_ROOT"

html_escape() {
  sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g'
}

load_run() {
  local status_file="$1"
  status=""
  exit_code=""
  run_id=""
  log_dir=""
  start_epoch=""
  end_epoch=""
  duration_secs=""
  last_stage=""
  git_sha=""
  git_branch=""
  # shellcheck disable=SC1090
  source "$status_file"
}

run_link() {
  local dir="$1"
  if [[ -f "$dir/report.html" ]]; then
    printf '%s' "$dir/report.html"
  else
    printf '%s' "$dir/status.env"
  fi
}

latest_run=""
last_passed=""
last_failed=""
run_status_files=()
if [[ -d "$RUNS_DIR" ]]; then
  while IFS= read -r status_file; do
    run_status_files+=("$status_file")
  done < <(find "$RUNS_DIR" -mindepth 2 -maxdepth 2 -name status.env -print | sort)
fi

for status_file in "${run_status_files[@]}"; do
  load_run "$status_file"
  run_dir="$(dirname "$status_file")"
  latest_run="$run_dir"
  if [[ "${status:-}" == "passed" ]]; then
    last_passed="$run_dir"
  elif [[ "${status:-}" == "failed" ]]; then
    last_failed="$run_dir"
  fi
done

write_summary_env() {
  {
    printf 'run_root=%q\n' "$RUN_ROOT"
    printf 'latest_run=%q\n' "$latest_run"
    printf 'last_passed=%q\n' "$last_passed"
    printf 'last_failed=%q\n' "$last_failed"
  } > "$RUN_ROOT/summary.env"
}

describe_run_text() {
  local label="$1"
  local dir="$2"
  if [[ -z "$dir" || ! -f "$dir/status.env" ]]; then
    printf '%-12s none\n' "$label"
    return
  fi
  load_run "$dir/status.env"
  printf '%-12s %s run_id=%s branch=%s sha=%s stage=%s duration=%ss report=%s\n' \
    "$label" "${status:-unknown}" "${run_id:-}" "${git_branch:-}" "${git_sha:-}" \
    "${last_stage:-}" "${duration_secs:-}" "$(run_link "$dir")"
}

write_summary_html() {
  local summary="$RUN_ROOT/summary.html"
  {
    cat <<'EOF'
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AgentDB Local CI Summary</title>
  <style>
    :root { color-scheme: light dark; --ok: #117733; --bad: #b00020; --muted: #667085; }
    body { font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 2rem; line-height: 1.45; }
    main { max-width: 1120px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 1rem; margin: 1rem 0 2rem; }
    .card { border: 1px solid color-mix(in srgb, currentColor 18%, transparent); border-radius: 12px; padding: 1rem; }
    .status { display: inline-block; padding: .2rem .55rem; border-radius: 999px; font-weight: 700; }
    .passed { color: white; background: var(--ok); }
    .failed { color: white; background: var(--bad); }
    .running { color: black; background: #facc15; }
    .unknown { color: currentColor; background: color-mix(in srgb, currentColor 12%, transparent); }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { border-bottom: 1px solid color-mix(in srgb, currentColor 18%, transparent); padding: .55rem; text-align: left; vertical-align: top; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .muted { color: var(--muted); }
  </style>
</head>
<body>
<main>
  <h1>AgentDB Local CI Summary</h1>
EOF
    echo "  <p class=\"muted\">Generated $(date -u '+%Y-%m-%dT%H:%M:%SZ')</p>"
    echo "  <section class=\"cards\">"
    for item in "Latest:$latest_run" "Last Passed:$last_passed" "Last Failed:$last_failed"; do
      label="${item%%:*}"
      dir="${item#*:}"
      echo "    <article class=\"card\">"
      echo "      <h2>$label</h2>"
      if [[ -n "$dir" && -f "$dir/status.env" ]]; then
        load_run "$dir/status.env"
        escaped_report="$(printf '%s' "$(run_link "$dir")" | html_escape)"
        echo "      <p><span class=\"status ${status:-unknown}\">${status:-unknown}</span></p>"
        echo "      <p><code>$(printf '%s' "${run_id:-}" | html_escape)</code></p>"
        echo "      <p>stage: <code>$(printf '%s' "${last_stage:-}" | html_escape)</code></p>"
        echo "      <p>commit: <code>$(printf '%s' "${git_sha:-}" | html_escape)</code></p>"
        echo "      <p><a href=\"$escaped_report\">report</a></p>"
      else
        echo "      <p class=\"muted\">none</p>"
      fi
      echo "    </article>"
    done
    echo "  </section>"
    echo "  <h2>Recent Runs</h2>"
    echo "  <table>"
    echo "    <thead><tr><th>Status</th><th>Run</th><th>Branch</th><th>Commit</th><th>Stage</th><th>Duration</th><th>Report</th></tr></thead>"
    echo "    <tbody>"
    count=0
    for ((idx=${#run_status_files[@]} - 1; idx >= 0; idx--)); do
      status_file="${run_status_files[$idx]}"
      run_dir="$(dirname "$status_file")"
      load_run "$status_file"
      report="$(run_link "$run_dir")"
      echo "      <tr>"
      echo "        <td><span class=\"status ${status:-unknown}\">${status:-unknown}</span></td>"
      echo "        <td><code>$(printf '%s' "${run_id:-}" | html_escape)</code></td>"
      echo "        <td><code>$(printf '%s' "${git_branch:-}" | html_escape)</code></td>"
      echo "        <td><code>$(printf '%s' "${git_sha:-}" | html_escape)</code></td>"
      echo "        <td><code>$(printf '%s' "${last_stage:-}" | html_escape)</code></td>"
      echo "        <td>$(printf '%s' "${duration_secs:-}" | html_escape)s</td>"
      echo "        <td><a href=\"$(printf '%s' "$report" | html_escape)\">report</a></td>"
      echo "      </tr>"
      count=$((count + 1))
      if ((count >= LIMIT)); then
        break
      fi
    done
    echo "    </tbody>"
    echo "  </table>"
    cat <<'EOF'
</main>
</body>
</html>
EOF
  } > "$summary"

  cat > "$RUN_ROOT/index.html" <<'EOF'
<!doctype html>
<meta charset="utf-8">
<title>AgentDB Local CI</title>
<h1>AgentDB Local CI</h1>
<ul>
  <li><a href="summary.html">Summary</a></li>
  <li><a href="latest/report.html">Latest completed run</a></li>
  <li><a href="current/report.html">Current run</a></li>
</ul>
EOF
}

write_summary_env
write_summary_html

if [[ "$QUIET" != "1" ]]; then
  echo "AgentDB local CI summary"
  describe_run_text "latest" "$latest_run"
  describe_run_text "passed" "$last_passed"
  describe_run_text "failed" "$last_failed"
  echo "summary: $RUN_ROOT/summary.html"
fi
