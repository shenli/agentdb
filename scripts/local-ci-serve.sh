#!/usr/bin/env bash
set -euo pipefail

RUN_ROOT="${LOCAL_CI_RUN_ROOT:-/tmp/agentdb-local-ci}"
HOST="${LOCAL_CI_DASHBOARD_HOST:-127.0.0.1}"
PORT="${LOCAL_CI_DASHBOARD_PORT:-8099}"

usage() {
  cat <<'EOF'
Usage: scripts/local-ci-serve.sh [OPTIONS]

Serves the static local CI dashboard.

Options:
  --root DIR   Local CI run root. Defaults to LOCAL_CI_RUN_ROOT or /tmp/agentdb-local-ci.
  --host HOST  Bind host. Defaults to LOCAL_CI_DASHBOARD_HOST or 127.0.0.1.
  --port PORT  Bind port. Defaults to LOCAL_CI_DASHBOARD_PORT or 8099.
  --help       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      shift
      RUN_ROOT="${1:-}"
      ;;
    --host)
      shift
      HOST="${1:-}"
      ;;
    --port)
      shift
      PORT="${1:-}"
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

mkdir -p "$RUN_ROOT"
if [[ ! -f "$RUN_ROOT/index.html" ]]; then
  cat > "$RUN_ROOT/index.html" <<'EOF'
<!doctype html>
<meta charset="utf-8">
<title>AgentDB Local CI</title>
<h1>AgentDB Local CI</h1>
<p>No CI run has published a report yet.</p>
EOF
fi

echo "serving AgentDB local CI dashboard at http://$HOST:$PORT"
cd "$RUN_ROOT"
exec python3 -m http.server "$PORT" --bind "$HOST"
