#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <agentdb-base-url>" >&2
  exit 1
fi

BASE_URL="$1"
export AGENTDB_URL="${BASE_URL}"

echo "==> health"
curl -fsS "${BASE_URL%/}/health/ready" >/dev/null

echo "==> session-first demo"
PYTHONPATH=clients/python python3 examples/agents/python/session_first_demo.py

echo "==> tiered-storage demo"
PYTHONPATH=clients/python python3 examples/agents/python/session_tiering_demo.py

echo "==> ticket agent tiered demo"
PYTHONPATH=clients/python python3 examples/agents/python/ticket_agent_tiered_demo.py

echo "==> storage summary"
curl -fsS "${BASE_URL%/}/api/storage/summary"
echo
