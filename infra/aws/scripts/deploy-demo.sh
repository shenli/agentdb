#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <server-host> [ssh-user]" >&2
  exit 1
fi

SERVER_HOST="$1"
SSH_USER="${2:-ec2-user}"
SSH_TARGET="${SSH_USER}@${SERVER_HOST}"

cd "$ROOT"

infra/aws/scripts/bootstrap-agentdb.sh "${SERVER_HOST}" "${SSH_USER}"

echo "==> syncing repo to ${SSH_TARGET}"
rsync -az --delete \
  --exclude target \
  --exclude .git \
  ./ "${SSH_TARGET}:~/agentdb-src/"

echo "==> building and starting agentdb-server on ${SSH_TARGET}"
ssh "${SSH_TARGET}" <<'EOF'
set -euo pipefail
cd ~/agentdb-src
cargo build -p agentdb-server
pkill -f "agentdb serve" || true
nohup target/debug/agentdb serve --config config/agentdb.toml \
  > ~/agentdb-deploy/logs/agentdb.log 2>&1 &
EOF

echo "==> waiting for health"
for _ in $(seq 1 30); do
  if curl -fsS "http://${SERVER_HOST}:7878/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "==> running smoke flow"
infra/aws/scripts/run-tiered-storage-smoke.sh "http://${SERVER_HOST}:7878"
