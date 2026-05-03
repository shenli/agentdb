#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <server-host> [ssh-user]" >&2
  exit 1
fi

SERVER_HOST="$1"
SSH_USER="${2:-ec2-user}"
SSH_TARGET="${SSH_USER}@${SERVER_HOST}"

echo "==> copying config and helper files to ${SSH_TARGET}"
ssh "${SSH_TARGET}" 'mkdir -p ~/agentdb-deploy/config ~/agentdb-deploy/logs'
scp config/agentdb.toml "${SSH_TARGET}:~/agentdb-deploy/config/agentdb.toml"

echo "==> ensuring Rust build prerequisites exist on ${SSH_TARGET}"
ssh "${SSH_TARGET}" <<'EOF'
set -euo pipefail
if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi
EOF

echo "==> bootstrap complete"
