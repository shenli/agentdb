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
if ssh "${SSH_TARGET}" 'test -f ~/agentdb.toml'; then
  ssh "${SSH_TARGET}" 'cp ~/agentdb.toml ~/agentdb-deploy/config/agentdb.toml'
else
  scp config/agentdb.toml "${SSH_TARGET}:~/agentdb-deploy/config/agentdb.toml"
fi
if ssh "${SSH_TARGET}" 'test -f ~/agentdb.env'; then
  ssh "${SSH_TARGET}" 'cp ~/agentdb.env ~/agentdb-deploy/agentdb.env'
fi
if ssh "${SSH_TARGET}" 'test -f ~/clickhouse-export.env'; then
  ssh "${SSH_TARGET}" 'cp ~/clickhouse-export.env ~/agentdb-deploy/clickhouse-export.env'
fi
if ssh "${SSH_TARGET}" 'test -f ~/jsonl-export.env'; then
  ssh "${SSH_TARGET}" 'cp ~/jsonl-export.env ~/agentdb-deploy/jsonl-export.env'
fi

echo "==> ensuring Rust build prerequisites exist on ${SSH_TARGET}"
ssh "${SSH_TARGET}" <<'EOF'
set -euo pipefail
if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi
EOF

echo "==> bootstrap complete"
