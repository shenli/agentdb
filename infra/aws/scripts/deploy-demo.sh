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
cargo build -p agentdb-server -p agentdbctl
pkill -f "agentdb serve" || true
pkill -f "agentdbctl export clickhouse run" || true
if [[ -f ~/agentdb-deploy/agentdb.env ]]; then
  set -a
  source ~/agentdb-deploy/agentdb.env
  set +a
fi
nohup target/debug/agentdb serve --config ~/agentdb-deploy/config/agentdb.toml \
  > ~/agentdb-deploy/logs/agentdb.log 2>&1 &
EOF

echo "==> waiting for health"
for _ in $(seq 1 30); do
  if curl -fsS "http://${SERVER_HOST}:7878/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "==> starting ClickHouse exporter when configured"
ssh "${SSH_TARGET}" <<'EOF'
set -euo pipefail
if [[ -f ~/agentdb-deploy/clickhouse-export.env ]]; then
  cd ~/agentdb-src
  set -a
  source ~/agentdb-deploy/clickhouse-export.env
  set +a
  AGENTDB_EXPORT_LEASE_OWNER="${AGENTDB_EXPORT_LEASE_OWNER:-$(hostname)-clickhouse-export}"
  target/debug/agentdbctl export clickhouse init --apply
  target/debug/agentdbctl export clickhouse run \
    --tenant-id "$AGENTDB_EXPORT_TENANT_ID" \
    --include tenants,agents,sessions,artifacts,records,skills,skill_bindings \
    --backfill \
    --max-batches 1000
  nohup target/debug/agentdbctl export clickhouse run \
    --tenant-id "$AGENTDB_EXPORT_TENANT_ID" \
    --consumer "${AGENTDB_EXPORT_CONSUMER:-prod-clickhouse}" \
    --lease-owner "$AGENTDB_EXPORT_LEASE_OWNER" \
    --lease-ms "${AGENTDB_EXPORT_LEASE_MS:-30000}" \
    --include events \
    --backfill \
    --tail \
    > ~/agentdb-deploy/logs/clickhouse-export.log 2>&1 &
fi
EOF

echo "==> running smoke flow"
infra/aws/scripts/run-tiered-storage-smoke.sh "http://${SERVER_HOST}:7878"
