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
pkill -f "agentdbctl export jsonl run" || true
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
  clickhouse_privacy_args=(--payload-mode "${AGENTDB_EXPORT_PAYLOAD_MODE:-full}")
  if [[ -n "${AGENTDB_EXPORT_ALLOW_OBJECT_TYPES:-}" ]]; then
    clickhouse_privacy_args+=(--allow-object-type "$AGENTDB_EXPORT_ALLOW_OBJECT_TYPES")
  fi
  if [[ -n "${AGENTDB_EXPORT_DENY_OBJECT_TYPES:-}" ]]; then
    clickhouse_privacy_args+=(--deny-object-type "$AGENTDB_EXPORT_DENY_OBJECT_TYPES")
  fi
  if [[ -n "${AGENTDB_EXPORT_ALLOW_EVENT_KINDS:-}" ]]; then
    clickhouse_privacy_args+=(--allow-event-kind "$AGENTDB_EXPORT_ALLOW_EVENT_KINDS")
  fi
  if [[ -n "${AGENTDB_EXPORT_DENY_EVENT_KINDS:-}" ]]; then
    clickhouse_privacy_args+=(--deny-event-kind "$AGENTDB_EXPORT_DENY_EVENT_KINDS")
  fi
  if [[ -n "${AGENTDB_EXPORT_REDACT_FIELDS:-}" ]]; then
    IFS=',' read -ra redact_fields <<< "$AGENTDB_EXPORT_REDACT_FIELDS"
    for field in "${redact_fields[@]}"; do
      clickhouse_privacy_args+=(--redact-field "$field")
    done
  fi
  target/debug/agentdbctl export clickhouse migrate --apply
  target/debug/agentdbctl export clickhouse run \
    --tenant-id "$AGENTDB_EXPORT_TENANT_ID" \
    --include tenants,agents,sessions,artifacts,records,skills,skill_bindings \
    --mode native \
    "${clickhouse_privacy_args[@]}" \
    --metrics-output "${AGENTDB_EXPORT_BACKFILL_METRICS_OUTPUT:-$HOME/agentdb-deploy/logs/clickhouse-export-backfill-status.json}" \
    --backfill \
    --max-batches 1000
  nohup target/debug/agentdbctl export clickhouse run \
    --tenant-id "$AGENTDB_EXPORT_TENANT_ID" \
    --consumer "${AGENTDB_EXPORT_CONSUMER:-prod-clickhouse}" \
    --lease-owner "$AGENTDB_EXPORT_LEASE_OWNER" \
    --lease-ms "${AGENTDB_EXPORT_LEASE_MS:-30000}" \
    --include events \
    --mode "${AGENTDB_EXPORT_MODE:-native}" \
    --max-retries "${AGENTDB_EXPORT_MAX_RETRIES:-6}" \
    --retry-initial-backoff-ms "${AGENTDB_EXPORT_RETRY_INITIAL_BACKOFF_MS:-250}" \
    "${clickhouse_privacy_args[@]}" \
    --metrics-output "${AGENTDB_EXPORT_METRICS_OUTPUT:-$HOME/agentdb-deploy/logs/clickhouse-export-status.json}" \
    --backfill \
    --tail \
    > ~/agentdb-deploy/logs/clickhouse-export.log 2>&1 &
fi
EOF

echo "==> starting JSONL exporter when configured"
ssh "${SSH_TARGET}" <<'EOF'
set -euo pipefail
if [[ -f ~/agentdb-deploy/jsonl-export.env ]]; then
  cd ~/agentdb-src
  set -a
  source ~/agentdb-deploy/jsonl-export.env
  set +a
  AGENTDB_JSONL_EXPORT_LEASE_OWNER="${AGENTDB_JSONL_EXPORT_LEASE_OWNER:-$(hostname)-jsonl-export}"
  jsonl_privacy_args=(--payload-mode "${AGENTDB_JSONL_EXPORT_PAYLOAD_MODE:-full}")
  if [[ -n "${AGENTDB_JSONL_EXPORT_ALLOW_OBJECT_TYPES:-}" ]]; then
    jsonl_privacy_args+=(--allow-object-type "$AGENTDB_JSONL_EXPORT_ALLOW_OBJECT_TYPES")
  fi
  if [[ -n "${AGENTDB_JSONL_EXPORT_DENY_OBJECT_TYPES:-}" ]]; then
    jsonl_privacy_args+=(--deny-object-type "$AGENTDB_JSONL_EXPORT_DENY_OBJECT_TYPES")
  fi
  if [[ -n "${AGENTDB_JSONL_EXPORT_ALLOW_EVENT_KINDS:-}" ]]; then
    jsonl_privacy_args+=(--allow-event-kind "$AGENTDB_JSONL_EXPORT_ALLOW_EVENT_KINDS")
  fi
  if [[ -n "${AGENTDB_JSONL_EXPORT_DENY_EVENT_KINDS:-}" ]]; then
    jsonl_privacy_args+=(--deny-event-kind "$AGENTDB_JSONL_EXPORT_DENY_EVENT_KINDS")
  fi
  if [[ -n "${AGENTDB_JSONL_EXPORT_REDACT_FIELDS:-}" ]]; then
    IFS=',' read -ra jsonl_redact_fields <<< "$AGENTDB_JSONL_EXPORT_REDACT_FIELDS"
    for field in "${jsonl_redact_fields[@]}"; do
      jsonl_privacy_args+=(--redact-field "$field")
    done
  fi
  nohup target/debug/agentdbctl export jsonl run \
    --tenant-id "$AGENTDB_JSONL_EXPORT_TENANT_ID" \
    --output "$AGENTDB_JSONL_EXPORT_URI" \
    --consumer "${AGENTDB_JSONL_EXPORT_CONSUMER:-prod-jsonl}" \
    --lease-owner "$AGENTDB_JSONL_EXPORT_LEASE_OWNER" \
    --lease-ms "${AGENTDB_JSONL_EXPORT_LEASE_MS:-30000}" \
    --include "${AGENTDB_JSONL_EXPORT_INCLUDE:-events}" \
    --backfill \
    --tail \
    --max-batches "${AGENTDB_JSONL_EXPORT_MAX_BATCHES:-1000}" \
    "${jsonl_privacy_args[@]}" \
    --metrics-output "${AGENTDB_JSONL_EXPORT_METRICS_OUTPUT:-$HOME/agentdb-deploy/logs/jsonl-export-status.json}" \
    > ~/agentdb-deploy/logs/jsonl-export.log 2>&1 &
fi
EOF

echo "==> running smoke flow"
infra/aws/scripts/run-tiered-storage-smoke.sh "http://${SERVER_HOST}:7878"
