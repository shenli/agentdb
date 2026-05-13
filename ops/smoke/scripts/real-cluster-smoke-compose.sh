#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_FILE="$ROOT/examples/cluster-smoke/docker-compose.yml"
WORK_DIR="${AGENTDB_CLUSTER_SMOKE_WORK_DIR:-/tmp/agentdb-cluster-smoke}"
POSTGRES_USER="${AGENTDB_CLUSTER_SMOKE_POSTGRES_USER:-agentdb}"
POSTGRES_PASSWORD="${AGENTDB_CLUSTER_SMOKE_POSTGRES_PASSWORD:-agentdb}"
POSTGRES_DB="${AGENTDB_CLUSTER_SMOKE_POSTGRES_DB:-agentdb_smoke}"
POSTGRES_PORT="${AGENTDB_CLUSTER_SMOKE_POSTGRES_PORT:-55432}"
MINIO_PORT="${AGENTDB_CLUSTER_SMOKE_MINIO_PORT:-19000}"
PROJECT_NAME="${AGENTDB_CLUSTER_SMOKE_COMPOSE_PROJECT:-agentdb-cluster-smoke}"
KEEP_RUNNING=0
for arg in "$@"; do
  if [[ "$arg" == "--keep-running" ]]; then
    KEEP_RUNNING=1
  fi
done

export AGENTDB_CLUSTER_SMOKE_POSTGRES_USER="$POSTGRES_USER"
export AGENTDB_CLUSTER_SMOKE_POSTGRES_PASSWORD="$POSTGRES_PASSWORD"
export AGENTDB_CLUSTER_SMOKE_POSTGRES_DB="$POSTGRES_DB"
export AGENTDB_CLUSTER_SMOKE_POSTGRES_PORT="$POSTGRES_PORT"
export AGENTDB_CLUSTER_SMOKE_MINIO_PORT="$MINIO_PORT"
export AGENTDB_CLUSTER_SMOKE_MINIO_CONSOLE_PORT="${AGENTDB_CLUSTER_SMOKE_MINIO_CONSOLE_PORT:-19001}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP:-true}"
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"
export AGENTDB_S3_ENDPOINT="${AGENTDB_S3_ENDPOINT:-http://127.0.0.1:$MINIO_PORT}"
export AGENTDB_S3_BUCKET="${AGENTDB_S3_BUCKET:-agentdb-smoke}"
export AGENTDB_S3_REGION="${AGENTDB_S3_REGION:-us-east-1}"
export AGENTDB_META_URL="${AGENTDB_META_URL:-postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:$POSTGRES_PORT/$POSTGRES_DB}"
export AGENTDB_CLUSTER_SMOKE_WORK_DIR="$WORK_DIR"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

collect_diagnostics() {
  mkdir -p "$WORK_DIR/diagnostics"
  compose ps >"$WORK_DIR/diagnostics/compose-ps.txt" 2>&1 || true
  compose logs --no-color postgres >"$WORK_DIR/diagnostics/postgres.log" 2>&1 || true
  compose logs --no-color minio >"$WORK_DIR/diagnostics/minio.log" 2>&1 || true
  compose logs --no-color minio-init >"$WORK_DIR/diagnostics/minio-init.log" 2>&1 || true
}

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "postgres did not become ready" >&2
  return 1
}

cleanup() {
  local status=$?
  collect_diagnostics
  if [[ "${AGENTDB_CLUSTER_SMOKE_KEEP_DEPS:-0}" != "1" && "$KEEP_RUNNING" != "1" ]]; then
    compose down
  fi
  if [[ $status -ne 0 ]]; then
    echo "real cluster smoke failed; diagnostics kept under $WORK_DIR" >&2
  fi
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the compose-backed smoke runner" >&2
  exit 2
fi

mkdir -p "$WORK_DIR/diagnostics"

cd "$ROOT"
compose up -d postgres minio
wait_for_postgres
compose up minio-init

echo "Postgres: $AGENTDB_META_URL"
echo "MinIO:    $AGENTDB_S3_ENDPOINT bucket=$AGENTDB_S3_BUCKET"
echo "Work dir: $WORK_DIR"
if [[ "$KEEP_RUNNING" == "1" ]]; then
  echo "Keeping dependency containers because --keep-running was passed."
fi

ops/smoke/scripts/real-cluster-smoke.sh --keep-work-dir "$@"

collect_diagnostics
echo "real cluster smoke passed; diagnostics kept under $WORK_DIR"
