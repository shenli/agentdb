#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${AGENTDB_META_URL:-}" ]]; then
  echo "AGENTDB_META_URL is required, for example postgres://agentdb:agentdb@127.0.0.1:5432/agentdb_smoke" >&2
  exit 2
fi

export AGENTDB_S3_ENDPOINT="${AGENTDB_S3_ENDPOINT:-http://127.0.0.1:9000}"
export AGENTDB_S3_BUCKET="${AGENTDB_S3_BUCKET:-agentdb-smoke}"
export AGENTDB_S3_REGION="${AGENTDB_S3_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP:-true}"

cd "$ROOT"
cargo build -q -p agentdb-server -p agentdbctl -p agentdb-cluster-smoke

exec target/debug/agentdb-cluster-smoke \
  --server-bin target/debug/agentdb \
  --reset-meta \
  "$@"
