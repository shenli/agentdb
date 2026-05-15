#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENTDBCTL="${AGENTDBCTL:-$ROOT_DIR/target/debug/agentdbctl}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb}"
AGENTDB_URL="${AGENTDB_URL:-http://127.0.0.1:7878}"
TENANT_ID="${TENANT_ID:-}"

clickhouse_query() {
  local sql="$1"
  curl -fsS -H 'Content-Type: text/plain; charset=utf-8' --data-binary "$sql" "$CLICKHOUSE_URL"
}

if [[ ! -x "$AGENTDBCTL" ]]; then
  cargo build -q -p agentdbctl
fi

echo "initializing AgentDB ClickHouse schema"
for _ in $(seq 1 60); do
  if clickhouse_query "SELECT 1" >/dev/null 2>&1; then
    clickhouse_ready=1
    break
  fi
  sleep 1
done
if [[ "${clickhouse_ready:-0}" != "1" ]]; then
  echo "ClickHouse did not become reachable at $CLICKHOUSE_URL" >&2
  exit 1
fi
CLICKHOUSE_URL="$CLICKHOUSE_URL" "$AGENTDBCTL" export clickhouse migrate --apply >/dev/null

echo "checking ClickStack compatibility views"
expected=$(
  clickhouse_query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('agentdb_otel_logs', 'agentdb_otel_traces', 'agentdb_clickstack_logs', 'agentdb_clickstack_traces', 'otel_logs', 'otel_traces')"
)
if [[ "$expected" != "6" ]]; then
  echo "expected 6 OTel/ClickStack tables or views, got $expected" >&2
  exit 1
fi

clickhouse_query "SELECT Timestamp, TimestampTime, TraceId, SpanId, ServiceName, Body FROM agentdb_clickstack_logs LIMIT 0" >/dev/null
clickhouse_query "SELECT Timestamp, TimestampTime, TraceId, SpanId, ParentSpanId, SpanName, Duration FROM agentdb_clickstack_traces LIMIT 0" >/dev/null

if [[ -n "$TENANT_ID" ]]; then
  echo "running AgentDB export doctor for tenant $TENANT_ID"
  CLICKHOUSE_URL="$CLICKHOUSE_URL" AGENTDB_URL="$AGENTDB_URL" \
    "$AGENTDBCTL" export clickhouse doctor --tenant-id "$TENANT_ID" >/dev/null

  if [[ "${AGENTDB_EXPORT_SMOKE_RUN:-0}" == "1" ]]; then
    echo "copying one bounded native+otel export batch for tenant $TENANT_ID"
    CLICKHOUSE_URL="$CLICKHOUSE_URL" AGENTDB_URL="$AGENTDB_URL" \
      "$AGENTDBCTL" export clickhouse run \
        --tenant-id "$TENANT_ID" \
        --consumer "${AGENTDB_EXPORT_SMOKE_CONSUMER:-smoke-clickhouse}" \
        --lease-owner "${AGENTDB_EXPORT_SMOKE_LEASE_OWNER:-smoke-clickhouse-$$}" \
        --mode native,otel \
        --backfill \
        --max-batches "${AGENTDB_EXPORT_SMOKE_MAX_BATCHES:-1}" >/dev/null
  fi
fi

echo "ok"
