#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENTDBCTL="${AGENTDBCTL:-$ROOT_DIR/target/debug/agentdbctl}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb}"
HYPERDX_URL="${HYPERDX_URL:-http://127.0.0.1:8080}"

clickhouse_query() {
  local sql="$1"
  curl -fsS -H 'Content-Type: text/plain; charset=utf-8' --data-binary "$sql" "$CLICKHOUSE_URL"
}

if [[ ! -x "$AGENTDBCTL" ]]; then
  cargo build -q -p agentdbctl
fi

echo "checking HyperDX/ClickStack UI at $HYPERDX_URL"
for _ in $(seq 1 60); do
  if curl -fsS "$HYPERDX_URL" >/dev/null 2>&1; then
    hyperdx_ready=1
    break
  fi
  sleep 1
done
if [[ "${hyperdx_ready:-0}" != "1" ]]; then
  echo "HyperDX UI did not become reachable at $HYPERDX_URL" >&2
  exit 1
fi

echo "initializing AgentDB ClickHouse schema"
CLICKHOUSE_URL="$CLICKHOUSE_URL" "$AGENTDBCTL" export clickhouse migrate --apply >/dev/null

echo "validating ClickStack-compatible source views"
clickhouse_query "SELECT TimestampTime, TraceId, SpanId, ServiceName, Body FROM agentdb_clickstack_logs LIMIT 0" >/dev/null
clickhouse_query "SELECT TimestampTime, TraceId, SpanId, ParentSpanId, SpanName, Duration FROM agentdb_clickstack_traces LIMIT 0" >/dev/null
clickhouse_query "SELECT TimestampTime, TraceId, SpanId, ServiceName, Body FROM otel_logs LIMIT 0" >/dev/null
clickhouse_query "SELECT TimestampTime, TraceId, SpanId, ParentSpanId, SpanName, Duration FROM otel_traces LIMIT 0" >/dev/null

echo "validating dashboard query bundle"
"$AGENTDBCTL" export clickhouse queries >/dev/null

echo "ok"
