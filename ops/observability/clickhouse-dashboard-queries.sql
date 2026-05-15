-- AgentDB ClickHouse dashboard starter queries.
--
-- Usage:
--   clickhouse-client --param_tenant_id=ten_... \
--     --queries-file ops/observability/clickhouse-dashboard-queries.sql
--
-- For HTTP-only deployments, copy one query at a time into the ClickHouse SQL
-- console and replace {tenant_id:String} with the tenant parameter or literal.

-- Recent failures
SELECT created_at, session_id, kind, error_kind, error_message
FROM agentdb_events
WHERE tenant_id = {tenant_id:String}
  AND created_at >= now() - INTERVAL 1 HOUR
  AND (kind = 'tool_error' OR error_kind != '')
ORDER BY created_at DESC
LIMIT 100;

-- Model cost by model
SELECT model, sum(total_tokens) AS tokens, sum(cost_micros) / 1000000 AS cost
FROM agentdb_events
WHERE tenant_id = {tenant_id:String}
  AND created_at >= now() - INTERVAL 1 DAY
GROUP BY model
ORDER BY cost DESC;

-- Slow tools
SELECT tool_name, quantile(0.95)(duration_ms) AS p95_ms, count() AS n
FROM agentdb_events
WHERE tenant_id = {tenant_id:String}
  AND kind IN ('tool_result', 'tool_error')
  AND created_at >= now() - INTERVAL 1 DAY
GROUP BY tool_name
ORDER BY p95_ms DESC;

-- ClickStack-compatible log rows
SELECT Timestamp, ServiceName, SeverityText, Body, TraceId, SpanId
FROM agentdb_clickstack_logs
WHERE AgentDBTenantId = {tenant_id:String}
ORDER BY Timestamp DESC
LIMIT 100;

-- ClickStack-compatible trace spans
SELECT Timestamp, ServiceName, StatusCode, round(Duration / 1000000) AS duration_ms, SpanName,
       TraceId, SpanId
FROM agentdb_clickstack_traces
WHERE AgentDBTenantId = {tenant_id:String}
ORDER BY Timestamp DESC
LIMIT 100;
