-- DuckDB demo queries for AgentDB JSONL export archives.
--
-- Usage:
--   duckdb -cmd ".param set jsonl_path './agentdb-events.jsonl'" \
--     -init ops/observability/duckdb-jsonl-demo.sql
--
-- S3 usage:
--   duckdb -cmd "INSTALL httpfs; LOAD httpfs;" \
--     -cmd "SET s3_region='us-east-1';" \
--     -cmd ".param set jsonl_path 's3://agentdb-exports/tenants/ten_x/events.jsonl'" \
--     -init ops/observability/duckdb-jsonl-demo.sql

CREATE OR REPLACE VIEW agentdb_export_items AS
SELECT
  export_version,
  cursor,
  op,
  object_type,
  tenant_id,
  agent_id,
  session_id,
  object_id,
  seq,
  created_at_ms,
  to_timestamp(created_at_ms / 1000.0) AS created_at,
  observed_at_ms,
  to_timestamp(observed_at_ms / 1000.0) AS observed_at,
  cell_id,
  placement_scope_id,
  tablet_id,
  tablet_epoch,
  applied_index,
  data,
  tombstone
FROM read_json_auto(getvariable('jsonl_path'), format = 'newline_delimited');

CREATE OR REPLACE VIEW agentdb_events AS
SELECT
  tenant_id,
  agent_id,
  session_id,
  object_id AS event_id,
  seq,
  json_extract_string(data, '$.kind') AS kind,
  created_at_ms,
  created_at,
  observed_at_ms,
  observed_at,
  cell_id,
  placement_scope_id,
  tablet_id,
  tablet_epoch,
  applied_index,
  json_extract_string(data, '$.payload.model') AS model,
  json_extract_string(data, '$.payload.provider') AS provider,
  json_extract_string(data, '$.payload.status') AS status,
  json_extract_string(data, '$.payload.tool_name') AS tool_name,
  json_extract_string(data, '$.payload.error_kind') AS error_kind,
  data
FROM agentdb_export_items
WHERE object_type = 'event';

CREATE OR REPLACE VIEW agentdb_tombstones AS
SELECT
  tenant_id,
  object_type,
  object_id,
  json_extract_string(tombstone, '$.reason') AS reason,
  json_extract_string(tombstone, '$.deleted_at_ms')::UBIGINT AS deleted_at_ms,
  to_timestamp(json_extract_string(tombstone, '$.deleted_at_ms')::UBIGINT / 1000.0) AS deleted_at,
  tombstone
FROM agentdb_export_items
WHERE op = 'delete';

-- Recent event volume by kind.
SELECT
  kind,
  count(*) AS events,
  min(created_at) AS first_seen,
  max(created_at) AS last_seen
FROM agentdb_events
GROUP BY kind
ORDER BY events DESC, kind;

-- Export lag by tenant/session based on observed_at - created_at.
SELECT
  tenant_id,
  session_id,
  count(*) AS rows,
  max(observed_at_ms - created_at_ms) AS max_export_lag_ms,
  avg(observed_at_ms - created_at_ms) AS avg_export_lag_ms
FROM agentdb_export_items
GROUP BY tenant_id, session_id
ORDER BY max_export_lag_ms DESC NULLS LAST
LIMIT 20;

-- Tombstone reasons, including retention_gc and tenant_purge.
SELECT
  reason,
  object_type,
  count(*) AS tombstones
FROM agentdb_tombstones
GROUP BY reason, object_type
ORDER BY tombstones DESC, reason, object_type;

-- Tablet/cell placement distribution visible in the export envelope.
SELECT
  coalesce(cell_id, 'unknown') AS cell_id,
  coalesce(placement_scope_id, 'unknown') AS placement_scope_id,
  tablet_id,
  count(*) AS rows
FROM agentdb_export_items
GROUP BY cell_id, placement_scope_id, tablet_id
ORDER BY rows DESC, cell_id, placement_scope_id, tablet_id;
