# Observability Export

This directory contains local deployment helpers for the AgentDB to ClickHouse
observability export path.

## Local ClickHouse

Start ClickHouse:

```bash
docker compose -f ops/observability/clickhouse-compose.yml up -d
```

Initialize the AgentDB schema:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse init --apply
```

Check both sides:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse doctor --tenant-id ten_...
```

Run a one-shot export batch:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse
```

Tail continuously:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse --backfill --tail
```

Write both native AgentDB tables and OTel-shaped ClickStack-friendly
projections:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse \
    --mode native,otel --backfill --tail
```

Backfill metadata snapshots into the raw export table:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... \
    --include tenants,agents,sessions,artifacts,records,skills,skill_bindings \
    --backfill
```

Archive raw export envelopes to JSONL locally or in S3-compatible object
storage:

```bash
agentdbctl export jsonl run --tenant-id ten_... --output ./agentdb-events.jsonl

AWS_REGION=us-east-1 \
agentdbctl export jsonl run --tenant-id ten_... \
  --output s3://agentdb-exports/tenants/ten_.../events.jsonl \
  --backfill --max-batches 100
```

The current implementation exports event and runtime metadata envelopes from
`/api/tenants/{tenant_id}/exports/watch`. Persistent AgentDB runtimes return
durable `ledger:...` cursors; in-memory/dev runtimes return `v2:...` event
watch cursors. It is suitable for local validation of schema, ClickHouse
reachability, cursor resume, metadata snapshot backfill, tombstone handling,
consumer leases, JSONL archival, and dashboard queries.

`--mode otel` writes event-derived rows into `agentdb_otel_logs` and
`agentdb_otel_traces` only. `--mode native,otel` writes the native raw/event
tables plus those projections. Trace IDs are deterministic from
`tenant_id/session_id`, span IDs are deterministic from event IDs, and
`source_refs` are preserved in trace `links_json`.

## HA Connector Pattern

Run two or more tailing workers with the same consumer name and distinct lease
owners:

```text
                 same consumer: prod-clickhouse

  worker-a  ---- claim lease ----> AgentDB export consumer
      |                              last_acked_cursor
      | insert then ack              lease_owner=worker-a
      v
  ClickHouse

  worker-b  ---- standby claim after lease expiry
```

Recommended production shape:

- Use `--consumer prod-clickhouse` for every worker that feeds the same
  ClickHouse tables.
- Set a unique `--lease-owner`, for example the pod name or instance id.
- Keep `--lease-ms` comfortably above expected ClickHouse insert latency.
- Alert on increasing `lag_ms`, `pending_items`, expired leases, and stale
  `high_watermark`.
- Never ack before ClickHouse accepts the batch. The ClickHouse tables use
  stable ids so retrying an already inserted batch is safe.

Stop ClickHouse:

```bash
docker compose -f ops/observability/clickhouse-compose.yml down
```
