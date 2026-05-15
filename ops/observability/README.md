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

Backfill metadata snapshots into the raw export table:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... \
    --include tenants,agents,sessions,artifacts,records,skills,skill_bindings \
    --backfill
```

The current implementation exports event and runtime metadata envelopes from
`/api/tenants/{tenant_id}/exports/watch`. Persistent AgentDB runtimes return
durable `ledger:...` cursors; in-memory/dev runtimes return `v2:...` event
watch cursors. It is suitable for local validation of schema, ClickHouse
reachability, cursor resume, metadata snapshot backfill, tombstone handling,
consumer leases, and dashboard queries.

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
