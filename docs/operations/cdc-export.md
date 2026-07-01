# CDC And Export

This runbook covers the single-node CDC/export operator story. AgentDB exposes
runtime change data through watch APIs and durable export APIs. Use the export
surface for replication into observability, archive, and lakehouse sinks.

For the detailed CLI reference, see [agentdbctl.md](agentdbctl.md). For the
API contract, see [../reference/api-sdk.md](../reference/api-sdk.md). For the
design, see [../design/observability-export.md](../design/observability-export.md).

## Surfaces

AgentDB has two related change surfaces:

| Surface | Use |
|---|---|
| `watch` | Runtime observers, dashboards, supervisors, and session/agent/tenant scoped tails. |
| `exports/watch` | Durable export envelopes for external sinks such as ClickHouse, JSONL, Parquet, and future stream bridges. |

Use `exports/watch` and managed consumers for CDC-style replication. It has
durable cursors on persistent runtimes, backfill support, tombstone envelopes,
and consumer checkpoint APIs.

## Inspect Export Capability

Check one tenant:

```bash
agentdbctl export clickhouse status --tenant-id ten_...
```

Inspect all managed export consumers on the node:

```bash
agentdbctl raw get /api/admin/exports
```

Status output should show export capability, high watermark, oldest retained
cursor, pending items, lag, and managed consumer lease state when a consumer is
configured.

## ClickHouse Golden Path

Start local ClickHouse:

```bash
docker compose -f ops/observability/clickhouse-compose.yml up -d
```

Apply schema migrations:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse migrate --apply
```

Run preflight checks:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl -o json export clickhouse doctor --tenant-id ten_...
```

Backfill and then tail:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse run \
    --tenant-id ten_... \
    --consumer prod-clickhouse \
    --backfill \
    --tail \
    --metrics-output /var/lib/agentdb/export-clickhouse-status.json
```

Use `--mode native,otel` when ClickStack/OpenTelemetry-shaped projections are
needed in addition to native AgentDB tables.

## JSONL And Parquet Archive Path

Write export envelopes to a local JSONL archive:

```bash
agentdbctl export jsonl run \
  --tenant-id ten_... \
  --output ./agentdb-events.jsonl \
  --backfill
```

Write to object storage:

```bash
AWS_REGION=us-east-1 \
agentdbctl export jsonl run \
  --tenant-id ten_... \
  --output s3://agentdb-exports/tenants/ten_.../events.jsonl \
  --backfill \
  --max-batches 100
```

Convert JSONL to Parquet:

```bash
agentdbctl export parquet from-jsonl \
  --input ./agentdb-events.jsonl \
  --output ./agentdb-events.parquet
```

Object-store outputs are uploaded as completed objects. Use bounded
`--max-batches` runs for archival batches rather than an unbounded object-store
tail.

## Managed Consumer Recovery

Official connectors should use a named managed consumer:

- claim a lease before writing
- write a batch to the sink
- ack only after the sink accepts the batch
- resume from the last acked cursor after restart
- use rewind only for intentional replay

Run two or more tailing processes with the same `--consumer` and distinct
`--lease-owner` values for standby failover. The active lease holder writes and
acks; a standby can claim after `--lease-ms` expires.

Inspect lag and lease state:

```bash
agentdbctl export clickhouse status --tenant-id ten_... --consumer prod-clickhouse
agentdbctl raw get /api/admin/exports
```

## Privacy Controls

Apply minimization before external sink writes:

```bash
agentdbctl export clickhouse run \
  --tenant-id ten_... \
  --consumer prod-clickhouse \
  --deny-event-kind debug,trace \
  --redact-field data.payload.api_key \
  --payload-mode hash \
  --backfill \
  --tail
```

The same allow/deny object type, allow/deny event kind, redaction, and
`full|hash|drop` payload modes are available on JSONL export.

## Failure Handling

If `doctor` fails:

- fix AgentDB reachability, ClickHouse reachability, schema migrations, grants,
  or write-smoke permissions before running a tailer
- use `--skip-write-smoke` only for read-only validation roles

If the exporter falls behind:

- inspect `/api/admin/exports`
- check sink availability and insert errors
- increase batch size only after confirming the sink can keep up
- add standby processes for availability, not for parallel writes to one
  consumer unless the connector explicitly supports that pattern

If a cursor is invalid:

- read the `oldest_available_cursor` from status or the error response
- rewind the managed consumer to a valid cursor only when duplicate sink writes
  are acceptable or deduplicated
- run a fresh backfill when the required cursor is no longer retained

If ClickHouse contains duplicate-looking rows:

- confirm connector deduplication tokens are stable
- confirm the sink tables/views query the latest envelope by cursor/object
- check whether an intentional rewind replayed already-exported data

## Routine Checks

Daily:

- `agentdbctl raw get /api/admin/exports`
- sink-specific status, such as `agentdbctl export clickhouse status`
- metrics file from the running exporter, if configured

After upgrades:

- rerun `export clickhouse doctor`
- rerun schema migration in dry-run or apply mode
- verify one bounded export batch before restarting an unbounded tail

If a ClickHouse schema rollout must be reverted, inspect the destructive plan
first:

```bash
agentdbctl export clickhouse rollback
agentdbctl -o json export clickhouse rollback | jq
```

Apply only when the external sink tables/views can be dropped and rebuilt:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse rollback --apply --allow-destructive
```

Rollback is scoped to the first-party ClickHouse export schema. It does not
roll back AgentDB runtime metadata or session state.

Before deleting old runtime data or export archives:

- confirm consumers have acked beyond the relevant cursor
- confirm the sink has its own retention and backup policy
- preserve enough JSONL/Parquet history for the deployment's audit needs
