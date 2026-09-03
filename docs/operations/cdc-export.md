# CDC And Export

This runbook covers the single-node CDC/export operator story. AgentDB exposes
runtime change data through one public changefeed over the durable export
ledger. Use it for replication into observability, archive, and lakehouse sinks.

For the detailed CLI reference, see [agentdbctl.md](agentdbctl.md). For the
API contract, see [../reference/api-sdk.md](../reference/api-sdk.md). For the
design, see [../design/observability-export.md](../design/observability-export.md).

## Surfaces

The export surface is a small, fixed set of routes:

| Route | Use |
|---|---|
| `GET /api/tenants/{t}/changes` | The one public changefeed: commit-ordered export envelopes for external sinks such as ClickHouse, JSONL, Parquet, and future stream bridges. |
| `POST /api/tenants/{t}/exports/backfill` | Paged current-state metadata snapshot for bootstrapping a sink. |
| `GET /api/tenants/{t}/exports/status` | Export health, high watermark, and oldest available cursor. |
| `GET /api/admin/exports` | Per-tenant `{ tenant_id, high_watermark, oldest_available_cursor }`. |

`/changes` returns durable export envelopes with `next_cursor`,
`high_watermark`, and `oldest_available_cursor`. It accepts optional `scope`
(tenant, agent, session), an `include` filter, `from_cursor` resume,
`backfill=true` for a pinned metadata snapshot, and `wait_ms` for long-poll.
`fork_edge` lineage arrives as an item with `object_type = "fork_edge"`. Records
appear only when the server runs with `labs` enabled. It has durable cursors on
persistent runtimes, backfill support, and tombstone envelopes; resume
durability is held client-side by the exporter.

## Cursor Contract

Persistent cursors are opaque. New rows use
`ledger:<tenant>/v2/<20-digit-sequence>/<kind>/...`; the durable sequence is
allocated per tenant at the serialized KV commit boundary. It therefore orders
that tenant's committed mutations without relying on wall-clock timestamps.
Legacy retained timestamp-based cursors remain resumable and order before v2
cursors.

For `/changes` and backfill responses:

- `next_cursor` is the last item returned in the current page, or null for an
  empty page. Resume from it via `from_cursor` and persist it in your
  client-side checkpoint.
- `high_watermark` is the ledger head captured before that request and can be
  ahead of `next_cursor` when `limit` truncates the page.
- `snapshot_high_watermark` is the durable ledger boundary paired with a
  metadata backfill. Capture it from the first page and send the same value on
  every continued page before switching to tail.

Do not invent a cursor or treat the high watermark as though every preceding
row was already delivered. `/changes` resumes only from a real retained ledger
item emitted for the requested tenant, scope, and include filter. When paging a
metadata snapshot (`backfill=true` or `exports/backfill`), wait for
`complete=true`, then tail `/changes` from `from_cursor =
snapshot_high_watermark`. AgentDB validates the snapshot tenant when present and
the retained ledger boundary; a fabricated cursor is rejected.

These durable export endpoints currently require a single-tablet runtime.
Metadata-backed cluster routing fails them closed with HTTP 422 because a
cross-tablet merge frontier is not implemented; do not run a sink against one
tablet owner and treat it as a tenant-wide feed.

## Inspect Export Capability

Check one tenant:

```bash
agentdbctl export clickhouse status --tenant-id ten_...
```

Inspect per-tenant export state on the node:

```bash
agentdbctl raw get /api/admin/exports
```

`/api/admin/exports` returns per-tenant
`{ tenant_id, high_watermark, oldest_available_cursor }`. The `status` output
shows export capability, high watermark, and oldest retained cursor.

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

## Resume And Availability

Resume durability is client-side. The server keeps no per-consumer state,
leases, or acks. The exporter persists the last `next_cursor` it processed and
seeds `from_cursor` from it on start:

- ClickHouse: an `agentdb_export_checkpoints` row keyed by
  `(consumer, tenant_id)`, upserted with `next_cursor` after each committed
  batch and read back on start.
- JSONL: a local `<output>.checkpoint` file, or an explicit `--checkpoint
  <path>`.

`--consumer <name>` is the client-side checkpoint key, not a server-side
consumer registration. The connector loop is:

- read a batch from `/changes`
- write the batch to the sink
- advance the checkpoint only after the sink accepts the batch
- resume from the persisted checkpoint after restart

Run exactly one exporter per `(consumer, tenant)`. Two exporters sharing the
same checkpoint key would race the checkpoint and produce duplicate or skipped
writes. For availability, supervise a single exporter (systemd, or a Kubernetes
Deployment with `replicas: 1`); a restart resumes from the persisted checkpoint.
There is no server-side lease HA.

Inspect export state:

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
- rely on a supervised single exporter for availability; do not run a second
  exporter on the same `(consumer, tenant)` checkpoint key

If a cursor is invalid:

- read the `oldest_available_cursor` from status or the error response
- verify that the cursor came from this tenant and this scope and include
  filter; fabricated or cross-scope cursors are rejected
- reset the client-side checkpoint to a valid cursor only when duplicate sink
  writes are acceptable or deduplicated
- run a fresh backfill when the required cursor is no longer retained

If ClickHouse contains duplicate-looking rows:

- confirm connector deduplication tokens are stable
- confirm the sink tables/views query the latest envelope by cursor/object
- check whether an intentional checkpoint reset replayed already-exported data

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

- confirm exporters have checkpointed beyond the relevant cursor
- confirm the sink has its own retention and backup policy
- preserve enough JSONL/Parquet history for the deployment's audit needs
