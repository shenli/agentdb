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
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse migrate --apply
```

`init --apply` is kept as an alias for the same latest-schema apply path.
`migrate --apply` records the current AgentDB schema stamp in
`agentdb_schema_migrations`; `doctor` and `status` use that stamp plus required
table/view checks to report schema drift.

Check both sides:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse doctor --tenant-id ten_...
```

`doctor` validates AgentDB reachability, export status, ClickHouse reachability,
schema currency, query grants, and a bounded write/read/drop smoke. Use
`--skip-write-smoke` only for read-only inspection.

Run a one-shot export batch:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse
```

Tail continuously:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse --backfill --tail
```

Write both native AgentDB tables and OTel-shaped ClickStack-friendly
projections:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse \
    --mode native,otel --backfill --tail
```

Exporter-side privacy gates are available for ClickHouse and JSONL sinks. They
filter or redact export envelopes before the connector writes the external
system; they do not replace AgentDB tenant auth or future policy-driven
redaction.

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse \
    --mode native,otel --backfill --tail \
    --deny-event-kind debug,trace \
    --redact-field data.payload.api_key \
    --payload-mode hash \
    --metrics-output ./clickhouse-export-status.json
```

`--payload-mode full` preserves payloads, `hash` replaces
`data.payload` with a SHA-256 fingerprint, and `drop` writes a null payload.
`--metrics-output` is refreshed after each processed batch and includes rows,
filtered items, sink insert rows, retry attempts, rows per second, and the last
cursor. Use `status --consumer` for AgentDB-side lag and lease state.

## Local Exporter Baseline

A local benchmark run on a developer workstation with in-memory AgentDB, local
Docker ClickHouse, one tenant/session, and 10 batches of 1000 export envelopes
produced:

| Export Mode | Exported Envelopes | Sink Rows Inserted | Duration | Throughput | Batch Time |
|---|---:|---:|---:|---:|---:|
| `native` | 10,000 | 20,000 | 3.22s | 3,106 envelopes/s | 322ms per 1000-envelope batch |
| `native,otel` | 10,000 | 40,000 | 5.41s | 1,847 envelopes/s | 541ms per 1000-envelope batch |

The run used `agentdbctl export clickhouse run --backfill --limit 1000
--max-batches 10 --metrics-output ...`. Treat this as a local reproducibility
baseline. Real throughput is usually bounded by ClickHouse insert latency,
network round trips, projection mode, payload size, and batch sizing.

The schema also installs ClickStack/HyperDX-friendly views over those
projections:

| View | Purpose |
|---|---|
| `agentdb_clickstack_logs` | Wide log source with `Timestamp`, `TraceId`, `SpanId`, `ServiceName`, `Body`, `ResourceAttributes`, and `LogAttributes`. |
| `agentdb_clickstack_traces` | Wide trace source with `Timestamp`, `TraceId`, `SpanId`, `ParentSpanId`, `SpanName`, `Duration`, `ResourceAttributes`, and `SpanAttributes`. |
| `otel_logs` / `otel_traces` | Convenience aliases for tools that expect the default ClickStack table names in a dedicated AgentDB observability database. |

For HyperDX, create sources pointing at `agentdb_clickstack_logs` and
`agentdb_clickstack_traces`, or use the `otel_logs` / `otel_traces` aliases in
a dedicated AgentDB database. The important source expressions are:

```text
timestamp: TimestampTime
display timestamp: Timestamp
service name: ServiceName
body: Body
trace id: TraceId
span id: SpanId
log attributes: LogAttributes
span attributes: SpanAttributes
resource attributes: ResourceAttributes
duration: Duration
```

Print source and dashboard configuration templates for ClickStack Cloud or
HyperDX setup:

```bash
agentdbctl -o json export clickhouse clickstack-config \
  --service-name "AgentDB Prod"
```

Backfill metadata snapshots into the raw export table:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
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

Inspect JSONL archives with DuckDB:

```bash
duckdb -cmd ".param set jsonl_path './agentdb-events.jsonl'" \
  -init ops/observability/duckdb-jsonl-demo.sql

duckdb -cmd "INSTALL httpfs; LOAD httpfs;" \
  -cmd "SET s3_region='us-east-1';" \
  -cmd ".param set jsonl_path 's3://agentdb-exports/tenants/ten_.../events.jsonl'" \
  -init ops/observability/duckdb-jsonl-demo.sql
```

Convert JSONL archives to Parquet with DuckDB:

```bash
agentdbctl export parquet from-jsonl \
  --input ./agentdb-events.jsonl \
  --output ./agentdb-events.parquet

agentdbctl export parquet from-jsonl \
  --input s3://agentdb-exports/tenants/ten_.../events.jsonl \
  --output s3://agentdb-exports/tenants/ten_.../events.parquet \
  --s3-region us-east-1
```

Print copyable ClickHouse dashboard queries:

```bash
agentdbctl export clickhouse queries
agentdbctl -o json export clickhouse queries
```

The same queries are stored in
`ops/observability/clickhouse-dashboard-queries.sql`.

Run the local ClickHouse/OTel projection smoke:

```bash
docker compose -f ops/observability/clickhouse-compose.yml up -d
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
  ops/observability/smoke-clickhouse-otel.sh
```

Set `TENANT_ID=ten_... AGENTDB_EXPORT_SMOKE_RUN=1` to also copy one bounded
`native,otel` export batch from a running AgentDB server.

Run a local ClickStack/HyperDX UI smoke:

```bash
docker compose -f ops/observability/clickstack-compose.yml up -d
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/?database=agentdb \
HYPERDX_URL=http://127.0.0.1:8080 \
  ops/observability/smoke-clickstack-hyperdx.sh
```

The smoke checks that the HyperDX UI is reachable and that the AgentDB
ClickStack source views compile in ClickHouse. Create HyperDX sources using the
field names above.

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

ClickStack itself can be run as the full ClickHouse-provided stack or as
HyperDX against an existing ClickHouse database. The AgentDB path is deliberately
direct-to-ClickHouse: AgentDB exports the rows, and HyperDX reads the
ClickStack-compatible views. This keeps AgentDB appends independent from the
OpenTelemetry Collector and preserves the managed export-consumer ack boundary.

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
- Use `--max-retries` and `--retry-initial-backoff-ms` to absorb transient
  ClickHouse failures. Retries reuse a stable per-table batch id derived from
  the exported cursor range.
- Alert on increasing `lag_ms`, `pending_items`, expired leases, and stale
  `high_watermark`.
- Never ack before ClickHouse accepts the batch. The ClickHouse tables use
  stable ids so retrying an already inserted batch is safe.

For systemd deployments, install:

```bash
sudo install -D -m 0644 ops/observability/agentdb-export-clickhouse.env.example \
  /etc/agentdb/agentdb-export-clickhouse.env
sudo install -D -m 0644 ops/observability/agentdb-export-clickhouse.service \
  /etc/systemd/system/agentdb-export-clickhouse.service
sudo systemctl daemon-reload
sudo systemctl enable --now agentdb-export-clickhouse.service
```

Edit the env file first so `AGENTDB_EXPORT_TENANT_ID`,
`AGENTDB_EXPORT_LEASE_OWNER`, and `CLICKHOUSE_URL` are deployment-specific.

Stop ClickHouse:

```bash
docker compose -f ops/observability/clickhouse-compose.yml down
```
