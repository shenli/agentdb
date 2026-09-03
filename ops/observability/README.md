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
cursor. Use `status --consumer` for AgentDB-side lag and the checkpoint cursor.

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

## Expected Performance Model

The exporter is outside the hot append path. Runtime writes commit to AgentDB
first; export workers later read the changefeed, transform the envelopes, write
the sink, and advance the client-side checkpoint cursor.

```text
throughput = min(source_read,
                 transform_cpu,
                 sink_insert / sink_rows_per_envelope,
                 network_bytes / bytes_per_envelope)

latency = ledger_visible
          + poll_or_batch_wait
          + transform_time
          + ClickHouse_insert_time
          + ack_time
```

For ClickHouse:

- `native` writes roughly two sink rows per event envelope: raw envelope plus
  flattened event.
- `native,otel` writes roughly four sink rows per event envelope: raw envelope,
  flattened event, OTel log, and OTel trace.
- If ClickHouse insert throughput is the bottleneck, `native,otel` envelope
  throughput should be about half of `native`.

Expected single-node shape:

| Scenario | Expected Throughput | Expected Export Freshness |
|---|---:|---:|
| Local/same-AZ ClickHouse, small payloads, `native` | low thousands to low tens of thousands envelopes/s | p95 around 1s to 2s |
| Local/same-AZ ClickHouse, small payloads, `native,otel` | about half to two-thirds of `native` | p95 around 1s to 3s |
| Remote ClickHouse over normal network | hundreds to several thousand envelopes/s | p95 around 1s to 5s |
| Large payloads or heavy redaction/hash | CPU/network-bound | seconds possible |

Cluster throughput scales by tenant and consumer parallelism, not by making one
consumer infinitely fast. One tenant/consumer remains ordered and checkpointed
serially. Many tenants can be exported by multiple workers across nodes or
cells until ClickHouse ingest, network, or exporter CPU becomes the shared
bottleneck.

```text
cluster throughput ~= sum(active tenant consumers) * per-consumer throughput
```

Recommended initial SLO targets:

| Metric | Initial Target |
|---|---:|
| Export freshness, same-AZ sink | p95 under 2s |
| Export freshness, remote sink | p95 under 5s |
| Failover RTO | `restart_time + poll_interval_ms + insert_time` |
| RPO | zero for committed events retained in the export ledger |

With a single supervised exporter, failover RTO is the process restart time
plus one poll interval plus the active insert time; on restart the exporter
resumes from its client-side checkpoint, so no committed rows are skipped or
double-inserted beyond the in-flight batch.

Future real-world tests should cover persistent AgentDB, AWS ClickHouse or
ClickHouse Cloud, 1/10/100 tenants, many sessions per tenant, payload sizes of
1 KiB, 10 KiB, and 100 KiB, batch sizes of 100/1000/5000, `native` vs
`native,otel`, worker failover, and throttled ClickHouse inserts. Reports should
include envelopes/s, sink rows/s, p50/p95/p99 batch latency, export freshness,
lag recovery time, retries, CPU, memory, and network throughput.

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
`/api/tenants/{tenant_id}/changes`. Persistent AgentDB runtimes return
durable `ledger:...` cursors; in-memory/dev runtimes return `v2:...` event
watch cursors. It is suitable for local validation of schema, ClickHouse
reachability, cursor resume, metadata snapshot backfill, tombstone handling,
checkpoint persistence, JSONL archival, and dashboard queries.

`--mode otel` writes event-derived rows into `agentdb_otel_logs` and
`agentdb_otel_traces` only. `--mode native,otel` writes the native raw/event
tables plus those projections. Trace IDs are deterministic from
`tenant_id/session_id`, span IDs are deterministic from event IDs, and
`source_refs` are preserved in trace `links_json`.

ClickStack itself can be run as the full ClickHouse-provided stack or as
HyperDX against an existing ClickHouse database. The AgentDB path is deliberately
direct-to-ClickHouse: AgentDB exports the rows, and HyperDX reads the
ClickStack-compatible views. This keeps AgentDB appends independent from the
OpenTelemetry Collector and preserves the exporter's checkpoint boundary.

## Resume And Availability

The exporter reads the tenant changefeed (`GET /api/tenants/{t}/changes`) and
holds its own resume cursor: after each written batch it upserts the batch's
`next_cursor` into the `agentdb_export_checkpoints` ClickHouse table, keyed by
`(consumer, tenant_id)`. On restart it seeds from that checkpoint, so a crash or
redeploy resumes exactly where it left off — no server-side consumer or lease.

```text
                 consumer name = checkpoint key: prod-clickhouse

  exporter  ---- GET /changes?from_cursor=... ----> AgentDB (stateless changefeed)
      |            (resume cursor from the checkpoint table)
      | insert batch, then upsert checkpoint
      v
  ClickHouse: <data tables> + agentdb_export_checkpoints(consumer, tenant_id, cursor)
```

Recommended production shape:

- Run **one exporter per `(consumer, tenant)`**. Two exporters sharing a
  consumer name and tenant would race the same checkpoint row (last write wins)
  and double-insert; partition by tenant or by consumer name instead.
- For failover, run a single active instance under a supervisor (systemd,
  Kubernetes Deployment with `replicas: 1`); on restart it resumes from the
  checkpoint. RTO is restart time plus one poll interval.
- Use `--max-retries` and `--retry-initial-backoff-ms` to absorb transient
  ClickHouse failures. Retries reuse a stable per-table batch id derived from
  the exported cursor range.
- Alert on a growing gap between the checkpoint cursor and the changefeed
  `high_watermark` (visible via `status`), and on export error rate.
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
`AGENTDB_EXPORT_CONSUMER`, and `CLICKHOUSE_URL` are deployment-specific.

Stop ClickHouse:

```bash
docker compose -f ops/observability/clickhouse-compose.yml down
```
