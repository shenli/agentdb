# AgentDB Observability Export Design

Status: draft

Date: 2026-05-14

## Purpose

AgentDB should not become a full observability warehouse in the hot runtime
path. It should make observability systems easy to build by exporting durable
runtime truth into systems that are optimized for search, analytics, and
dashboards.

ClickHouse is the first-class sink. It is not the only sink. S3/Parquet,
OpenTelemetry Collector, Kafka, lakehouse, warehouse, and customer-specific
pipelines should all use the same export envelope, cursor, backfill, and
consumer contracts. The design goal is that `AgentDB -> ClickHouse` feels like
the best-supported path, while the underlying export layer stays sink-neutral.

```text
AgentDB runtime log
  -> durable export ledger, backfill, watch, and managed consumer cursors
      -> ClickHouse native AgentDB schema
      -> optional OpenTelemetry / ClickStack projection
      -> S3/Parquet archive or lakehouse table
      -> Kafka / queue bridge
      -> OpenTelemetry Collector bridge
      -> customer-specific pipelines
```

AgentDB remains responsible for ordered session state, append-and-fetch,
replay, fork, context, artifacts, records, policy, and lifecycle. Downstream
sinks are responsible for broad cross-session filtering, high-cardinality
search, aggregations, dashboards, alerting, retention-friendly analytics, data
lake retention, or integration-specific delivery.

## Design Review

The initial design had the right boundary: export runtime truth into analytical
sinks instead of embedding OLAP inside AgentDB. It was too generic at the UX
layer and not explicit enough about future sinks. If ClickHouse is the default
observability backend for AI systems, AgentDB should ship an opinionated
ClickHouse integration with great defaults, explicit failure modes, and a
polished operator/agent experience. The core export protocol must remain
portable so future sinks do not require new AgentDB runtime semantics.

The highest-risk gaps were:

- No concrete "golden path" from a running AgentDB server to visible ClickHouse
  rows.
- No clear ClickHouse schema, engine choice, migration story, or retry/dedup
  contract.
- No managed consumer cursor model. Requiring every connector to invent
  checkpointing creates avoidable data-loss and duplicate risks.
- No OpenTelemetry / ClickStack compatibility plan. Native AgentDB schema is
  necessary, but many observability tools expect OTel-shaped logs/traces.
- No connector capability model for future sinks. Without that, every sink would
  invent its own semantics for backfill, acks, tombstones, and lag.
- No UX/AX bar. A human or coding agent should be able to configure, verify,
  backfill, tail, and debug the integration without reverse engineering docs.

This revision makes ClickHouse the first product path and defines a generic sink
contract below it.

## Current State

AgentDB already has the beginning of this surface:

- `GET /api/tenants/{tenant_id}/watch` returns `ChangefeedItem` values.
- Watch supports tenant, agent, and session scope.
- Watch uses a resume cursor and a result limit.
- `subscribe` and `replay` cover session-local event recovery.
- Records already have a durable outbox-like path for async global indexing.
- Architecture docs already treat watch/changefeed cursors as a dashboard and
  supervisor primitive.

The current watch implementation is not yet enough as a general replication
contract:

- It primarily emits session events, not every runtime mutation.
- It reconstructs results from live session events instead of reading a
  dedicated durable changefeed ledger.
- The cursor is derived from event timestamp, session id, and sequence. That is
  useful but not a complete placement- and restore-safe export cursor.
- It does not define retention, expired-cursor handling, sink checkpointing, or
  backfill behavior as explicit product contracts.
- It does not provide projection-friendly schemas for ClickHouse and other
  analytical sinks.

## Goals

1. Make AgentDB data easy to replicate into ClickHouse with one documented
   command path.
2. Keep ordinary session appends fast and predictable.
3. Provide replayable, idempotent, scoped export with stable cursors.
4. Preserve enough structure for trace, dashboard, cost, error, and search
   workloads without forcing an OLAP query engine into AgentDB.
5. Make sink implementations boring: no reverse engineering of AgentDB internals
   and no best-effort scraping of replay endpoints.
6. Make the integration agent-friendly: structured `doctor`, `init`, `status`,
   and `repair` output that coding agents can parse and act on.
7. Support future sinks through a stable protocol and connector capability
   levels rather than ClickHouse-specific runtime APIs.

## Non-Goals

- Build a general SQL/OLAP query engine inside AgentDB.
- Make AgentDB the primary system for arbitrary log analytics.
- Synchronously update every downstream observability index during append.
- Stream large artifact blobs through the changefeed.
- Guarantee exactly-once delivery to external systems. The contract should be
  at-least-once with idempotent sink writes.
- Require Kafka for the default path. Kafka can be an advanced deployment mode
  and future sink, but the best first experience should be direct
  AgentDB-to-ClickHouse export.

## ClickHouse Golden Path

The best-in-class integration should optimize for time to first useful query:

```bash
agentdbctl export clickhouse doctor \
  --agentdb-url http://127.0.0.1:7878 \
  --clickhouse-url https://user:pass@host:8443/agentdb

agentdbctl export clickhouse init \
  --clickhouse-url https://user:pass@host:8443/agentdb \
  --apply

agentdb-export-clickhouse run \
  --agentdb-url http://127.0.0.1:7878 \
  --tenant ten_... \
  --clickhouse-url https://user:pass@host:8443/agentdb \
  --consumer devroom-prod \
  --backfill \
  --tail
```

The first successful run should produce:

- visible rows in `agentdb_events`
- a saved consumer cursor
- a health report with rows exported, lag, and last cursor
- copyable sample queries
- optional ClickStack/HyperDX and Grafana setup snippets

The connector should support two schema modes:

| Mode | Purpose |
|---|---|
| `native` | Lossless AgentDB runtime data with flattened columns for common queries. |
| `otel` | Optional OpenTelemetry / ClickStack-compatible logs and spans derived from AgentDB events. |

The default local/dev command can create schema automatically. Production should
prefer an explicit `init --apply` or migration step; long-running exporters
should not race to create or mutate tables.

## UX And AX Requirements

Here, AX means agent experience as well as operator automation. The integration
should be easy for humans and reliable for coding agents.

### Human UX

- One-page quickstart for ClickHouse Cloud, local ClickHouse, and ClickStack.
- `doctor` verifies AgentDB reachability, ClickHouse version, credentials,
  database existence, required grants, schema version, insert settings, and a
  write/read smoke test.
- `init --dry-run` prints exact DDL and required grants before applying.
- `status` shows backfill progress, tail lag, rows per second, duplicate
  batches, failed batches, oldest retained cursor, and last error.
- Error messages name the broken prerequisite and the next command to run.
- Sample queries are printed after setup: recent failures, token cost by model,
  slow tools, sessions with pending approvals, and top error kinds.

### Agent Experience

- Every command supports `--output json`.
- `doctor --output json` returns stable machine-readable check IDs:
  `agentdb_reachable`, `clickhouse_reachable`, `schema_current`,
  `can_insert`, `can_query`, `consumer_cursor_valid`, `lag_within_slo`.
- Errors carry stable codes and remediation text.
- The connector emits structured logs and optional Prometheus metrics.
- The docs include a minimal "AI coding agent playbook" with exact commands,
  expected output fields, and safe retry rules.

### Product Bar

- Local time to first ClickHouse row: under 5 minutes.
- No custom SQL required for the default path.
- Backfill can be stopped and resumed without manual cleanup.
- Re-running `init` is idempotent.
- Re-running `run --backfill --tail` does not duplicate query-facing rows.

## Sink Model

ClickHouse gets the best UX first, but AgentDB should define sink-neutral
capabilities so future connectors are compatible by construction.

### Sink Capability Levels

| Level | Capability | Examples |
|---|---|---|
| L0 | Stateless polling from `exports/watch`; sink owns checkpointing. | Scripts, prototypes, local JSONL. |
| L1 | Managed consumer with claim/ack/rewind and lag reporting. | Official ClickHouse and S3 connectors. |
| L2 | Backfill plus tail with snapshot high-watermark handoff. | ClickHouse, S3/Parquet, Kafka bootstrap. |
| L3 | Sink-specific schema/init/doctor/status UX. | ClickHouse DDL/migrations, ClickStack projection. |
| L4 | Bidirectional operational integration. | Future alerts, replay links, or UI deep links back into AgentDB. |

The export envelope, cursor, tombstone, and backfill contracts are common to all
levels. Only schema creation, insert mechanics, and query UX are sink-specific.

### Future Sink Targets

The design should leave room for:

- `agentdb-export-s3` for JSONL/Parquet archives and lakehouse ingestion.
- `agentdb-export-kafka` for teams that already standardize on stream
  processors before ClickHouse or warehouses.
- `agentdb-export-otel` for OpenTelemetry Collector pipelines.
- `agentdb-export-bigquery` / `agentdb-export-snowflake` for warehouse
  analytics.
- embedded/local export for development and tests.

AgentDB should not add a bespoke runtime API for each target. If a sink cannot
be implemented through the export envelope, backfill, watch, and managed
consumer APIs, the export contract is incomplete.

## Required Additions

### 1. Durable Export Ledger

Add a durable, ordered export ledger per tenant or tablet. This ledger is the
replication source for external sinks.

It should include changes for:

- tenant, agent, and session metadata creation/update/delete
- event append
- artifact metadata creation/update/delete
- record write/delete
- skill registration/binding changes
- session lifecycle transitions
- policy changes that affect retention or observability interpretation
- tombstones for deletes and retention-driven purges

The export ledger may be stored as tablet-local changefeed rows and merged by
tenant-level readers. The important contract is that a sink can resume from a
cursor without needing to scan all session event keys.

Export ledger writes happen after the runtime mutation is committed and locally
visible. External sink failure must not block ordinary AgentDB appends.

### 2. Managed Export Consumers

Add optional named export consumers. This is not strictly required for generic
changefeeds, but it is the right default for a polished ClickHouse integration.

Consumer state should include:

```text
tenant_id
consumer_name
scope
include object types
last_acked_cursor
snapshot_high_watermark
lease_owner
lease_expires_at_ms
created_at_ms
updated_at_ms
```

APIs:

```text
POST /api/tenants/{tenant_id}/exports/consumers
GET  /api/tenants/{tenant_id}/exports/consumers/{name}
POST /api/tenants/{tenant_id}/exports/consumers/{name}/claim
POST /api/tenants/{tenant_id}/exports/consumers/{name}/ack
POST /api/tenants/{tenant_id}/exports/consumers/{name}/rewind
```

Sinks can still store their own checkpoints. The managed consumer path gives
the official connector a safe default and gives operators one place to inspect
export lag.

### 3. Stable Export Cursor

The current watch cursor should evolve into an opaque cursor with enough
information to survive tablet movement, restore, split, and merge.

The internal cursor can encode:

```text
version
tenant_id or tenant shard
placement scope / cell
tablet_id
tablet_epoch
applied_index
change_seq_within_applied_index
```

Clients must treat the cursor as opaque. AgentDB can expose decoded cursor
diagnostics in admin endpoints, but SDKs and sinks should persist and replay the
opaque string.

Expired cursors should return a structured error with:

- `oldest_available_cursor`
- `requested_cursor`
- `reason`
- remediation: rerun backfill from a snapshot high watermark

### 4. Backfill API

Tail-only changefeed is insufficient. New sinks need to bootstrap from existing
data.

Add an export backfill API that can page through an ordered snapshot:

```text
POST /api/tenants/{tenant_id}/exports/backfill
{
  "scope": "tenant" | "agent" | "session",
  "agent_id": "...",
  "session_id": "...",
  "include": ["sessions", "events", "artifacts", "records", "skills", "policy"],
  "from_cursor": "...",
  "limit": 1000
}
```

The response should include:

```json
{
  "items": [],
  "next_cursor": "...",
  "snapshot_high_watermark": "...",
  "complete": false
}
```

The `snapshot_high_watermark` lets a sink backfill up to a point and then switch
to tailing watch from that same boundary without gaps.

### 5. Export Envelope

Do not export only raw `EventEnvelope`. Wrap every mutation in a stable export
envelope:

```json
{
  "export_version": "2026-05-14",
  "cursor": "...",
  "op": "insert",
  "object_type": "event",
  "tenant_id": "ten_...",
  "agent_id": "agt_...",
  "session_id": "ses_...",
  "object_id": "evt_...",
  "seq": 42,
  "created_at_ms": 1778780000000,
  "observed_at_ms": 1778780000001,
  "data": {},
  "tombstone": null
}
```

Allowed `op` values:

```text
insert
upsert
delete
snapshot
```

Allowed `object_type` values:

```text
tenant
agent
session
event
artifact
record
skill
skill_binding
policy
consumer
```

For `object_type = "event"`, `data` should include the canonical
`EventEnvelope`.

For deletes, `data` may be omitted and `tombstone` should include the reason:

```json
{
  "reason": "user_delete",
  "deleted_at_ms": 1778780000000
}
```

Allowed tombstone reasons:

```text
soft_delete
user_delete
retention_gc
tenant_purge
policy_redaction
```

### 6. Normalized Observability Columns

Keep arbitrary payloads, but also extract common fields into predictable export
columns so sinks can build efficient schemas.

For events, export normalized fields when present:

- `event_id`
- `kind`
- `seq`
- `session_id`
- `agent_id`
- `created_at_ms`
- `parent_event_id`
- `source_refs`
- `status`
- `model`
- `provider`
- `finish_reason`
- `tool_name`
- `tool_call_id`
- `duration_ms`
- `timeout_ms`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- `cost_micros`
- `error_kind`
- `error_message`
- `retryable`
- `exit_code`
- `approval_id`
- `approval_decision`
- `context_pack_id`
- `artifact_refs`
- `tags`
- `metadata`

These fields should be derived from the event convention payloads. Unknown or
adapter-specific payload fields remain in JSON.

### 7. ClickHouse Native Schema

Publish and maintain first-party DDL. The schema should be queryable directly
and usable as a stable substrate for ClickStack, HyperDX, Grafana, notebooks,
and customer SQL.

Default tables:

```text
agentdb_export_items_raw
agentdb_sessions
agentdb_events
agentdb_event_refs
agentdb_artifacts
agentdb_records
agentdb_skills
agentdb_metrics_1m
agentdb_tombstones
agentdb_export_checkpoints
agentdb_schema_migrations
```

Recommended table strategy:

| Table | Engine | Purpose |
|---|---|---|
| `agentdb_export_items_raw` | `MergeTree` / `ReplicatedMergeTree` | Lossless envelope archive for replay/debug. |
| `agentdb_events` | `MergeTree` / `ReplicatedMergeTree` | Flattened immutable event rows. |
| `agentdb_event_refs` | `MergeTree` / `ReplicatedMergeTree` | Source refs, parent/child edges, trace links. |
| `agentdb_sessions` | `ReplacingMergeTree(updated_at_ms)` | Latest session metadata. |
| `agentdb_artifacts` | `ReplacingMergeTree(updated_at_ms)` | Artifact metadata, not blobs. |
| `agentdb_records` | `ReplacingMergeTree(updated_at_ms)` | Searchable records and metadata. |
| `agentdb_metrics_1m` | `AggregatingMergeTree` | Cost, token, latency, and error rollups. |
| `agentdb_tombstones` | `MergeTree` | Delete/redaction/retention actions. |

Default event ordering:

```sql
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, created_at, session_id, seq, event_id)
```

Use daily partitions for very high-volume tenants or strict short retention.
Use monthly partitions for simpler operations and moderate volume.

Payload storage:

- Store the full payload as `payload_json String` in the default schema.
- Optionally add `payload JSON` for ClickHouse versions where the JSON type is
  production-ready and the operator wants dynamic path queries.
- Promote common paths to real columns instead of relying on dynamic JSON for
  hot filters.

Retry and dedup:

- The connector batches rows and uses stable `batch_id` values.
- ClickHouse inserts should use retry-safe settings such as
  `insert_deduplicate=1` and, when available, `insert_deduplication_token`.
- Query-facing rows should also carry stable keys: `event_id` for events and
  `(object_type, object_id)` for mutable metadata.
- Metadata/latest tables use `ReplacingMergeTree(updated_at_ms)` so repeated
  updates converge.
- The connector should ack the AgentDB consumer cursor only after ClickHouse
  confirms the batch.

Do not rely on ClickHouse alone to provide exactly-once semantics. The product
contract is at-least-once export plus idempotent inserts, deduplication tokens,
stable row keys, and safe replays.

### 8. OpenTelemetry And ClickStack Projection

Native AgentDB schema is the source of truth. Add optional OTel-shaped
projections so teams can use ClickStack/HyperDX and other OTel-first tools
without a custom UI.

Projection rules:

- Every AgentDB event can become an OTel log-like wide event with a timestamp,
  body, severity/status, resource attributes, and event attributes.
- Sessions map to traces by deriving a deterministic OTel `TraceId` from
  `(tenant_id, session_id)`.
- Tool calls, model calls, approvals, and context-building steps can become
  spans when the event pair supplies enough start/end or duration data.
- `source_refs` become span links or edge rows. Do not drop the original
  AgentDB refs.
- Original AgentDB IDs must remain available as attributes:
  `agentdb.tenant_id`, `agentdb.agent_id`, `agentdb.session_id`,
  `agentdb.event_id`, `agentdb.seq`.

Default output tables can be either:

```text
agentdb_otel_logs
agentdb_otel_traces
```

or ClickStack-compatible table names selected by the operator. The connector
should offer `--mode native`, `--mode otel`, and `--mode native,otel`.

### 9. Artifact Export Contract

Changefeed items should carry artifact metadata, not artifact bytes.

Artifact export metadata should include:

- `artifact_id`
- `tenant_id`
- `session_id`
- `content_type`
- `bytes`
- `sha256`
- `storage_class`
- `content_uri` or a retrievable AgentDB blob route
- `summary`
- retention/lifecycle metadata

Sinks can decide whether to copy blobs, index only summaries, or late-load bytes
from AgentDB/object storage when a user opens a detail view.

### 10. Reference Connectors

Ship reference connectors after the export envelope is stable. ClickHouse is
the first production-quality connector; other sinks should reuse the same
connector framework and capability model.

1. `agentdb-export-clickhouse`
   - backfills a tenant
   - tails watch
   - writes idempotently into ClickHouse
   - initializes and migrates schema
   - supports native and OTel projection modes
   - exposes lag, duplicate, retry, rows/sec, and batch-size counters

2. `agentdb-export-s3`
   - writes JSONL first
   - later writes Parquet
   - rotates files by tenant, object type, and date

3. Local demo
   - exports to local files
   - queries with DuckDB
   - proves the schema without requiring infrastructure

Future connectors:

- `agentdb-export-kafka` for stream processing and custom fan-out.
- `agentdb-export-otel` for OpenTelemetry Collector ingestion.
- `agentdb-export-bigquery` and `agentdb-export-snowflake` for warehouse
  users.
- Iceberg/Delta lakehouse writers once the Parquet layout is stable.

## ClickHouse Connector Details

### Configuration

Support both CLI flags and config-file operation:

```toml
[exports.clickhouse.default]
enabled = true
tenant_id = "ten_..."
consumer = "prod-clickhouse"
agentdb_url = "http://127.0.0.1:7878"
clickhouse_url = "https://user:pass@host:8443/agentdb"
mode = ["native", "otel"]
batch_rows = 10000
batch_bytes = 10485760
flush_interval_ms = 1000
create_schema = false
```

Secrets should be accepted from environment variables or a secret manager, not
only from config files.

### Insert Path

The default connector should use ClickHouse HTTP with `JSONEachRow` because it
is simple, scriptable, and available everywhere. The native driver can be an
optimization later.

Default insert behavior:

- batch rows by count, bytes, and time
- use `async_insert=1` with `wait_for_async_insert=1` when configured
- set a stable deduplication token per AgentDB export batch
- retry failed batches with the same batch id and ClickHouse settings
- ack AgentDB only after ClickHouse returns success

This gives low operational friction while preserving a clear durability
boundary.

### Schema Migrations

The connector owns additive schema migrations:

```bash
agentdbctl export clickhouse migrate --to latest --apply
```

Rules:

- migrations are idempotent
- migrations are versioned in `agentdb_schema_migrations`
- long-running exporters refuse to start if the schema is too old
- backward-compatible additive columns are preferred
- destructive migrations require an explicit `--allow-destructive` flag

### Sample Queries

The connector should install or print query snippets:

```sql
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
```

## Delivery Semantics

The export contract should be:

- **At-least-once delivery.** Sinks must deduplicate by stable object keys.
- **Session-local event order.** Events for one session must preserve `seq`.
- **Tenant-scoped replay.** Tenant exports can be replayed from a saved cursor.
- **No gap on normal resume.** If the cursor is still retained, resume returns
  the next item after the cursor.
- **Explicit gap on expired cursor.** AgentDB returns an error, not silent data
  loss.
- **Tombstones for deletes.** Sinks receive delete/purge intent so dashboards can
  honor retention and privacy policy.
- **Ack after sink durability.** Official connectors ack managed consumers only
  after the sink has durably accepted the batch.

Exactly-once effects are a sink responsibility implemented with idempotent
insert keys, deduplication tokens, replacing/merging tables, and replay-safe
consumer cursors.

## Runtime Isolation

Export rows should be produced after the runtime mutation is committed and
locally visible. External sink failure must not block normal appends.

Backpressure rules:

- If the export ledger grows too large, report lag and apply policy-based
  retention or backpressure.
- Do not synchronously wait for ClickHouse, S3, or any external sink in the
  append path.
- Admin status should expose oldest retained cursor, export lag, dropped cursor
  count, and ledger bytes by tenant/tablet.

## Security And Privacy

Observability export can leak sensitive prompts, outputs, tool inputs, and
artifact metadata. The export contract needs policy hooks before teams can use
it in production.

Required controls:

- tenant-scoped export authorization
- implemented in `agentdbctl`: object-type allow/deny lists
- implemented in `agentdbctl`: event-kind allow/deny lists
- implemented in `agentdbctl`: payload field redaction rules
- artifact metadata export without blob export by default
- implemented in `agentdbctl`: payload hashing or payload drop mode
- tombstones for policy redaction and retention purge
- ClickHouse grants with least privilege: create/migrate role separate from
  insert-only connector role

The connector should never print secrets in logs or `doctor` output. Current
exporter-side controls are a sink-boundary safeguard; policy-driven server-side
redaction remains future work once tenant policy mutation APIs are promoted.

## API Sketch

Existing:

```text
GET /api/tenants/{tenant_id}/watch
```

Keep it for simple consumers. Add export-specific APIs:

```text
GET  /api/tenants/{tenant_id}/exports/watch
POST /api/tenants/{tenant_id}/exports/backfill
GET  /api/tenants/{tenant_id}/exports/status
POST /api/tenants/{tenant_id}/exports/consumers
GET  /api/tenants/{tenant_id}/exports/consumers/{name}
POST /api/tenants/{tenant_id}/exports/consumers/{name}/claim
POST /api/tenants/{tenant_id}/exports/consumers/{name}/ack
POST /api/tenants/{tenant_id}/exports/consumers/{name}/rewind
GET  /api/admin/exports
```

`exports/watch` differs from current `watch` by returning export envelopes for
all requested object types, not only session events.

Example:

```text
GET /api/tenants/ten_x/exports/watch?scope=tenant&from_cursor=...&limit=1000
```

Response:

```json
{
  "items": [],
  "next_cursor": "...",
  "oldest_available_cursor": "...",
  "high_watermark": "..."
}
```

## Implementation Plan

### Phase 1: ClickHouse Golden Path And Durable Export Ledger

- Implemented: `ExportEnvelope` DTOs for events and runtime metadata objects.
- Implemented: `GET /api/tenants/{tenant_id}/exports/watch` returning export
  envelopes. Persistent runtimes serve this from a durable export ledger;
  in-memory/dev runtimes fall back to current event watch logic.
- Implemented: `GET /api/tenants/{tenant_id}/exports/status` exposing the
  durable ledger capability and supported object types.
- Implemented: `POST /api/tenants/{tenant_id}/exports/backfill` with event
  snapshots plus tenant-scope metadata snapshots for tenants, agents, sessions,
  artifacts, records, skills, and skill bindings.
- Implemented: explicit persisted cursor validation and
  `oldest_available_cursor` reporting for export watch, backfill, and status.
- Implemented: managed export consumers with durable checkpoints, include
  filters, leases, ack, and rewind.
- Implemented: per-tenant durable export high-watermark pointers for
  persistent runtimes, so `exports/status` does not scan the full ledger.
- Implemented: durable session and agent export ledger indexes for scoped
  event and metadata consumers. The public cursor remains the canonical tenant
  `ledger:...` cursor.
- Implemented: tenant, agent, session, artifact, record, skill, and
  skill-binding mutation ledger rows.
- Implemented: soft-delete tombstone export for metadata objects that use the
  runtime soft-delete contract.
- Implemented: retention-GC and tenant-purge tombstone export for physical GC
  removal of events and runtime metadata objects.
- Implemented: persisted export watch can drain retained ledger rows after a
  tenant has been physically purged, so final tenant-purge tombstones remain
  observable.
- Implemented: retention-aware cursor validation. A consumer resuming from a
  pruned durable-ledger cursor receives `InvalidCursor` with the current
  `oldest_available_cursor`.
- Implemented: tablet movement snapshot classification for export ledger,
  scoped ledger indexes, high-watermark rows, and managed consumer rows.
- Implemented: restore/conformance coverage proving tablet snapshot restore
  preserves export ledger high watermarks and managed consumer checkpoints.
- Implemented: `GET /api/admin/exports` visibility for managed consumers and
  current export capability, including pending item count, high watermark,
  oldest pending item timestamp, lag, and lease-active state.
- Implemented: aggregate admin export metrics for total pending items, max
  lag, oldest pending timestamp, lease counts, and oldest retained cursors.
- Implemented: normalized event observability fields for common convention
  payload keys.
- Implemented: `agentdbctl export clickhouse doctor/init/migrate/run/status`,
  including `run --backfill --tail`, using ClickHouse HTTP and
  `JSONEachRow`.
- Implemented: first-party ClickHouse DDL for raw envelopes, flattened events,
  checkpoints, schema migrations, and tombstones.
- Implemented: latest-schema stamp checks for ClickHouse schema drift. Because
  AgentDB has no production compatibility requirement yet, this is a
  current-schema apply/check path rather than a historical upgrade ladder.
- Implemented: retry-safe ClickHouse inserts with stable per-table
  deduplication tokens derived from the exported cursor range.
- Implemented: exporter-side privacy gates for ClickHouse and JSONL runs:
  object/event allow-deny filters, dot-path redaction, and payload
  `full`/`hash`/`drop` modes.
- Implemented: local exporter status output through `--metrics-output`,
  refreshed after every processed batch for tailing workers.
- Implemented: focused tests for cursor resume, route context propagation,
  durable ledger resume after restart, normalized fields, and CLI schema/path
  helpers, ClickStack config templates, privacy filtering/redaction, and
  exporter metrics.
- Implemented: systemd service/env packaging for the long-running
  `agentdbctl` exporter. A separate `agentdb-export-clickhouse` daemon binary
  remains optional because the packaged service uses the same consumer lease
  protocol.

This phase is a durable export API, metadata snapshot, schema, HA, and UX
commitment. It includes tenant-prefixed durable ledger rows for persisted
runtimes and tombstone envelopes for runtime soft deletes.

Current scale boundary: tenant export resumes use a KV lower bound derived from
`ledger:...` cursors, so normal polling does not scan from the start. Tenant
high watermark is stored separately and read directly. Agent and session scoped
exports use secondary index rows that point back to the canonical tenant ledger
entry, avoiding full tenant scans while preserving one sink checkpoint cursor
format.

HA behavior: run multiple connector workers with the same consumer name and
distinct lease owners. Only the active lease holder can ack. The connector
writes a batch to ClickHouse before acking AgentDB, so failover can replay the
last unacked batch without data loss. RPO is zero for committed AgentDB events
that remain in the export ledger. RTO is bounded by the configured export
consumer lease expiry plus the worker poll interval and ClickHouse insert time.

### Phase 2: Remaining Durable Ledger Coverage

- Add first-class policy object ledger rows when runtime policy mutation APIs
  are promoted.
- Add policy-redaction tombstone reasons once redaction APIs exist.

### Phase 3: Retention And Metrics

Implemented in the durable-ledger phase for the first-party admin/API surface.
Future work is Prometheus/OTel scraping once the broader metrics subsystem is
formalized.

### Phase 4: OTel / ClickStack Projection

- Implemented: optional `agentdb_otel_logs` and `agentdb_otel_traces`
  ClickHouse output from `agentdbctl export clickhouse run --mode otel` or
  `--mode native,otel`.
- Implemented: ClickStack/HyperDX-compatible views
  `agentdb_clickstack_logs` and `agentdb_clickstack_traces`, plus `otel_logs`
  and `otel_traces` aliases for dedicated AgentDB observability databases.
- Implemented: deterministic AgentDB ID to OTel trace/span ID mapping.
- Implemented: source-ref preservation as trace `links_json`.
- Implemented: local schema/projection smoke script for ClickHouse OTel and
  ClickStack view compatibility.
- Implemented: local ClickStack/HyperDX UI reachability smoke plus ClickHouse
  source-view compilation checks.
- Implemented: `agentdbctl export clickhouse clickstack-config` emits source
  and dashboard configuration templates for ClickStack Cloud/HyperDX setup.
- Remaining: live saved source/dashboard API writes once the ClickStack API is
  generally available enough to depend on in unattended deployments.

### Phase 5: Additional Sinks

- Implemented: JSONL sink in `agentdbctl export jsonl run`, including local
  files, stdout, and object-store URIs such as `s3://...`.
- Implemented: Parquet conversion command in
  `agentdbctl export parquet from-jsonl`, backed by DuckDB for local and S3
  JSONL archives.
- Implemented: DuckDB demo queries for JSONL archives in
  `ops/observability/duckdb-jsonl-demo.sql`.
- Implemented: starter native and ClickStack dashboard query bundle through
  `agentdbctl export clickhouse queries` and
  `ops/observability/clickhouse-dashboard-queries.sql`.
- Implemented: ClickHouse/AWS/systemd deployment hooks for schema apply,
  retries, payload minimization, and local exporter status files.
- Add Kafka bridge if customers need queue-based fan-out or stream processing.
- Add warehouse/lakehouse connectors only after the envelope and Parquet layout
  are stable.

## Testing Requirements

- Resume from every returned cursor without gaps or duplicates.
- Retry connector batches and verify idempotent sink state.
- Backfill then tail from `snapshot_high_watermark` without gaps.
- Delete and retention GC produce tombstones.
- Export lag metrics increase when a sink stops and recover when it resumes.
- Cursor expiry returns a structured error.
- Tenant/agent/session scopes never leak cross-tenant data.
- Restore and tablet movement preserve or rebuild export cursor state.
- `agentdbctl export clickhouse doctor --output json` remains stable.
- ClickHouse schema migrations are idempotent.
- ClickHouse connector crash after insert but before ack does not create
  duplicate query-facing rows after restart.

## Open Questions

- Should the first durable export ledger be tenant-local or tablet-local?
- Which normalized fields should become mandatory in the event conventions?
- Should the ClickHouse connector be built into `agentdbctl`, shipped as a
  separate binary, or both?
- What is the minimum retained changefeed window for hosted deployments?
- Should the OTel projection target ClickStack's exact default table names or
  AgentDB-prefixed tables with compatibility views?
- After ClickHouse and S3/Parquet, should Kafka or OpenTelemetry Collector be
  the next official sink?

## Recommendation

Build this in this order:

1. Ship the ClickHouse golden path on top of current watch.
2. Add managed consumers, durable ledger, and backfill.
3. Expand object coverage to artifacts, records, lifecycle, policy, and
   tombstones.
4. Add OTel/ClickStack projection after the native schema is stable.
5. Add S3/Parquet once the envelope and cursor contract are stable.
6. Add Kafka, OpenTelemetry Collector, and warehouse/lakehouse connectors based
   on customer demand, without changing runtime export semantics.

This gives observability systems a clean path without making AgentDB's runtime
write path pay the cost of analytical storage and query execution.

## External References

- ClickHouse ClickStack describes the modern ClickHouse observability path:
  OpenTelemetry-native wide events, HyperDX UI, schema-agnostic custom
  pipelines, and ClickHouse-backed search/dashboards.
  <https://clickhouse.com/clickstack>
- ClickHouse's OpenTelemetry guidance emphasizes the collector/exporter path and
  the reality that observability schemas must be optimized for the use case.
  <https://clickhouse.com/blog/clickhouse-and-open-telemtry>
- ClickHouse async inserts support HTTP/native ingestion and `wait_for_async_insert`
  controls whether success means the data has reached storage.
  <https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse>
- ClickHouse insert deduplication on retries documents the MergeTree
  deduplication window and `insert_deduplication_token`.
  <https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries>
- ClickHouse JSON type is useful for dynamic field queries, but has insert and
  whole-document read tradeoffs; the default AgentDB schema should promote hot
  fields to columns and keep full payload JSON available.
  <https://clickhouse.com/docs/sql-reference/data-types/newjson>
