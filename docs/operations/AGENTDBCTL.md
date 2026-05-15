# agentdbctl

`agentdbctl` is the recommended human and AI-agent interface for operating
AgentDB. It wraps the admin HTTP API with concise commands, table output for
humans, and JSON output for automation.

The REST API remains the stable machine contract. Operators should not need to
hand-write `curl` payloads for normal day-2 workflows.

## Configuration

Default target:

```bash
agentdbctl status
```

Custom target:

```bash
agentdbctl --url http://127.0.0.1:7878 status
```

Environment variables:

```bash
export AGENTDB_URL=http://127.0.0.1:7878
export AGENTDB_TOKEN=...
```

Use JSON output for scripts and AI agents:

```bash
agentdbctl -o json status
agentdbctl -o json nodes list
```

## Core Workflows

Cluster status:

```bash
agentdbctl status
agentdbctl nodes list
agentdbctl tablets list
agentdbctl raft status
```

`agentdbctl status` also prints the local tablet-store layout and sizing when
the server exposes it, including open domain count, grouped-domain memtable
budget, and total pending compaction bytes for open domains. Use this after
deploying AWS or Kubernetes nodes to confirm `grouped_rocksdb`,
`tablets_per_domain`, `max_open_domains`, and compaction pressure match the
intended instance memory budget.

Tenant placement:

```bash
agentdbctl cells list
agentdbctl tenant-policies list
agentdbctl tenant-policies upsert --tenant-id ten_123 \
  --cell-id cell-a --placement-scope-id tenant/acme \
  --tier paid --priority 100 --placement-class dedicated
agentdbctl tenant-policies delete ten_123
```

Drain and undrain a node:

```bash
agentdbctl drain start
agentdbctl nodes maintenance node-a alive
```

Move ownership:

```bash
agentdbctl tablets rebalance --dry-run
agentdbctl tablets rebalance --max-movements 10
agentdbctl tablets handoff 7 node-b
agentdbctl tablets complete-handoff 7 node-a
agentdbctl tablets prepare-replacement 7 node-b --source-node-id node-a
agentdbctl raft catch-up --tablet-id 7 --target-node-id node-b
agentdbctl tablets complete-replacement 7 node-b --source-node-id node-a --min-applied-index 1024 --retire-source
```

`rebalance` assigns unowned tablets directly. For owned tablets, it prepares a
replacement intent by marking the target as a learner and handoff destination;
the next rebalance pass can complete the ownership handoff after the target is
healthy and caught up. When the scheduler node has the local Raft group
registered, automatic completion also promotes the target to voter, removes the
source from Raft, and deletes the retired source replica row. If ownership
completion runs before the local Raft group is available, the old source
replica is left as `draining` and later rebalance passes retry Raft retirement.
Use `raft catch-up` to accelerate convergence and `complete-replacement` when
you need explicit operator-controlled promotion or source retirement.

Split/merge lifecycle:

```bash
agentdbctl tablets prepare-split 7 --child-range 0:49:8 --child-range 50:99:9
agentdbctl tablets movements --status pending
agentdbctl tablets complete-split 7 --child-range 0:49:8 --child-range 50:99:9 --map-epoch 2 --min-applied-index 1024
agentdbctl tablets prepare-merge --source-tablet-id 8 --source-tablet-id 9 10
agentdbctl tablets movements --status pending
agentdbctl tablets complete-merge --source-tablet-id 8 --source-tablet-id 9 10 --map-epoch 3 --min-applied-index 1024
```

Prepare starts only from `serving` source tablets and requires destination
tablets to be missing or already `creating` with no owned ranges in the same
placement scope. It then fences metadata, creates destination tablets, seeds
destination replica intent, and enqueues `tablet_movements` state-copy work. The
local tablet movement executor copies state for pending rows when this node
hosts a destination replica; it uses a healthy caught-up local source replica
when available and otherwise fetches the filtered source snapshot from a
healthy caught-up remote source replica over the internal authenticated movement
endpoint. Tablet replica metadata changes notify cluster workers, so new
split/merge destination replicas are opened as local Raft runtime groups by the
registry sync loop. The scheduler leader scans movement rows, marks them
completed once all destination serving replicas report healthy and caught up,
and then publishes the split/merge range-map cutover when the whole prepared
movement group is complete. After cutover, tombstoned source tablets are
ignored by runtime Raft registry sync and their replica rows are removed by the
scheduler cleanup pass. The explicit `complete-*` commands remain for
operator-controlled cutover or recovery. Use `--allow-unready` only for
emergency/manual recovery after separately proving state safety.

Automatic preparation is opt-in. Set
`AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_SHARDS` to prepare a two-way split for a
serving tablet whose contiguous shard span exceeds the threshold. Set
`AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_KEYS` or
`AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_STORAGE_BYTES` to split from locally
published tablet stats instead. `AGENTDB_TABLET_LIFECYCLE_SPLIT_MIN_WRITE_OPS_PER_SEC`
splits when the heartbeat-derived applied-index write rate reaches the
configured rate. Use
`AGENTDB_TABLET_LIFECYCLE_SPLIT_MIN_WRITE_OPS_PER_SEC_MILLI` when sub-1-op/sec
precision is needed. Set
`AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_COMPACTION_DEBT_OPS` to split when a
tablet's retained committed log entries exceed the threshold; this is the
current retained-log compaction-debt signal, not a full LSM compaction metric.
Set
`AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_SHARDS` to prepare a merge for adjacent
serving tablets whose individual spans are at or below the threshold.
`AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_KEYS` and
`AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_STORAGE_BYTES` add stat-based merge caps;
when those caps are configured, tablets without stats are skipped rather than
merged blindly.
`AGENTDB_TABLET_LIFECYCLE_MAX_ACTIONS` bounds prepared lifecycle groups per
scheduler tick. `AGENTDB_TABLET_LIFECYCLE_COOLDOWN_MS` suppresses follow-on
automatic split/merge work for tablets involved in recently completed movement
rows. These knobs only enqueue the same movement rows as the manual commands;
existing copy/catch-up/cutover safety checks still gate completion.

Retained-log cleanup is also opt-in. Set
`AGENTDB_TABLET_LOG_COMPACTION_MIN_DEBT_OPS` to compact local tablet logs when
a tablet's retained committed log entries reach the threshold. This is separate
from split/merge policy: it reduces local log debt for healthy tablets without
preparing movement rows.

Published stats are visible through:

```bash
agentdbctl tablets stats
agentdbctl tablets stats --tablet-id 7
```

Raft catch-up:

```bash
agentdbctl raft status
agentdbctl raft catch-up --tablet-id 7 --target-node-id node-b --dry-run
agentdbctl raft catch-up --tablet-id 7 --target-node-id node-b --min-lag 128
agentdbctl raft snapshots --tablet-id 7
agentdbctl raft prune-snapshots --dry-run
```

Tablet-store maintenance:

```bash
agentdbctl tablet-store compact --tablet-id 7 --dry-run
agentdbctl tablet-store compact --tablet-id 7
```

Backup and restore validation:

```bash
agentdbctl backup create --backup-id nightly-2026-05-05 --directory ./backups
agentdbctl backup create --backup-id tablet-copy --directory ./backups --include-tablet-checkpoints
agentdbctl backup create --backup-id metadata-only --skip-runtime-summary
agentdbctl restore validate ./backups/nightly-2026-05-05/manifest.json
agentdbctl restore install-tablet-checkpoint \
  --tablet-id 7 \
  --checkpoint-dir ./backups/tablet-copy/tablet-stores/tablet-00000000000000000007 \
  --tablet-store-root /var/lib/agentdb/tablet-stores
```

`--include-tablet-checkpoints` checkpoints local physical tablet-store domains
into the backup manifest. It requires `storage.tablet.layout = "grouped_rocksdb"`
with `tablets_per_domain = 1`; grouped domains with multiple tablets are
intentionally rejected because a checkpoint would include unrelated tablet
state. Restore validation recomputes checkpoint size and checksum from disk and
rejects mismatches.

`restore install-tablet-checkpoint` is an offline local filesystem operation,
not an online cluster cutover. Stop the target node first. It restores only
`grouped_rocksdb` with `--tablets-per-domain 1` and refuses non-empty target
domains.

Register or remove nodes:

```bash
agentdbctl cells upsert \
  --cell-id prod-c001 \
  --region us-west-2 \
  --domain-class standard \
  --deployment-profile multi_az_standard \
  --capacity-class stable

agentdbctl nodes register \
  --node-id node-a \
  --address http://10.0.0.10:7878 \
  --cell-id prod-c001 \
  --region us-west-2 \
  --availability-zone us-west-2a \
  --capacity-class stable

agentdbctl cells list
agentdbctl nodes delete node-a
```

## Observability Export

The first supported observability sink is ClickHouse over HTTP. The initial
implementation exports event and runtime metadata envelopes, supports event
backfill, supports tenant-scope metadata snapshot backfill, and provides
managed consumer checkpoints. Persistent AgentDB runtimes use durable ledger
cursors for export; in-memory/dev runtimes fall back to current event watch
cursors. Runtime soft deletes are exported as tombstone envelopes.

Start a local ClickHouse with the helper compose file:

```bash
docker compose -f ops/observability/clickhouse-compose.yml up -d
```

Initialize the schema:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse init --apply
```

Check AgentDB and ClickHouse reachability:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl -o json export clickhouse doctor --tenant-id ten_...
```

Export one batch:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse
```

Backfill current events and then tail continuously:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse --backfill --tail
```

Export OTel-shaped ClickStack-friendly projections in addition to the native
tables:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... --consumer prod-clickhouse \
    --mode native,otel --backfill --tail
```

Backfill metadata snapshots before the event tail:

```bash
CLICKHOUSE_URL=http://agentdb:agentdb@127.0.0.1:8123/agentdb \
  agentdbctl export clickhouse run --tenant-id ten_... \
    --include tenants,agents,sessions,artifacts,records,skills,skill_bindings \
    --backfill
```

Write durable export envelopes to JSONL:

```bash
agentdbctl export jsonl run --tenant-id ten_... --output ./agentdb-events.jsonl
```

For object-store archival, use an object-store URI. S3-compatible destinations
use the standard AWS environment variables supported by the Rust object-store
stack:

```bash
AWS_REGION=us-east-1 \
agentdbctl export jsonl run --tenant-id ten_... \
  --output s3://agentdb-exports/tenants/ten_.../events.jsonl \
  --backfill --max-batches 100
```

Continuous `--tail` is supported for local files and stdout. Object-store
outputs are uploaded as a completed object, so use `--max-batches` with
`--tail` for bounded archival batches.

Use `ops/observability/duckdb-jsonl-demo.sql` to inspect local or S3 JSONL
archives with DuckDB:

```bash
duckdb -cmd ".param set jsonl_path './agentdb-events.jsonl'" \
  -init ops/observability/duckdb-jsonl-demo.sql
```

Convert a JSONL archive to Parquet using DuckDB:

```bash
agentdbctl export parquet from-jsonl \
  --input ./agentdb-events.jsonl \
  --output ./agentdb-events.parquet

AWS_REGION=us-east-1 \
agentdbctl export parquet from-jsonl \
  --input s3://agentdb-exports/tenants/ten_.../events.jsonl \
  --output s3://agentdb-exports/tenants/ten_.../events.parquet \
  --s3-region us-east-1
```

Inspect AgentDB export capability for a tenant:

```bash
agentdbctl export clickhouse status --tenant-id ten_...
agentdbctl export clickhouse status --tenant-id ten_... --consumer prod-clickhouse
```

The underlying HTTP API also exposes
`POST /api/tenants/{tenant_id}/exports/backfill` for event and tenant-scope
metadata snapshot paging before a tailing export loop. Managed export consumers
are available through `/api/tenants/{tenant_id}/exports/consumers`; they store
durable checkpoints, include filters, lease ownership, and rewind state for
official connectors. Use `agentdbctl raw get /api/admin/exports` to inspect all
export consumers on a node, including pending item count, high watermark, lag,
and lease-active state.

For HA connector deployment, run two or more `run --tail` processes with the
same `--consumer` and distinct `--lease-owner` values. The process holding the
lease writes to ClickHouse and acks only after ClickHouse accepts the batch. A
standby process can claim the lease after `--lease-ms` expires and resumes from
the last acked cursor. Use `status --consumer` or `/api/admin/exports` to watch
lag, pending item count, high watermark, active lease state, and lease owner.

## Raw Escape Hatch

Use `raw` for endpoints that the CLI does not model yet:

```bash
agentdbctl raw get /api/admin/routing
agentdbctl raw post /api/admin/tablets/rebalance --body '{"dry_run":true}'
```

Prefer modeled commands in runbooks. `raw` is for development and emergency
workarounds.

## Design Rules

- Every mutating command maps to a documented admin API.
- `table` output is optimized for humans.
- `json` output is stable enough for scripts and AI agents.
- Dangerous operations should use explicit verbs (`delete`, `relinquish`,
  `maintenance`) rather than hidden flags.
