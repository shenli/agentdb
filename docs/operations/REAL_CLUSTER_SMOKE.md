# Real Cluster Smoke

This runbook validates the current multi-process Raft path on one development
machine. It is intentionally a throwaway harness, not a production bootstrap
procedure.

For the detailed scenario matrix, invariants, ASCII topology diagrams, and
planned unimplemented failure cases, see [ops/smoke/README.md](../../ops/smoke/README.md).

## One-Command Local Stack

The easiest local path is the Compose-backed wrapper:

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh
```

This starts Postgres and MinIO, creates the bucket, builds the local AgentDB
binaries, seeds a throwaway cluster topology, starts three local AgentDB server
processes, runs the smoke checks, and keeps diagnostics under
`/tmp/agentdb-cluster-smoke`.

The default seeded topology creates the default cell, one serving placement
scope, one serving tablet, three Raft voters, and distinct local AZ labels
(`local-a`, `local-b`, `local-c`). With `--two-cell --nodes 6`, it also seeds
`cell-b` with its own placement scope, tablet, and three voters. The harness
verifies cell and node placement labels through the admin APIs before running
write checks.

Useful options:

```bash
# Include the owner-kill failover probe.
ops/smoke/scripts/real-cluster-smoke-compose.sh --failover

# Seed two cells, run writes in both cells, kill the primary-cell owner,
# then verify both the failed-over tablet and the unaffected cell can write.
ops/smoke/scripts/real-cluster-smoke-compose.sh --nodes 6 --two-cell --failover

# Include bounded random kill/restart chaos.
ops/smoke/scripts/real-cluster-smoke-compose.sh --chaos --chaos-iterations 48

# Run deterministic hardening probes: process restart from persisted
# raft/KV directories, tablet-store checkpoint install, one-AZ loss,
# rolling restart, explicit replacement, and split/merge metadata cutover.
ops/smoke/scripts/real-cluster-smoke-compose.sh --nodes 6 --two-cell --full-hardening

# Leave Postgres and MinIO running after the smoke finishes.
AGENTDB_CLUSTER_SMOKE_KEEP_DEPS=1 ops/smoke/scripts/real-cluster-smoke-compose.sh

# Leave AgentDB server processes running for manual inspection.
ops/smoke/scripts/real-cluster-smoke-compose.sh --keep-running
```

`--keep-running` also keeps the Compose dependency containers up. Without
`--keep-running`, set `AGENTDB_CLUSTER_SMOKE_KEEP_DEPS=1` when you want to keep
Postgres and MinIO running after the smoke exits.

Default local ports:

- Postgres: `127.0.0.1:55432`
- MinIO API: `127.0.0.1:19000`
- MinIO console: `127.0.0.1:19001`
- AgentDB nodes: `127.0.0.1:30100`, `30101`, `30102`

Override them with:

```bash
AGENTDB_CLUSTER_SMOKE_POSTGRES_PORT=55433 \
AGENTDB_CLUSTER_SMOKE_MINIO_PORT=19010 \
AGENTDB_CLUSTER_SMOKE_MINIO_CONSOLE_PORT=19011 \
ops/smoke/scripts/real-cluster-smoke-compose.sh
```

The wrapper intentionally runs only Postgres and MinIO in containers. AgentDB
itself runs as local child processes so the harness can kill the owner process,
collect per-node logs, and inspect generated configs directly.

## Manual Dependencies

Use this path when you already have Postgres and S3-compatible storage running.

### Prerequisites

- Postgres database dedicated to this smoke run.
- S3-compatible object store with an existing bucket. MinIO is suitable.
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set for the object store.
- Ports `30100`, `30101`, and `30102` available.

Example local services:

```bash
docker run --rm --name agentdb-smoke-postgres \
  -e POSTGRES_USER=agentdb \
  -e POSTGRES_PASSWORD=agentdb \
  -e POSTGRES_DB=agentdb_smoke \
  -p 5432:5432 postgres:16

docker run --rm --name agentdb-smoke-minio \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -p 9000:9000 -p 9001:9001 \
  quay.io/minio/minio server /data --console-address :9001

mc alias set agentdb-smoke http://127.0.0.1:9000 minioadmin minioadmin
mc mb --ignore-existing agentdb-smoke/agentdb-smoke
```

### Run

```bash
export AGENTDB_META_URL=postgres://agentdb:agentdb@127.0.0.1:5432/agentdb_smoke
export AGENTDB_S3_ENDPOINT=http://127.0.0.1:9000
export AGENTDB_S3_BUCKET=agentdb-smoke
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

ops/smoke/scripts/real-cluster-smoke.sh
```

The script builds `agentdb-server` and `agentdb-cluster-smoke`, clears cluster
control-plane rows in the throwaway metadata database, seeds one serving tablet
with three Raft voters in one cell, labels the nodes across three local AZs,
starts three server processes, waits for local Raft leaders, appends events
through more than one API node, fetches the event log, and prints health,
scheduler, tablet-store, and Raft summaries.

Generated node configs use the cluster RocksDB tablet-store runtime with
`layout = "grouped_rocksdb"` and `tablets_per_domain = 1` so the smoke can
exercise checkpoint install without opening unbounded RocksDB instances. Each
domain is capped to a 4 MiB write buffer, two write buffers, one background job,
and 64 open files. Production deployments should normally use larger grouped
domains, for example `tablets_per_domain = 32`, and reserve single-tablet
domains for checkpoint/restore validation or high-isolation experiments.

To leave the servers running for manual inspection:

```bash
ops/smoke/scripts/real-cluster-smoke.sh --keep-running --keep-work-dir
```

Then inspect:

```bash
AGENTDB_URL=http://127.0.0.1:30100 target/debug/agentdbctl status
AGENTDB_URL=http://127.0.0.1:30101 target/debug/agentdbctl raft status
AGENTDB_URL=http://127.0.0.1:30102 target/debug/agentdbctl tablets list
```

## Optional Failover Probe

```bash
ops/smoke/scripts/real-cluster-smoke.sh --failover --keep-work-dir
```

The failover probe kills the initial owner process, waits for tablet ownership
to move through Raft election and ownership reconciliation, attempts another
append through a surviving node, and then waits for the slower stale-node
cleanup. In two-cell mode it also appends to the unaffected cell after the
primary-cell owner failure.

## Optional Chaos Probe

```bash
ops/smoke/scripts/real-cluster-smoke.sh --chaos --chaos-iterations 48 --keep-work-dir
```

The chaos probe is intended for nightly stability testing. It keeps the cluster
within quorum by killing and restarting one node at a time, then verifies writes
while the node is down and again after it rejoins. A deterministic run can be
replayed with:

```bash
ops/smoke/scripts/real-cluster-smoke.sh --chaos --chaos-seed 12345
```

This is not a full network-partition test yet. It targets the most common local
stability regressions: leader loss, follower loss/rejoin, restart recovery, and
replica catch-up after missed writes. The smoke output prints the raft catch-up
request duration, attempted/sent/skipped counts, and the convergence wait time
after the catch-up snapshot so deployment runs can spot catch-up regressions.

## Optional Hardening Probe

```bash
ops/smoke/scripts/real-cluster-smoke.sh --nodes 6 --two-cell --full-hardening --keep-work-dir
```

`--full-hardening` runs deterministic lifecycle checks after the normal write
and catch-up smoke:

- restarts the current owner with the same raft-engine and KV directories, then
  verifies another committed write
- creates a tablet-store checkpoint through the admin backup API and installs it
  offline into a fresh single-tablet grouped RocksDB root
- stops every node in one local AZ, verifies quorum writes continue in both
  cells, then restarts that AZ
- restarts every node one at a time and verifies writes after each restart
- runs explicit tablet replacement prepare/complete with a source applied-index
  watermark and verifies writes through the new metadata owner
- prepares and completes split/merge metadata cutover while asserting watch
  cursors still observe writes committed during the prepared split window

Individual probes can be run with `--restart-persistence`, `--checkpoint-replacement`,
`--replacement`, `--az-failure`, `--rolling-restart`, or `--split-merge`.

## Generated Files

The low-level `ops/smoke/scripts/real-cluster-smoke.sh` removes the work directory after a
successful run unless `--keep-work-dir` is passed. The Compose wrapper always
passes `--keep-work-dir` and stores dependency diagnostics there too.

The work directory contains:

- `node-a.toml`, `node-b.toml`, `node-c.toml`
- per-node runtime and content directories
- per-node logs under `logs/`
- Compose dependency logs under `diagnostics/`

## Safety

Use a disposable Postgres database. Both smoke scripts pass `--reset-meta`,
which deletes rows from cluster metadata tables before seeding the smoke
topology. The Compose wrapper runs against its own default database and leaves
volumes in place unless you remove them with Docker Compose.
