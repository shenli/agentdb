# Real Cluster Smoke

This runbook validates the current multi-process Raft path on one development
machine. It is intentionally a throwaway harness, not a production bootstrap
procedure.

## One-Command Local Stack

The easiest local path is the Compose-backed wrapper:

```bash
ops/smoke/scripts/real-cluster-smoke-compose.sh
```

This starts Postgres and MinIO, creates the bucket, builds the local AgentDB
binaries, seeds a throwaway cluster topology, starts three local AgentDB server
processes, runs the smoke checks, and keeps diagnostics under
`/tmp/agentdb-cluster-smoke`.

Useful options:

```bash
# Include the owner-kill failover probe.
ops/smoke/scripts/real-cluster-smoke-compose.sh --failover

# Include bounded random kill/restart chaos.
ops/smoke/scripts/real-cluster-smoke-compose.sh --chaos --chaos-iterations 48

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
with three Raft voters, starts three server processes, waits for local Raft
leaders, appends events through more than one API node, fetches the event log,
and prints health, scheduler, and Raft summaries.

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

The failover probe kills the initial owner process, waits for the scheduler to
mark it dead and move tablet ownership, then attempts another append through a
surviving node. This is the first real-process validation target for the Raft
and rebalance path.

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
