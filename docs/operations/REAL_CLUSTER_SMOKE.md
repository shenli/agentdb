# Real Cluster Smoke

This runbook validates the current multi-process Raft path on one development
machine. It is intentionally a throwaway harness, not a production bootstrap
procedure.

## Prerequisites

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

## Run

```bash
export AGENTDB_META_URL=postgres://agentdb:agentdb@127.0.0.1:5432/agentdb_smoke
export AGENTDB_S3_ENDPOINT=http://127.0.0.1:9000
export AGENTDB_S3_BUCKET=agentdb-smoke
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

scripts/real-cluster-smoke.sh
```

The script builds `agentdb-server` and `agentdb-cluster-smoke`, clears cluster
control-plane rows in the throwaway metadata database, seeds one serving tablet
with three Raft voters, starts three server processes, waits for local Raft
leaders, appends events through more than one API node, fetches the event log,
and prints health, scheduler, and Raft summaries.

To leave the servers running for manual inspection:

```bash
scripts/real-cluster-smoke.sh --keep-running --keep-work-dir
```

Then inspect:

```bash
AGENTDB_URL=http://127.0.0.1:30100 target/debug/agentdbctl status
AGENTDB_URL=http://127.0.0.1:30101 target/debug/agentdbctl raft status
AGENTDB_URL=http://127.0.0.1:30102 target/debug/agentdbctl tablets list
```

## Optional Failover Probe

```bash
scripts/real-cluster-smoke.sh --failover --keep-work-dir
```

The failover probe kills the initial owner process, waits for the scheduler to
mark it dead and move tablet ownership, then attempts another append through a
surviving node. This is the first real-process validation target for the Raft
and rebalance path.

## Generated Files

By default the harness uses `/tmp/agentdb-cluster-smoke` and removes it after a
successful run. Use `--keep-work-dir` to retain:

- `node-a.toml`, `node-b.toml`, `node-c.toml`
- per-node runtime and content directories
- per-node logs under `logs/`

## Safety

Use a disposable Postgres database. `scripts/real-cluster-smoke.sh` passes
`--reset-meta`, which deletes rows from cluster metadata tables before seeding
the smoke topology.
