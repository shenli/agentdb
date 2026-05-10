# agentdb-bench

`agentdb-bench` prepares and runs synthetic agent-runtime workloads against
AgentDB. The first implemented backend is `agentdb-native-rocksdb`: native HTTP
API plus local RocksDB storage.

In this profile, RocksDB stores metadata and indexes. Artifact bytes are written
through AgentDB's content engine, which is local filesystem in the example
below. This avoids putting large blobs into RocksDB.

Start a local persistent AgentDB server:

```bash
cargo run -p agentdb-server -- serve \
  --bind 127.0.0.1:7890 \
  --data-dir /tmp/agentdb-bench-data \
  --content-dir /tmp/agentdb-bench-content
```

Or run the full local RocksDB/content-engine smoke in one command:

```bash
PROFILE=smoke ARTIFACT_MODE=content-backend \
  scripts/agentdb-bench-local.sh
```

The runner builds release binaries by default. Set `BUILD_PROFILE=debug` for a
faster development-only smoke.

Run the normalized Postgres event-log baseline:

```bash
PROFILE=smoke scripts/agentdb-bench-postgres.sh
```

The Postgres baseline uses `benchmarks/postgres/docker-compose.yml` and
`benchmarks/postgres/event_log.sql`. It stores normalized session/event rows and
artifact metadata references. It does not put large artifact bytes into a JSONB
session row.

Prepare a dataset:

```bash
cargo run -p agentdb-bench -- prepare \
  --base-url http://127.0.0.1:7890 \
  --run-id smoke-local \
  --out bench-out \
  --profile smoke \
  --events-per-session 10 \
  --records-per-session 2 \
  --artifact-rate 0.2 \
  --artifact-mode content-backend \
  --data-dir /tmp/agentdb-bench-data \
  --content-dir /tmp/agentdb-bench-content
```

Run a workload:

```bash
cargo run -p agentdb-bench -- run \
  --base-url http://127.0.0.1:7890 \
  --run-id smoke-local \
  --out bench-out \
  --workload mixed-agent-loop \
  --concurrency 1,4,16 \
  --warmup 5s \
  --duration 30s
```

Outputs are written under `bench-out/runs/<run-id>/`:

- `dataset.json` records prepared tenant/agent/session IDs.
- `host-manifest.json` records hardware and OS information.
- `software-manifest.json` records toolchain and runtime versions.
- `database-config.json` records storage/content engine configuration.
- `raw.jsonl` records each measured operation by operation type, latency,
  success/failure, and worker ID. Set `NO_RAW_JSONL=1` when using
  `scripts/agentdb-bench-local.sh` if raw operation output is too expensive for
  a large run.
- `report-*.json` contains machine-readable latency summaries.
- `report.md` contains a compact human-readable report with aggregate and
  per-operation latency tables.
