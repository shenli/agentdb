# agentdb-bench

`agentdb-bench` prepares and runs synthetic agent-runtime workloads against
AgentDB, Postgres, and MongoDB. The primary AgentDB backend is
`agentdb-native-rocksdb`: native HTTP API plus local RocksDB storage.

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

Latest `li-dev` AgentDB local RocksDB results captured for this PR:

| Run | Workload | c=1 | c=4 | c=8 | c=16 | c=32 |
|---|---|---:|---:|---:|---:|---:|
| `agentdb-append-lockfix` | `append-fetch-hot` | 5634 ops/s | 10431 ops/s | 13515 ops/s | 13377 ops/s | 13961 ops/s |
| `agentdb-mixed-lockfix` | `mixed-agent-loop` | 6194 ops/s | 15322 ops/s | 17652 ops/s | 17063 ops/s | 16761 ops/s |

The detailed reports are stored on `li-dev` under
`/home/lishen/agentdb-bench-runs/<run-id>/runs/<run-id>/report.md`.

Run the normalized Postgres event-log baseline:

```bash
PROFILE=smoke scripts/agentdb-bench-postgres.sh
```

The Postgres baseline uses `benchmarks/postgres/docker-compose.yml` and
`benchmarks/postgres/event_log.sql`. It stores normalized session/event rows and
artifact metadata references. It does not put large artifact bytes into a JSONB
session row.

Run the normalized MongoDB event-log baseline:

```bash
PROFILE=smoke scripts/agentdb-bench-mongodb.sh
```

The MongoDB baseline uses `benchmarks/mongodb/docker-compose.yml` and the
`mongodb-event-log` backend. It stores sessions, events, records, and artifacts
in separate collections with tenant/session/sequence indexes. It intentionally
does not use the naive single-session-document model for the default comparison.

Run AgentDB through its MongoDB-compatible listener:

```bash
PROFILE=smoke scripts/agentdb-bench-agentdb-mongodb.sh
```

This uses the `agentdb-mongodb-api` backend. It starts AgentDB with both the
native HTTP server and MongoDB wire listener, prepares data through MongoDB
driver calls, and runs workloads through AgentDB's MongoDB-compatible command
subset. Treat this as an adapter-overhead profile for AgentDB, separate from
both `agentdb-native-rocksdb` and real `mongodb-event-log`.

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

For MongoDB directly:

```bash
cargo run -p agentdb-bench -- prepare \
  --backend mongodb-event-log \
  --mongo-uri mongodb://127.0.0.1:27017 \
  --mongo-db agentdb_bench \
  --run-id smoke-mongo \
  --out bench-out \
  --profile smoke

cargo run -p agentdb-bench -- run \
  --backend mongodb-event-log \
  --mongo-uri mongodb://127.0.0.1:27017 \
  --mongo-db agentdb_bench \
  --run-id smoke-mongo \
  --out bench-out \
  --workload mixed-agent-loop
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
