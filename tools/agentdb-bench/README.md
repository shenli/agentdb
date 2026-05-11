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
| `agentdb-native-single-mixed-v2` | `mixed-agent-loop` | 6268 ops/s | 13960 ops/s | 17204 ops/s | 17706 ops/s | 17429 ops/s |
| `agentdb-mongodb-api-append` | `append-fetch-hot` | 5363 ops/s | 6586 ops/s | 14118 ops/s | 12028 ops/s | 11937 ops/s |
| `agentdb-mongodb-api-single-mixed-v2` | `mixed-agent-loop` | 4505 ops/s | 6058 ops/s | 15041 ops/s | 12651 ops/s | 12467 ops/s |
| `agentdb-adaptive-group-append-sweep` | `append-fetch-hot` | 9459 ops/s | n/a | 23467 ops/s | 27354 ops/s | 28035 ops/s |
| `agentdb-adaptive-group-mixed` | `mixed-agent-loop` | 9766 ops/s | n/a | 19143 ops/s | 25492 ops/s | 24242 ops/s |
| `agentdb-defaultgroup-append-only-sweep` | `append-only` | 11442 ops/s | n/a | 27082 ops/s | 32484 ops/s | 34120 ops/s |
| `agentdb-defaultgroup-append-fetch-hot-sweep` | `append-fetch-hot` | 10103 ops/s | n/a | 23740 ops/s | 28243 ops/s | 28286 ops/s |
| `agentdb-defaultgroup-mixed-agent-loop-sweep` | `mixed-agent-loop` | 10878 ops/s | n/a | 19719 ops/s | 24491 ops/s | 24321 ops/s |
| `agentdb-mimalloc-hotcache-append-only-sweep` | `append-only` | 12033 ops/s | n/a | 26874 ops/s | 34484 ops/s | 35249 ops/s |
| `agentdb-mimalloc-hotcache-append-fetch-hot-sweep` | `append-fetch-hot` | 10138 ops/s | n/a | 24702 ops/s | 30354 ops/s | 31613 ops/s |
| `agentdb-mimalloc-hotcache-mixed-agent-loop-sweep` | `mixed-agent-loop` | 11692 ops/s | n/a | 21695 ops/s | 28290 ops/s | 25292 ops/s |
| `agentdb-mixedpaths-append-only-sweep` | `append-only` | 12301 ops/s | n/a | 28010 ops/s | 34684 ops/s | 36592 ops/s |
| `agentdb-mixedpaths-append-fetch-hot-sweep` | `append-fetch-hot` | 10938 ops/s | n/a | 25140 ops/s | 30746 ops/s | 31614 ops/s |
| `agentdb-mixedpaths-mixed-agent-loop-sweep` | `mixed-agent-loop` | 11266 ops/s | n/a | 32935 ops/s | 40515 ops/s | 41236 ops/s |
| `agentdb-mixedpaths-record-write-only-sweep` | `record-write-only` | 11535 ops/s | n/a | 21602 ops/s | 28966 ops/s | 35192 ops/s |
| `agentdb-mixedpaths-artifact-only-sweep` | `artifact-only` | 11534 ops/s | n/a | 26054 ops/s | 32596 ops/s | 34523 ops/s |
| `agentdb-final-eventsource-session-mixed-sweep` | `mixed-agent-loop` | 10812 ops/s | n/a | 39396 ops/s | 49686 ops/s | 52920 ops/s |
| `agentdb-final-policyfast-mixed-sweep` | `mixed-agent-loop` | 11714 ops/s | n/a | 39964 ops/s | 52085 ops/s | 55834 ops/s |
| `mongodb-append-only-fixed` | `append-only` | 5303 ops/s | n/a | 6173 ops/s | 20477 ops/s | 19553 ops/s |
| `mongodb-mixed-same-shape` | `mixed-agent-loop` | 3526 ops/s | 2808 ops/s | 4846 ops/s | 24217 ops/s | 25603 ops/s |
| `mongodb-final-mixed-current-shape` | `mixed-agent-loop` | 3867 ops/s | n/a | 6357 ops/s | 25460 ops/s | 26186 ops/s |
| `postgres-final-mixed-current-shape` | `mixed-agent-loop` | 449 ops/s | n/a | 2106 ops/s | 3776 ops/s | 5334 ops/s |

The detailed reports are stored on `li-dev` under the run-specific `OUT`
directory, usually
`/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/<run-id>/runs/<run-id>/report.md`
for larger local runs.

The latest native RocksDB mixed run uses adaptive write batching, hot-tail
append-and-fetch, route-level backpressure reuse, event-sourced session-head
recovery, and a fast path for metadata-only tenant policy JSON. On `li-dev`,
`agentdb-final-policyfast-mixed-sweep` reached 55.8k ops/s at c=32. The same
benchmark shape reached 26.2k ops/s on MongoDB and 5.3k ops/s on Postgres, so
this run is about 2.13x MongoDB and 10.47x Postgres for the synthetic mixed
agent loop.

Use `WORKLOAD=append-only` to isolate append acknowledgement throughput without
the tail-read/context serialization included in `append-fetch-hot`.
Use `WORKLOAD=record-write-only`, `record-search-only`, `artifact-only`, or
`fork-only` to isolate the non-append paths that contribute to
`mixed-agent-loop` latency.

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
