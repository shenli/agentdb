# agentdb-bench

`agentdb-bench` prepares and runs synthetic agent-runtime workloads against
AgentDB, Postgres, and MongoDB. The primary AgentDB backend is
`agentdb-native-rocksdb`: native HTTP API plus local RocksDB storage.

Treat the current numbers as internal single-node microbenchmarks for
agent-runtime API shape and hot-path behavior. They are not yet external
marketing claims about general database performance: the default comparison is
single-host, in-process-to-localhost, cache-warm, and uses AgentDB's fused
`append-and-fetch` primitive while the Postgres/MongoDB baselines perform an
append followed by a fetch.

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

## Current Comparison Snapshot

The table below keeps only the latest comparison cells. Historical tuning runs
live in `docs/operations/AGENTDB_BENCH.md`; they are useful for engineering
forensics, but they should not be the first thing a benchmark reader sees.

| Cell | Shape | AgentDB | MongoDB | Postgres | Ratio |
|---|---|---:|---:|---:|---|
| Mixed loop, Zipf-1.2, `single-node-wal` | c=32, seed 43, 60s measured window, 1000 sessions | 53.9k ops/s | 26.0k ops/s | 13.1k ops/s | 2.07x MongoDB, 4.11x Postgres |

Interpret these as internal single-node benchmark cells, not product claims.
The AgentDB API uses a fused `append-and-fetch` primitive; MongoDB and Postgres
perform append and fetch as separate client operations. `single-node-wal` is
the default local developer profile: AgentDB writes to the RocksDB WAL and
memtable without forcing a per-request media sync, MongoDB uses standalone
acknowledged writes without `journal=true`, and Postgres uses
`synchronous_commit=off`.

The first matched `single-node-fsync` sanity run tells a different story:

| Cell | Shape | AgentDB | MongoDB | Postgres | Result |
|---|---|---:|---:|---:|---|
| Mixed loop, Zipf-1.2, `single-node-fsync` | c=32, seed 43, 60s measured window | 256 ops/s | 884 ops/s | 2,182 ops/s | AgentDB is 0.29x MongoDB and 0.12x Postgres |

After same-session append coalescing, a compact append-log fast path, shared
fsync group-commit batching for Persist writes, and a zero same-session
coalescing wait, the AgentDB `single-node-fsync` c=32 cell improved to 1,441
ops/s with p95 47ms and p99 75ms on the same `li-dev` shape. That is a 5.6x
AgentDB improvement over the first matched durable run. AgentDB now beats the
MongoDB `single-node-fsync` sanity cell, but still trails the Postgres
`single-node-fsync` sanity cell at c=32.

A higher-concurrency c=256 tuning cell reached 2,355 ops/s for AgentDB versus
2,127 ops/s for Postgres, but AgentDB's p95/p99 were worse. Treat that as
evidence that AgentDB can batch enough work to match Postgres throughput, not
as proof of a large fsync-mode advantage.

That `single-node-fsync` result uses RocksDB sync writes with safe append group
commit, MongoDB `journal=true&w=1`, and Postgres `synchronous_commit=on`. It is
a single sanity cell, not a final comparison. Publication-quality numbers still
need repeated durable runs, working-set-exceeds-cache profiles,
tenant-aware noisy-neighbor cells, and multi-minute runs across more workload
shapes.

Use `WORKLOAD=append-only` to isolate append acknowledgement throughput without
the tail-read/context serialization included in `append-fetch-hot`.
Use `WORKLOAD=record-write-only`, `record-search-only`, `artifact-only`, or
`fork-only` to isolate the non-append paths that contribute to
`mixed-agent-loop` latency.

Use `SESSION_SELECTION=uniform`, `SESSION_SELECTION=zipf`, or
`SESSION_SELECTION=hotset-90-10` to control how workers choose sessions during
the measured run. `ZIPF_EXPONENT` defaults to `1.2` for Zipfian selection. Use
hundreds or thousands of sessions for skewed runs; the `smoke` profile is too
small for meaningful skew sensitivity. See
`docs/operations/AGENTDB_BENCH.md` for the current short skew-sensitivity
control run and caveats.

Use `DURABILITY_PROFILE=single-node-wal`, `single-node-fsync`, or
`strict-fsync` to make durability explicit in wrapper scripts.
`single-node-wal` is the default local setup and is optimized for developer
tryout performance with RocksDB WAL safety. `single-node-fsync` enables local
sync writes while preserving safe append group commit. `strict-fsync` disables
append group commit to expose per-operation fsync cost and should be treated as
a stress profile, not the default comparison.

Use `APPEND_PATH_PROFILE_MS=5000` with the AgentDB wrapper scripts to emit
server-side append-stage profiles into the run log. This is the right first
step before tuning durable writes because it separates session lock wait,
state-lock work, group-commit queue wait, and RocksDB write time.

Use `scripts/agentdb-bench-repeat.sh` to run clean per-seed repeats and produce
an aggregate summary:

```bash
BENCH_SCRIPT=scripts/agentdb-bench-local.sh \
  RUN_ID=agentdb-zipf-60s \
  PROFILE=default-agent-runtime \
  WORKLOAD=mixed-agent-loop \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  REPEATS=3 \
  SEEDS=43,44,45 \
  NO_RAW_JSONL=1 \
  scripts/agentdb-bench-repeat.sh
```

The repeat script creates one run directory per seed and writes
`bench-out/summaries/<RUN_ID>/summary.md` plus `summary.json`. Point
`BENCH_SCRIPT` at `scripts/agentdb-bench-mongodb.sh` or
`scripts/agentdb-bench-postgres.sh` to repeat the same cell on those backends.
The current `li-dev` 3x/60s Zipf c=32 cell is documented in
`docs/operations/AGENTDB_BENCH.md`; it produced a 1.66x AgentDB/MongoDB ratio
and a 19.84x AgentDB/Postgres ratio for that internal single-node cell.

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
  --session-selection zipf \
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
