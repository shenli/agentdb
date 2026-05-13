# agentdb-bench

`agentdb-bench` is the repo-local benchmark suite for synthetic agent-runtime
workloads. It can prepare the same tenant/agent/session/event/record/artifact
dataset and run it against:

- `agentdb-native-rocksdb`: AgentDB native HTTP API with local RocksDB storage.
- `agentdb-mongodb-api`: AgentDB through its MongoDB-compatible listener.
- `mongodb-event-log`: MongoDB with normalized event-log collections.
- `postgres-event-log`: Postgres with normalized event-log tables.

The benchmark is meant for engineering decisions. Treat the numbers below as
internal single-node, cache-warm, localhost microbenchmarks, not external
product claims.

## Repository Layout

- `tools/agentdb-bench` contains the Rust benchmark CLI that prepares datasets,
  runs workloads, and writes reports.
- `scripts/agentdb-bench-*.sh` are wrapper scripts for common local runs.
- `tools/agentdb-bench/baselines/` contains baseline infrastructure and
  fixtures, not a separate runner. For example, it holds Docker Compose files
  for MongoDB/Postgres and SQL schema used by the baseline backends.

## Current Read

The current best summary is:

- For local developer tryout, AgentDB is clearly faster in the default
  `single-node-wal` profile.
- On the core bounded matrix, AgentDB leads throughput on `mixed-agent-loop`,
  `append-only`, and `append-fetch-hot`. Simple append-only now uses the
  same-session append coalescer, which removed the previous Zipfian p99 tail.
- For matched single-node media-sync durability, AgentDB has improved a lot and
  beats MongoDB at c=32, but Postgres remains a strong baseline.
- For the larger working-set profile, AgentDB now beats both MongoDB and
  Postgres after replacing JSON tablet-log encoding with a compact binary
  command format and avoiding full global-index scans.
- At very high concurrency, AgentDB can slightly exceed Postgres throughput in
  `single-node-fsync`, but with worse p95/p99 latency. That is not a large win.

Why the split matters:

- `single-node-wal` means writes reach the database WAL/memtable but do not
  force a media sync for every acknowledged operation. This is the default local
  developer profile and matches how replicated production should usually think:
  each node writes its local WAL and durability comes from Raft quorum.
- `single-node-fsync` means the single node forces local media sync durability.
  This is useful as a safety sanity check, but it is also where Postgres WAL
  group commit is extremely optimized.
- AgentDB uses a fused `append-and-fetch` API. MongoDB and Postgres baselines
  perform append and fetch as separate client operations. That is a real API
  advantage, but it is not a pure storage-engine comparison.

## Latest Comparison

All rows below use the mixed agent loop with Zipf-1.2 session selection, seed
43, 1000 sessions, and a 60s measured window unless noted. Results were captured
on `li-dev`; see `docs/operations/AGENTDB_BENCH.md` for historical tuning
details and run paths.

The workload is intentionally agent-runtime shaped:

- `mixed-agent-loop` models a service running tool-using agents, not a generic
  key-value or OLTP benchmark. Each measured operation is one runtime action:
  append the next event and read context, fetch recent context, write/search
  memory records, fork a session, or attach artifact metadata.
- The current operation mix is 50% append-and-fetch, 25% fetch recent context,
  8% record write, 7% record search, 7% fork, and 3% artifact. That emphasizes
  the hot path of an agent step: log the model/tool result, immediately recover
  the tail needed for the next step, occasionally update memory, branch, or
  attach generated files.
- The dataset has many sessions rather than one global stream because agent
  systems are naturally session-scoped. A session is the unit for ordered event
  history, read-after-write semantics, replay, fork, and debugging.
- Zipf-1.2 session selection means some sessions are much hotter than others.
  That matches real agent services where active conversations or long-running
  tasks receive repeated writes while most sessions are idle or cold.
- The 1000-session shape is large enough for skew and batching behavior to show
  up, but still small enough to keep the current comparison focused on the
  single-node hot path. It is not a working-set-exceeds-cache benchmark.

For cache-pressure validation, use `PROFILE=working-set-exceeds-cache` instead
of the latest-comparison shape. That preset prepares a much larger logical
history: 4,000 sessions, 2 million prepared events, 400,000 prepared records,
and about 8 GiB of event payload before database/index overhead. The generated
`report.md` prints total sessions, total events, total records, and estimated
event payload bytes. Compare that estimate with host RAM, database cache
settings, and observed disk usage before claiming the run truly exceeds cache.

| Profile | Concurrency | AgentDB | MongoDB | Postgres | What It Means |
|---|---:|---:|---:|---:|---|
| `single-node-wal` | 32 | 53.9k ops/s, p99 2.08ms | 26.0k ops/s, p99 2.86ms | 13.1k ops/s, p99 35.8ms | Best local developer story: AgentDB is 2.07x MongoDB and 4.11x Postgres. |
| `working-set-exceeds-cache`, `single-node-wal`, 300s | 32 | 37.2k ops/s, p99 2.35ms | 23.8k ops/s, p99 2.22ms | 6.0k ops/s, p99 114ms | AgentDB is 1.56x MongoDB and 6.18x Postgres on the larger cache-pressure cell. |
| `single-node-fsync` | 32 | 1,545 ops/s, p99 70ms | 884 ops/s, p99 65ms | 2,182 ops/s, p99 238ms | Compact tablet log improves AgentDB, but forced media sync remains the ceiling. |
| `single-node-fsync` tuning cell | 256 | 2,355 ops/s, p99 615ms | not rerun | 2,127 ops/s, p99 386ms | AgentDB slightly beats Postgres throughput, but has worse tail latency. |

### Bounded Core Workload Matrix

This 30s `li-dev` matrix uses the bounded latest-comparison shape: 10 tenants,
2 agents per tenant, 50 sessions per agent, 50 prepared events per session, 10
records per session, 2 KiB event payloads, c=32, Zipf-1.2, and
`single-node-wal`. It is a fast engineering check, not a publication-grade
repeat set.

| Workload | AgentDB | MongoDB | Postgres | What It Means |
|---|---:|---:|---:|---|
| `mixed-agent-loop` | 44,393 ops/s, p99 2.03ms | 24,644 ops/s, p99 2.27ms | 20,298 ops/s, p99 17.39ms | AgentDB is 1.80x MongoDB and 2.19x Postgres on the full agent loop. |
| `append-only` | 79,781 ops/s, p99 1.15ms | 15,384 ops/s, p99 4.98ms | 9,897 ops/s, p99 37.82ms | Simple append-only benefits from same-session coalescing under Zipfian hot sessions. |
| `append-fetch-hot` | 38,481 ops/s, p99 2.28ms | 16,959 ops/s, p99 2.83ms | 22,539 ops/s, p99 12.45ms | AgentDB's fused append+tail-read path remains the strongest API-shape win. |

The previous append-only cell was 23,878 ops/s with p99 8.71ms. Routing simple
append-only requests through the same-session coalescer moved the same cell to
79,781 ops/s with p99 1.15ms, while the mixed-agent-loop and append-fetch-hot
checks stayed flat.

Use this interpretation:

- If the question is "Is AgentDB fast out of the box for a local agent runtime?",
  use the `single-node-wal` row.
- If the question is "Does AgentDB beat mature databases under forced local
  fsync?", the honest answer is mixed. It beats MongoDB in the current c=32 cell,
  but Postgres is still highly competitive.
- If the question is "Does the local WAL advantage survive cache pressure?",
  the current answer is yes for throughput. MongoDB still has slightly tighter
  p99 in this single run, so repeat before using the row externally.
- If the question is "Why not a huge Postgres win yet?", the bottleneck is not
  query flexibility or read caching. It is durable WAL/fsync batching. A larger
  margin likely requires a more specialized durable append-log path than the
  current RocksDB sync-write path.

## Run A Smoke Test

Run the full local AgentDB RocksDB/content-engine smoke:

```bash
PROFILE=smoke ARTIFACT_MODE=content-backend \
  scripts/agentdb-bench-local.sh
```

The wrapper builds release binaries by default. Use `BUILD_PROFILE=debug` for
faster development-only smoke runs:

```bash
BUILD_PROFILE=debug PROFILE=smoke scripts/agentdb-bench-local.sh
```

To start AgentDB manually instead:

```bash
cargo run -p agentdb-server -- serve \
  --bind 127.0.0.1:7890 \
  --data-dir /tmp/agentdb-bench-data \
  --content-dir /tmp/agentdb-bench-content
```

Artifact bytes go through AgentDB's content engine. In the local example that
is the filesystem, not RocksDB, because RocksDB is not the right place for large
blobs.

## Run The Main Comparison

For a broader value-prop matrix across backends and workloads, use:

```bash
MATRIX_ID=value-wal-$(date +%Y%m%d) \
  BACKENDS=agentdb,mongodb,postgres \
  WORKLOADS=mixed-agent-loop,append-only,append-fetch-hot,artifact-only,record-search-only \
  PROFILE=default-agent-runtime \
  TENANTS=10 \
  AGENTS_PER_TENANT=2 \
  SESSIONS_PER_AGENT=50 \
  EVENTS_PER_SESSION=50 \
  RECORDS_PER_SESSION=10 \
  PAYLOAD_BYTES=2048 \
  DURABILITY_PROFILE=single-node-wal \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  FETCH_LAST_N=20 \
  NO_RAW_JSONL=1 \
  scripts/agentdb-bench-value-matrix.sh
```

This writes a Markdown matrix under `bench-out/matrix/`. Use it for internal
engineering comparisons before updating the curated table above. For
publication-grade numbers, set `REPEATS=3` and keep the generated per-backend
summaries. Keep the explicit dataset dimensions unless the host has enough disk
for the full `default-agent-runtime` preset; the preset itself prepares a much
larger corpus than the curated latest-comparison shape.

For a long-run validation of the same value-prop matrix, use:

```bash
MATRIX_ID=longrun-value-wal-$(date +%Y%m%d) \
  OUT=/path/with/enough/free/space \
  scripts/agentdb-bench-longrun-value-matrix.sh
```

The long-run wrapper defaults to `REPEATS=3`, seeds `43,44,45`, `WARMUP=30s`,
`DURATION=10m`, the bounded 1000-session shape above, c=32, Zipf-1.2, and
`single-node-wal`. Use it before making external performance claims; use
`scripts/agentdb-bench-value-matrix.sh` for faster one-off engineering checks.

To inspect a running or completed matrix, use:

```bash
scripts/agentdb-bench-status.sh \
  --out /home/lishen/work/agentdb-bench-runs \
  --matrix-id longrun-value-wal-20260513-1
```

The status helper prints disk headroom, matching active processes, completed
repeat summaries, completed run-report count, and the tail of the matrix log.

To generate a comparison-ready Markdown summary from completed repeat summaries,
use:

```bash
scripts/agentdb-bench-finalize-matrix.sh \
  --out /home/lishen/work/agentdb-bench-runs \
  --matrix-id longrun-value-wal-20260513-1
```

The finalizer writes `OUT/matrix/<matrix-id>-final.md`. It is safe to run while
a matrix is still active: completed cells are included, missing cells remain
absent, and active matching processes are recorded in the generated summary.

Long-run outputs can fill disks quickly. Prune benchmark roots with a dry-run
first:

```bash
scripts/agentdb-bench-prune.sh \
  --root /home/lishen/work/agentdb-bench-runs \
  --keep 30 \
  --target-free-gb 120
```

If the dry-run plan is correct, add `--delete`. The prune script only considers
direct child directories under `--root`, keeps the newest `--keep` directories,
and refuses unsafe roots such as `/` and `$HOME`. If old MongoDB or Postgres
Docker data is root-owned, add `--sudo` to the dry-run and delete commands so
the script can size and remove those directories with passwordless sudo. Shared
output directories named `logs`, `matrix`, `runs`, and `summaries` are excluded by
default; add repeated `--exclude NAME` flags for other non-run directories. If
a delete run hits unreadable or root-owned directories without `--sudo`, it
continues deleting the remaining selected directories and reports the failed
paths at the end.

AgentDB native:

```bash
BENCH_SCRIPT=scripts/agentdb-bench-local.sh \
  RUN_ID=agentdb-zipf-wal \
  PROFILE=default-agent-runtime \
  WORKLOAD=mixed-agent-loop \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  DURABILITY_PROFILE=single-node-wal \
  REPEATS=3 \
  SEEDS=43,44,45 \
  NO_RAW_JSONL=1 \
  scripts/agentdb-bench-repeat.sh
```

MongoDB:

```bash
BENCH_SCRIPT=scripts/agentdb-bench-mongodb.sh \
  RUN_ID=mongodb-zipf-wal \
  PROFILE=default-agent-runtime \
  WORKLOAD=mixed-agent-loop \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  DURABILITY_PROFILE=single-node-wal \
  REPEATS=3 \
  SEEDS=43,44,45 \
  NO_RAW_JSONL=1 \
  scripts/agentdb-bench-repeat.sh
```

Postgres:

```bash
BENCH_SCRIPT=scripts/agentdb-bench-postgres.sh \
  RUN_ID=postgres-zipf-wal \
  PROFILE=default-agent-runtime \
  WORKLOAD=mixed-agent-loop \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  DURABILITY_PROFILE=single-node-wal \
  REPEATS=3 \
  SEEDS=43,44,45 \
  NO_RAW_JSONL=1 \
  scripts/agentdb-bench-repeat.sh
```

For the matched local fsync sanity check, change
`DURABILITY_PROFILE=single-node-wal` to `DURABILITY_PROFILE=single-node-fsync`
for all backends.

## Key Knobs

| Knob | Common Values | Use |
|---|---|---|
| `PROFILE` | `smoke`, `default-agent-runtime`, `working-set-exceeds-cache` | Dataset size preset. Use `smoke` only for correctness checks. |
| `WORKLOAD` | `mixed-agent-loop`, `append-only`, `append-fetch-hot`, `record-write-only`, `record-search-only`, `artifact-only`, `fork-only` | Choose the agent-runtime path to measure. |
| `CONCURRENCY` | `32`, `1,4,16,32`, `256` | Client worker count. Use the same value across backends. |
| `DURABILITY_PROFILE` | `single-node-wal`, `single-node-fsync`, `strict-fsync` | Controls durability semantics. Do not mix profiles in one comparison. |
| `SESSION_SELECTION` | `uniform`, `zipf`, `hotset-90-10` | Controls session hotness. Use hundreds or thousands of sessions for skew. |
| `ZIPF_EXPONENT` | `1.2` | Skew strength for `SESSION_SELECTION=zipf`. |
| `ARTIFACT_MODE` | `reference`, `content-backend`, `mixed` | Controls whether artifact bytes are stored through the content engine. |
| `NO_RAW_JSONL` | `1` | Avoids large per-operation logs in bigger runs. |
| `MIN_FREE_GB` | `5`, `80` | Minimum free disk guard used by wrapper scripts. Defaults to `80` for `working-set-exceeds-cache`. |
| `AGENTDB_BENCH_NOFILE` | `65535` | File descriptor limit requested by AgentDB wrappers for `working-set-exceeds-cache`. |
| `AGENTDB_PERSISTED_HOT_EVENT_CACHE_PER_SESSION` | `64`, `2048` | AgentDB persisted hot-tail event cache per session. AgentDB wrappers default this to `64` for `working-set-exceeds-cache`. |
| `AGENTDB_ROCKSDB_COMPRESSION` | `lz4`, `none`, `snappy`, `lz4hc` | RocksDB compression used by AgentDB. Defaults to `lz4` to reduce working-set storage and compaction IO. |
| `AGENTDB_APPEND_GROUP_COMMIT_US` | `0`, `1000`, `2000`, `4000` | AgentDB append group-commit wait window. The single-node fsync default is `2000`; WAL mode defaults to `0`. |
| `AGENTDB_SESSION_APPEND_BATCH_WAIT_US` | `0`, `250`, `1000` | Experimental same-session append coalescing wait. Keep at `0` unless a profile proves p99 still improves. |
| `APPEND_PATH_PROFILE_MS` | `5000` | Enables AgentDB server-side append-stage profiling in logs. |
| `AGENTDB_API_JSON_PROFILE_MS` | `5000` | Enables AgentDB JSON response serialization profiling in logs. |

`strict-fsync` disables append group commit and is mostly a stress profile. It
should not be the default comparison row.

## Backend Shapes

The default MongoDB and Postgres baselines are intentionally not strawman
single-document designs:

- MongoDB uses separate `sessions`, `events`, `records`, and `artifacts`
  collections with tenant/session/sequence indexes.
- Postgres uses normalized session/event/record/artifact tables.
- Large artifact bytes are not stored inside MongoDB documents or Postgres JSONB
  session rows for the default comparison.

AgentDB's MongoDB-compatible listener is measured separately:

```bash
PROFILE=smoke scripts/agentdb-bench-agentdb-mongodb.sh
```

Treat `agentdb-mongodb-api` as adapter-overhead measurement for AgentDB, not as
the same thing as real MongoDB.

The MongoDB and Postgres baseline wrappers use per-run Docker Compose project
names and tear containers down on exit. MongoDB data is bind-mounted under the
benchmark output root and the container runs as the host `MONGO_UID:MONGO_GID`
by default, so new benchmark data remains removable by the benchmark user.
Unless `MONGO_PORT` or `PG_PORT` is set, the wrappers choose a free localhost
port at runtime so they do not conflict with existing development containers.
When testing from a separate worktree, set `CARGO_TARGET_DIR` to reuse an
existing Cargo target directory without breaking the wrapper binary lookup.

## Working-Set-Exceeds-Cache Cell

Use this cell to test whether the comparison survives beyond a cache-warm hot
set. It is intentionally heavier than the latest-comparison row and should be
run on `li-dev` or another machine with enough disk space:

```bash
BENCH_SCRIPT=scripts/agentdb-bench-local.sh \
  RUN_ID=agentdb-working-set-wal \
  PROFILE=working-set-exceeds-cache \
  WORKLOAD=mixed-agent-loop \
  CONCURRENCY=32 \
  WARMUP=30s \
  DURATION=300s \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  DURABILITY_PROFILE=single-node-wal \
  MIN_FREE_GB=80 \
  REPEATS=1 \
  SEEDS=43 \
  NO_RAW_JSONL=1 \
  scripts/agentdb-bench-repeat.sh
```

Run the same cell with `scripts/agentdb-bench-mongodb.sh` and
`scripts/agentdb-bench-postgres.sh` before drawing a comparison. If the prepared
dataset plus database/index overhead still fits in RAM and database cache on
the test host, increase `TENANTS`, `SESSIONS_PER_AGENT`, `EVENTS_PER_SESSION`,
or `PAYLOAD_BYTES` explicitly and record those overrides in the report.

## Manual Prepare And Run

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

## Outputs

Each run writes files under `bench-out/runs/<run-id>/`:

- `dataset.json`: prepared tenant/agent/session IDs.
- `host-manifest.json`: hardware and OS information.
- `software-manifest.json`: toolchain and runtime versions.
- `database-config.json`: storage/content engine configuration.
- `raw.jsonl`: per-operation latency records, unless `NO_RAW_JSONL=1`.
- `report-*.json`: machine-readable latency summaries.
- `report.md`: human-readable aggregate and per-operation report.

`scripts/agentdb-bench-repeat.sh` also writes aggregate summaries under
`bench-out/summaries/<RUN_ID>/summary.md` and `summary.json`.

## Publication Rules

Before quoting numbers outside the repo:

- Repeat runs with multiple seeds and report variance.
- Use at least 60s measured windows; use longer windows for compaction and
  checkpoint behavior.
- Keep durability profiles matched across all backends.
- Say when AgentDB's fused `append-and-fetch` API is part of the advantage.
- Add working-set-exceeds-cache and tenant-aware noisy-neighbor cells for
  broader claims.
- Keep historical tuning data in `docs/operations/AGENTDB_BENCH.md`, not in
  this README.
