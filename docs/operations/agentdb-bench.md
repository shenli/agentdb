# AgentDB Benchmark Methodology

`agentdb-bench` is the repo-local benchmark suite for comparing AgentDB with
MongoDB and Postgres on agent-runtime workloads. The first implementation phase
focuses on local AgentDB storage backed by RocksDB. Cluster, remote object
storage, and replicated durability profiles come later, after the local storage
comparison is stable and reproducible.

The goal is not to prove that AgentDB is a better general-purpose database. The
goal is to measure whether AgentDB's native session, fork, memory, index, and
artifact model reduces latency, storage cost, and application complexity for
agent execution logs.

The benchmark must be reproducible, transparent, and conservative. Any public
claim must include the exact schema, hardware, durability setting, workload mix,
git SHA, and database versions used for the run.

The archived dedicated-host numbers are internal microbenchmarks for the
agent-runtime data model; they should not be used as external MongoDB/Postgres
performance claims. The historical comparison shape is
single-node, same-host, cache-warm, short-duration, and API-shape aware:
AgentDB uses its fused `append-and-fetch` primitive, while MongoDB/Postgres
baselines perform append and fetch as separate client operations.

Before publishing external ratios, close these benchmark gaps:

- Derive the operation mix from real agent traces, or label it as a heuristic
  mix and publish the rationale for each operation weight.
- Run the implemented `working-set-exceeds-cache` profile and report dataset
  bytes versus RAM, page cache, RocksDB block cache, MongoDB WiredTiger cache,
  and Postgres shared buffers.
- Expand the durability matrix: one single-node-fsync sanity cell now exists,
  but it needs repeats and follow-up after append-path instrumentation.
- Run longer multi-minute cells so RocksDB compaction, Postgres checkpoint, and
  MongoDB journal/checkpoint tails can appear in p99/max latency.
- Repeat skewed session selection with publication-grade settings: uniform,
  Zipfian, and `hotset-90-10` where 10% of sessions receive about 90% of
  operations.
- Include noisy-neighbor multi-tenant cells that report tenant-level p99, not
  only aggregate throughput.
- Treat current `record_search` as scalar/tag top-K, not semantic retrieval;
  vector/semantic memory search needs separate cells.

## Results And History

Use [performance notes](performance.md) for newer measurements and current
server defaults. Earlier dedicated-host comparisons, removed wire-listener
profiles, and append-path tuning runs are preserved in the
[benchmark evaluation history](../archive/benchmark-evaluation-history.md).
Their numbers and default-setting descriptions apply only to those runs.

## Questions To Answer

1. How fast is the hot agent loop: append one event, then read enough recent
   context for the next model/tool call?
2. How expensive is session resume over long histories?
3. How expensive is branch/fork creation when an agent explores alternatives?
4. How well do records and indexes support scoped memory search?
5. How do artifact metadata and artifact payload/reference paths affect write
   latency, read latency, and storage growth?
6. What storage and index overhead do MongoDB/Postgres pay to emulate the same
   semantics?
7. Under durable settings, how do p95/p99 latency and throughput change?

Non-goals:

- No generic TPC-C, TPC-H, or document-store benchmark.
- No claim that initial MongoDB/Postgres settings are expert-optimized.
- No managed-service-only feature dependency in the default local benchmark.
- No LLM calls in the benchmark path. Payloads are deterministic fixtures.

## Backends

| Backend | Purpose |
|---|---|
| `agentdb-native-rocksdb` | First AgentDB target: native HTTP API with local RocksDB storage. |
| `mongodb-session-doc` | Naive MongoDB model: one document per session with an embedded event array. |
| `mongodb-event-log` | Implemented MongoDB baseline: sessions, events, records, and artifacts collections. |
| `postgres-jsonb-session` | Naive Postgres model: one session row with a JSONB event array. |
| `postgres-event-log` | Reasonable Postgres model: normalized sessions/events/records/artifacts tables. |
| `postgres-event-log-vector` | Optional tuned Postgres model with `pgvector`, full-text indexes, and JSONB indexes. |

MongoDB Community Edition does not provide the same local vector-search surface
as Atlas Vector Search. The default local MongoDB benchmark should therefore
measure exact scalar/text/tag search and mark vector search as unsupported,
unless a separate Atlas-backed profile is explicitly configured.

Later AgentDB profiles should be added separately instead of folding them into
the RocksDB baseline:

| Future Profile | Purpose |
|---|---|
| `agentdb-native-slate-local` | Local filesystem object-store path for SlateDB/tiered-storage experiments. |
| `agentdb-native-s3` | Remote object-store profile for artifact/tiered-storage cost and latency. |
| `agentdb-native-raft-rocksdb` | Multi-node Raft quorum with local RocksDB per node. |

## Durability Profiles

Run every benchmark under at least one explicit durability profile.

| Profile | AgentDB | MongoDB | Postgres | Use |
|---|---|---|---|---|
| `single-node-wal` | RocksDB WAL plus memtable, no forced media sync per request | Standalone acknowledged writes without `journal=true` | `synchronous_commit=off` | Explicit opt-out of media sync for fast-path comparison. |
| `single-node-fsync` | RocksDB sync writes with safe append group commit | Journaled acknowledged writes | `synchronous_commit=on` | Host-crash durable single-node sanity comparison. |
| `strict-fsync` | RocksDB sync writes with append group commit disabled | Journaled acknowledged writes | `synchronous_commit=on` | Stress cell for per-operation fsync cost; not the default comparison. |
| `durable-quorum` | Raft quorum write ack | Replica set `writeConcern=majority`, journaled | Sync replication when configured | Real safety comparison on dedicated benchmark or cluster test hardware. |

The server defaults to WAL fsync (`AGENTDB_ROCKSDB_SYNC_WRITES=1`). Reports
about default behavior must use `single-node-fsync`; `single-node-wal` results
must explicitly disclose the durability opt-out. Replicated safety comparisons
should use `durable-quorum` once equivalent AgentDB, MongoDB, and Postgres
clusters are part of the harness.

## Benchmark Host

Use a dedicated benchmark host for reproducible comparison runs. Laptop runs are useful
for smoke testing the harness, but they should not be used for comparison
reports unless the report clearly labels them as laptop/dev runs.

Dedicated benchmark host baseline observed on 2026-05-10:

| Field | Value |
|---|---|
| OS | Ubuntu 22.04 kernel `6.8.0-90-generic` |
| Architecture | `x86_64` |
| CPU | Intel Core i7-14700KF |
| CPU topology | 1 socket, 20 cores, 28 hardware threads |
| Memory | 31 GiB total |
| Docker | 29.4.1 |
| Docker Compose | v5.1.3 |
| Rust | `rustc 1.95.0`, `cargo 1.95.0` |
| Root/home disk | `/dev/nvme0n1p5`, 281 GiB total, about 29 GiB free at observation time |

The harness must capture the current host manifest at run time instead of
relying on this static table. The table is only a starting point.

Recommended dedicated-host rules:

- Run from a clean checkout of latest `main`.
- Keep database data, logs, and reports outside the repo under a dedicated
  benchmark output directory.
- Refuse full comparison runs if free disk is below a configurable threshold,
  default 80 GiB. Allow smoke runs with a lower threshold.
- Keep only the latest N full runs by default and compress or delete older raw
  logs.
- Do not run nightly stability tests and full benchmarks at the same time.

## Workloads

Each workload runs the same deterministic operation stream against each backend.
Workload generators must use a fixed seed and write the seed into the report.
The tool must support a `prepare` phase that creates agents, sessions, chat
history, records, and artifacts before a measured `run` phase.

| Workload | Operation Shape | Why It Matters |
|---|---|---|
| `append_only` | Append a message/tool event and return only the append acknowledgement. | Isolates write sequencing, durability, and index-update cost from tail-read/context serialization. |
| `append_fetch_hot` | Append a message/tool event, then fetch the latest N events from the same session. | Main agent runtime loop. Measures read-after-write in the same client/session. |
| `long_context_resume` | Load sessions with 100, 1k, 10k, and 100k events, then fetch recent context and selected checkpoints. | Agents often resume long-running tasks. Full-history scans should not be required. |
| `fork_branch` | Create branches from a parent session at a sequence number, append divergent events, then read parent and child context. | Agent planning, retries, and speculative execution need cheap branching. |
| `fork_only` | Create branches from existing sessions without extra mixed operations. | Isolates branch metadata/index cost from append and fetch cost. |
| `memory_write_search` | Write records with tags, text, optional embeddings, and optional sharding key; query both scoped and global search. | Measures local index, global index, eventual visibility, and tenant isolation. |
| `record_write_only` | Write searchable memory records only. | Isolates record primary row, namespace index, and global-index outbox write cost. |
| `record_search_only` | Query searchable memory records only. | Isolates local/global search index read cost without write pressure. |
| `artifact_pointer` | Write event metadata plus artifact metadata and payload/reference records. | Agent runs frequently produce files, screenshots, traces, and tool outputs. |
| `artifact_only` | Write artifact metadata/content references only. | Isolates artifact metadata persistence and content-engine write cost. |
| `mixed_agent_loop` | Weighted mix of append, fetch, resume, record write, search, fork, and artifact metadata writes. | End-to-end workload that approximates a real agent service. |

Recommended default mix for `mixed_agent_loop`:

| Operation | Weight |
|---|---:|
| Append event | 35% |
| Fetch recent context | 25% |
| Append and fetch in one logical step | 15% |
| Write memory record | 8% |
| Scoped record search | 7% |
| Global record search | 4% |
| Fork branch | 3% |
| Artifact metadata write/read | 3% |

## Dataset Generator

`agentdb-bench` should behave more like `sysbench` or TPC-C than a fixed demo:
users prepare a synthetic dataset with parameters, then run one or more workload
profiles against that dataset.

Recommended CLI shape:

```bash
agentdb-bench prepare \
  --backend agentdb-native-rocksdb \
  --profile default-agent-runtime \
  --tenants 100 \
  --agents-per-tenant 10 \
  --sessions-per-agent 20 \
  --events-per-session 1000 \
  --records-per-session 50 \
  --artifact-rate 0.05 \
  --artifact-small-bytes 4096 \
  --artifact-large-bytes 1048576 \
  --artifact-mode reference \
  --payload-bytes 1024 \
  --seed 42

agentdb-bench run \
  --backend agentdb-native-rocksdb \
  --workload mixed_agent_loop \
  --concurrency 1,4,16,64 \
  --session-selection zipf \
  --warmup 15s \
  --duration 60s \
  --out bench-out/
```

The `prepare` phase should be idempotent for a given run ID. It should either
create a fresh isolated dataset namespace or fail clearly if the target dataset
already exists and `--reset` was not provided.

Dataset knobs:

| Parameter | Meaning |
|---|---|
| `--tenants` | Synthetic tenant count used internally for all backends. |
| `--agents-per-tenant` | Number of agents per tenant. |
| `--sessions-per-agent` | Number of sessions per agent. |
| `--events-per-session` | Chat/event history depth per prepared session. |
| `--event-kind-mix` | Distribution of user, assistant, tool-call, tool-result, summary, and error events. |
| `--payload-bytes` | Default event payload size. |
| `--payload-size-dist` | Fixed, uniform, lognormal, or Pareto payload distribution. |
| `--records-per-session` | Number of memory records attached to each session. |
| `--global-record-ratio` | Ratio of records without a session/sharding key, used to exercise global index paths. |
| `--artifact-rate` | Fraction of events that create an artifact. |
| `--artifact-small-bytes` | Size for small artifacts stored through the content engine. |
| `--artifact-large-bytes` | Size for large artifact payloads. |
| `--artifact-mode` | `reference`, `content-backend`, or `mixed`; `content-backend` writes bytes through AgentDB's content engine, not RocksDB. |
| `--forks-per-parent` | Number of prepared child branches per selected parent session. |
| `--seed` | Deterministic data generation seed. |

Run knobs:

| Parameter | Meaning |
|---|---|
| `--session-selection` | `uniform`, `zipf`, or `hotset-90-10` fixture selection during the measured run. Use skewed modes for publishable comparisons. |
| `--zipf-exponent` | Zipfian skew exponent for `--session-selection zipf`; default is `1.2`. |
| `--fetch-last-n` | Tail size returned by append/fetch and fetch-recent operations. |
| `--warmup` / `--duration` | Warmup and measured windows. Public comparisons should use multi-minute measured windows. |
| `tools/agentdb-bench/scripts/agentdb-bench-repeat.sh` | Runs the selected backend wrapper once per seed with clean run IDs, then aggregates report JSON files into `bench-out/summaries/<summary-id>/summary.md`. |

Skewed session selection is only meaningful with a sufficiently large fixture
set. The `smoke` profile has too few sessions to exercise Zipfian or hotset
behavior; use `default-agent-runtime` or explicit overrides with hundreds to
thousands of sessions. Current skew is applied across the flattened
tenant-agent-session fixture list, so multi-tenant noisy-neighbor studies need
a later tenant-aware selector that applies hot-session skew independently per
tenant and reports tenant-level p99.

Repeat-run controls should use fresh run IDs per seed instead of looping inside
one prepared dataset. Reusing one mutated dataset lets earlier repeats change
session heads, event counts, and cache state for later repeats. The repeat
script preserves the existing backend wrappers and sets `RUN_ID=<base>-rN-sSEED`
plus `SEED=SEED` for each invocation before calling
`agentdb-bench summarize-repeats`.

Example 3x Zipf validation cell:

```bash
BENCH_SCRIPT=tools/agentdb-bench/scripts/agentdb-bench-local.sh \
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
  tools/agentdb-bench/scripts/agentdb-bench-repeat.sh
```

Run the same command with
`BENCH_SCRIPT=tools/agentdb-bench/scripts/agentdb-bench-mongodb.sh` and
`BENCH_SCRIPT=tools/agentdb-bench/scripts/agentdb-bench-postgres.sh` before treating the Zipf row as
a comparison cell.

Predefined profiles should be thin wrappers around these parameters:

| Profile | Purpose |
|---|---|
| `smoke` | Small, fast dataset for local correctness checks. |
| `default-agent-runtime` | Balanced chat history, memory, fork, and artifact dataset. |
| `working-set-exceeds-cache` | Large logical history and memory dataset for cache-pressure validation. Reports total sessions/events/records and estimated event payload bytes. |
| `long-context` | Deep session history with fewer tenants. |
| `artifact-heavy` | High artifact rate and larger payload/reference metadata. |
| `memory-heavy` | More records per session and more global search pressure. |
| `fork-heavy` | More branches per parent session. |

## Data Model Requirements

The benchmark should model the product semantics directly:

- A tenant is implicit in auth/user context for AgentDB. The benchmark harness
  still records a synthetic tenant ID internally so MongoDB/Postgres schemas can
  enforce equivalent isolation.
- Records are tenant-scoped and can optionally include a session or sharding key.
- Reads in the same benchmark worker must observe its previous acknowledged
  writes.
- Global search can be eventually consistent, but each backend must report
  visibility lag or the time spent waiting for the expected write token.
- Forked sessions must isolate child writes from parent reads.
- Artifact data should be split into metadata and payload/reference paths so the
  benchmark does not force every backend to store large blobs inline.

Artifact semantics:

- Small artifact mode stores metadata in the database and content through the
  backend's content/blob path when the backend supports it.
- Large artifact reference mode stores metadata plus `content_uri`, `size`,
  `sha256`, `media_type`, and `created_at`.
- Large artifact content-backend mode is optional and should be reported
  separately because it measures content-engine write/read behavior.
- The default comparison should use reference mode for large artifacts so the
  benchmark does not collapse into a blob-store benchmark.

## MongoDB Schemas

`mongodb-session-doc` is intentionally simple:

```javascript
sessions: {
  _id,
  tenant_id,
  agent_id,
  session_id,
  fork_parent_id,
  fork_at_seq,
  events: [ { seq, kind, created_at, payload } ],
  updated_at
}
```

Indexes:

```javascript
db.sessions.createIndex({ tenant_id: 1, session_id: 1 }, { unique: true })
db.sessions.createIndex({ tenant_id: 1, agent_id: 1, updated_at: -1 })
```

This schema is useful because many teams start here, but it has known limits:
large document rewrites, array growth, document-size limits, and expensive forks
if the implementation copies event history.

`mongodb-event-log` is the fairer local baseline:

```javascript
sessions:  { tenant_id, agent_id, session_id, fork_parent_id, fork_at_seq, ... }
events:    { tenant_id, session_id, seq, kind, created_at, payload }
records:   { tenant_id, session_id, sharding_key, tags, text, embedding, payload }
artifacts: { tenant_id, session_id, artifact_id, content_uri, media_type, size }
```

Indexes:

```javascript
db.sessions.createIndex({ tenant_id: 1, session_id: 1 }, { unique: true })
db.events.createIndex({ tenant_id: 1, session_id: 1, seq: 1 }, { unique: true })
db.events.createIndex({ tenant_id: 1, session_id: 1, seq: -1 })
db.records.createIndex({ tenant_id: 1, session_id: 1, sharding_key: 1 })
db.records.createIndex({ tenant_id: 1, tags: 1 })
db.records.createIndex({ tenant_id: 1, tags: 1, created_at: -1 })
db.records.createIndex({ tenant_id: 1, text: "text" })
```

Repo-local runner:

```bash
PROFILE=smoke tools/agentdb-bench/scripts/agentdb-bench-mongodb.sh
```

The implemented runner accepts the same dataset knobs as the AgentDB/Postgres
runners, plus `MONGO_PORT`, `MONGO_URI`, and `MONGO_DB`. It uses reference-mode
artifact metadata by default so the comparison measures agent runtime metadata
paths instead of large blob storage.

Fork behavior should be reported in two modes if both are implemented:

| Mode | Behavior |
|---|---|
| `copy` | Copy visible parent events into the child. Simple but expected to scale poorly. |
| `overlay` | Store parent pointer plus child-only events. Closer to AgentDB semantics, but application logic is more complex. |

## Postgres Schemas

`postgres-jsonb-session` is the naive schema:

```sql
CREATE TABLE sessions_jsonb (
  tenant_id text NOT NULL,
  agent_id text NOT NULL,
  session_id text NOT NULL,
  fork_parent_id text,
  fork_at_seq bigint,
  events jsonb NOT NULL DEFAULT '[]'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id)
);
```

This mirrors the embedded MongoDB model and exposes the same application-level
tradeoff: simple reads but growing row updates.

`postgres-event-log` is the fairer local baseline:

```sql
CREATE TABLE sessions (
  tenant_id text NOT NULL,
  agent_id text NOT NULL,
  session_id text NOT NULL,
  fork_parent_id text,
  fork_at_seq bigint,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE events (
  tenant_id text NOT NULL,
  session_id text NOT NULL,
  seq bigint NOT NULL,
  kind text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL,
  PRIMARY KEY (tenant_id, session_id, seq)
);

CREATE INDEX events_recent_idx
  ON events (tenant_id, session_id, seq DESC);
```

Records and artifacts should use separate tables with tenant-leading indexes.
The optional vector profile can add `pgvector`, HNSW/IVFFlat indexes, GIN JSONB
indexes, and generated `tsvector` columns. It must be reported separately from
the no-extension baseline.

## Metrics

Each benchmark cell must emit raw JSONL operation records and a summarized JSON
report.

Required latency metrics:

- p50, p90, p95, p99, and max latency per operation type.
- Throughput in successful operations per second.
- Error count and error class.
- Timeout count.
- Read-after-write violation count.
- Duplicate or missing event sequence count.

Required system metrics:

- CPU usage.
- Resident memory.
- Disk bytes written.
- Database size and index size after load.
- WAL/oplog/raft log growth when available.
- AgentDB index lag and record-index worker backlog.
- MongoDB and Postgres connection pool saturation when available.

Required metadata:

- `agentdb-bench` git SHA.
- AgentDB git SHA.
- Hostname.
- Hardware: CPU model, sockets, cores, hardware threads, NUMA nodes, cache
  summary, memory total/available, disk model when available, filesystem, and
  free disk before/after the run.
- OS: kernel, distribution, architecture, cgroup version, and relevant sysctl
  settings when available.
- Runtime: Rust version, Docker version, Docker Compose version, container image
  tags, and benchmark binary version.
- Database versions and exact server parameters that affect durability,
  journaling, fsync, replication, cache size, and connection limits.
- Workload seed and workload config.
- Docker image tags or binary paths.
- Durability profile.
- Schema profile.
- Warm-up duration and measured duration.

## Correctness Checks

Performance numbers are invalid if correctness checks fail.

The harness must verify:

- A worker can read its own acknowledged writes in the same logical session.
- Event sequences are contiguous per session.
- Concurrent appends do not produce duplicate sequence numbers.
- Forked child reads include parent history up to `fork_at_seq`.
- Forked child writes do not appear in parent reads.
- Tenant-scoped reads never return another tenant's data.
- Record search eventually returns a written record within the configured
  visibility timeout.
- Artifact metadata reads return the exact content reference written.

For AgentDB global index tests, the report should include both:

- Immediate read result after write.
- Time until search observes the write token, up to a configured timeout.

This keeps eventual consistency visible instead of hiding it behind retries.

## Run Matrix

Start small and make the matrix configurable.

Default smoke profile:

| Parameter | Value |
|---|---:|
| Tenants | 4 |
| Agents per tenant | 4 |
| Sessions per agent | 8 |
| Events per preloaded session | 100 |
| Concurrency | 1, 4 |
| Measured duration | 10s per cell |
| Payload sizes | 512 B, 4 KiB |

Default comparison profile:

| Parameter | Value |
|---|---:|
| Tenants | 100 |
| Agents per tenant | 10 |
| Sessions per agent | 20 |
| Events per preloaded session | 1k |
| Long-session sizes | 1k, 10k, 100k |
| Concurrency | 1, 4, 16, 64, 128 |
| Warm-up | 15s |
| Measured duration | 60s per cell |
| Payload sizes | 512 B, 4 KiB, 32 KiB |

Scale profiles should vary one dimension at a time:

- Number of tenants.
- Events per session.
- Payload size.
- Records per tenant.
- Branches per parent session.
- Global-index lag under write load.

## Implementation Plan

### PR 1: Harness Skeleton

- Add `tools/agentdb-bench` as a Rust CLI so the benchmark builds with the
  existing workspace and does not introduce a new Go dependency. Initial status:
  implemented for `agentdb-native-rocksdb`.
- Add backend trait, workload trait, config parsing, deterministic ID/payload
  generation, histogram collection, JSONL output, and summary JSON output.
  Initial status: implemented as a first native-AgentDB harness; generic
  backend traits are still follow-up work. Raw JSONL and per-operation latency
  summaries are implemented.
- Add `prepare` and `run` subcommands. `prepare` owns synthetic dataset
  creation for tenants, agents, sessions, chat history, records, forks, and
  artifacts. Initial status: implemented for tenants, agents, sessions, chat
  history, records, forks, and content-engine artifacts. Metadata-only external
  artifact references need a runtime/API shape before they can be represented as
  native artifact metadata.
- Add Docker Compose files for local Postgres and MongoDB.
- Add schema files for the MongoDB and Postgres profiles above.
- Add benchmark wrapper scripts under `tools/agentdb-bench/scripts/` to build
  AgentDB, start databases, run smoke matrices, and write `bench-out/`.
  Initial status: local AgentDB-only runner is implemented as
  `tools/agentdb-bench/scripts/agentdb-bench-local.sh`; MongoDB/Postgres
  orchestration is still follow-up work.

### PR 2: Backend Adapters And Correctness Tests

- Implement `agentdb-native`, `mongodb-session-doc`, `mongodb-event-log`,
  `postgres-jsonb-session`, and `postgres-event-log`.
  Current status: `agentdb-native-rocksdb`, `mongodb-event-log`, and
  `postgres-event-log` are implemented; the `agentdb-mongodb-api` profile was
  removed with the wire listener. MongoDB session-doc and Postgres
  JSONB-session remain follow-up work.
- Start AgentDB with local RocksDB storage for the first AgentDB profile.
- Add adapter conformance tests for dataset prepare, append, fetch, fork,
  records, search, artifact metadata, and artifact payload/reference behavior.
- Gate benchmark summaries on correctness passing.

### PR 3: Workloads And Reports

- Implement the six workloads in this document.
- Add raw JSONL, summary JSON, and Markdown report generation.
- Add a compact terminal summary for local runs.
- Add optional static HTML output only after the JSON report format is stable.

### PR 4: Dedicated Benchmark Host Profile

- Add a dedicated-host profile that runs from a clean checkout of latest `main`.
- Keep benchmark data outside the repo under a dedicated benchmark output
  directory.
- Capture machine metadata, DB logs, AgentDB logs, and benchmark reports.
- Add a disk-space guard and retention cleanup for old benchmark runs. Current
  scripts include `tools/agentdb-bench/scripts/agentdb-bench-prune.sh` for dry-run-first pruning of
  direct child run directories.
- Wire a nightly or manually triggered run separately from the existing
  stability test so long benchmark runs do not block correctness CI.

### PR 5: Replicated Durability Profile

- Add AgentDB `durable-quorum` runs against the real multi-node cluster.
- Add MongoDB replica-set and Postgres replication profiles if they are part of
  the comparison.
- Report failover behavior separately from steady-state performance.

## Output Layout

Recommended output directory:

```text
bench-out/
  runs/<timestamp>/
    config.json
    host-manifest.json
    software-manifest.json
    database-config.json
    raw.jsonl
    summary.json
    report.md
    agentdb.log
    postgres.log
    mongodb.log
```

`summary.json` should be stable enough for dashboards and trend comparisons.
`raw.jsonl` can evolve faster as long as every row includes a schema version.

`report.md` must start with a "Run Configuration" section that includes:

- Hostname and hardware summary.
- OS and kernel.
- AgentDB commit and benchmark commit.
- MongoDB/Postgres versions and image tags.
- AgentDB storage, raft, and index settings.
- MongoDB write concern, journaling, cache, and replica-set settings.
- Postgres `synchronous_commit`, WAL, checkpoint, shared buffer, and extension
  settings.
- Durability profile and schema profile.
- Workload seed, duration, concurrency, payload sizes, and preload size.
- Free disk before and after the run.

## Reporting Rules

- Never compare a naive schema against AgentDB without also showing the fairer
  event-log schema.
- Never combine local vector-capable Postgres and vector-limited MongoDB into a
  single "search winner" chart without calling out feature differences.
- Always separate `single-node-wal`, `single-node-fsync`, and
  `durable-quorum` runs.
- Always publish correctness failures before latency numbers.
- Always label AgentDB native API and AgentDB MongoDB API runs separately.

## Open Risks

- MongoDB and Postgres can be tuned many ways. The initial harness should make
  schema and settings inspectable so experts can improve the baselines later.
- Fork semantics are not native in MongoDB/Postgres. Both copy and overlay
  implementations are valid baselines, but they measure different application
  tradeoffs.
- Vector search is not symmetric across local MongoDB, Postgres, and AgentDB.
  Treat vector search as an optional profile, not the default comparison.
- A colocated benchmark runner can hide network costs. The first reports should
  state whether the client and database run on the same host.
- AgentDB async global index behavior needs explicit visibility-lag reporting,
  otherwise retries can accidentally hide the consistency model.
