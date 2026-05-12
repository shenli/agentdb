# AgentDB Bench Design

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

Current status: the `li-dev` numbers are credible internal microbenchmarks for
the agent-runtime data model, but they should not be used as external
MongoDB/Postgres performance claims yet. The current headline shape is
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

## Latest `li-dev` Results

These are the current single-node local RocksDB results from `li-dev` after the
append-path lock narrowing, MongoDB-API recent-fetch changes, append
group-commit profiling/fixes, metadata-only policy fast-pathing, local-route
forwarding-body laziness, and local RocksDB default tuning. They use release
binaries, local
RocksDB metadata/index storage, local filesystem content storage, 1 tenant, 1
agent, 16-320 sessions depending on the run, 3s warmup, and 8-10s measured
duration depending on the sweep.

| Run | Workload | c=1 | c=4 | c=8 | c=16 | c=32 | Report |
|---|---|---:|---:|---:|---:|---:|---|
| `agentdb-append-lockfix` | `append-fetch-hot` | 5634 ops/s | 10431 ops/s | 13515 ops/s | 13377 ops/s | 13961 ops/s | `/home/lishen/agentdb-bench-runs/agentdb-append-lockfix/runs/agentdb-append-lockfix/report.md` |
| `agentdb-native-single-mixed-v2` | `mixed-agent-loop` | 6268 ops/s | 13960 ops/s | 17204 ops/s | 17706 ops/s | 17429 ops/s | `/home/lishen/agentdb-bench-runs/agentdb-native-single-mixed-v2/runs/agentdb-native-single-mixed-v2/report.md` |
| `agentdb-mongodb-api-append` | `append-fetch-hot` | 5363 ops/s | 6586 ops/s | 14118 ops/s | 12028 ops/s | 11937 ops/s | `/home/lishen/agentdb-bench-runs/agentdb-mongodb-api-append/runs/agentdb-mongodb-api-append/report.md` |
| `agentdb-mongodb-api-single-mixed-v2` | `mixed-agent-loop` | 4505 ops/s | 6058 ops/s | 15041 ops/s | 12651 ops/s | 12467 ops/s | `/home/lishen/agentdb-bench-runs/agentdb-mongodb-api-single-mixed-v2/runs/agentdb-mongodb-api-single-mixed-v2/report.md` |
| `agentdb-adaptive-group-append-sweep` | `append-fetch-hot` | 9459 ops/s | n/a | 23467 ops/s | 27354 ops/s | 28035 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-adaptive-group-append-sweep/runs/agentdb-adaptive-group-append-sweep/report.md` |
| `agentdb-adaptive-group-mixed` | `mixed-agent-loop` | 9766 ops/s | n/a | 19143 ops/s | 25492 ops/s | 24242 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-adaptive-group-mixed/runs/agentdb-adaptive-group-mixed/report.md` |
| `agentdb-defaultgroup-append-only-sweep` | `append-only` | 11442 ops/s | n/a | 27082 ops/s | 32484 ops/s | 34120 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-defaultgroup-append-only-sweep/runs/agentdb-defaultgroup-append-only-sweep/report.md` |
| `agentdb-defaultgroup-append-fetch-hot-sweep` | `append-fetch-hot` | 10103 ops/s | n/a | 23740 ops/s | 28243 ops/s | 28286 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-defaultgroup-append-fetch-hot-sweep/runs/agentdb-defaultgroup-append-fetch-hot-sweep/report.md` |
| `agentdb-defaultgroup-mixed-agent-loop-sweep` | `mixed-agent-loop` | 10878 ops/s | n/a | 19719 ops/s | 24491 ops/s | 24321 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-defaultgroup-mixed-agent-loop-sweep/runs/agentdb-defaultgroup-mixed-agent-loop-sweep/report.md` |
| `agentdb-mimalloc-hotcache-append-only-sweep` | `append-only` | 12033 ops/s | n/a | 26874 ops/s | 34484 ops/s | 35249 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mimalloc-hotcache-append-only-sweep/runs/agentdb-mimalloc-hotcache-append-only-sweep/report.md` |
| `agentdb-mimalloc-hotcache-append-fetch-hot-sweep` | `append-fetch-hot` | 10138 ops/s | n/a | 24702 ops/s | 30354 ops/s | 31613 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mimalloc-hotcache-append-fetch-hot-sweep/runs/agentdb-mimalloc-hotcache-append-fetch-hot-sweep/report.md` |
| `agentdb-mimalloc-hotcache-mixed-agent-loop-sweep` | `mixed-agent-loop` | 11692 ops/s | n/a | 21695 ops/s | 28290 ops/s | 25292 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mimalloc-hotcache-mixed-agent-loop-sweep/runs/agentdb-mimalloc-hotcache-mixed-agent-loop-sweep/report.md` |
| `agentdb-mixedpaths-append-only-sweep` | `append-only` | 12301 ops/s | n/a | 28010 ops/s | 34684 ops/s | 36592 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mixedpaths-append-only-sweep/runs/agentdb-mixedpaths-append-only-sweep/report.md` |
| `agentdb-mixedpaths-append-fetch-hot-sweep` | `append-fetch-hot` | 10938 ops/s | n/a | 25140 ops/s | 30746 ops/s | 31614 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mixedpaths-append-fetch-hot-sweep/runs/agentdb-mixedpaths-append-fetch-hot-sweep/report.md` |
| `agentdb-mixedpaths-mixed-agent-loop-sweep` | `mixed-agent-loop` | 11266 ops/s | n/a | 32935 ops/s | 40515 ops/s | 41236 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mixedpaths-mixed-agent-loop-sweep/runs/agentdb-mixedpaths-mixed-agent-loop-sweep/report.md` |
| `agentdb-mixedpaths-record-write-only-sweep` | `record-write-only` | 11535 ops/s | n/a | 21602 ops/s | 28966 ops/s | 35192 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mixedpaths-record-write-only-sweep/runs/agentdb-mixedpaths-record-write-only-sweep/report.md` |
| `agentdb-mixedpaths-artifact-only-sweep` | `artifact-only` | 11534 ops/s | n/a | 26054 ops/s | 32596 ops/s | 34523 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-mixedpaths-artifact-only-sweep/runs/agentdb-mixedpaths-artifact-only-sweep/report.md` |
| `agentdb-final-eventsource-session-mixed-sweep` | `mixed-agent-loop` | 10812 ops/s | n/a | 39396 ops/s | 49686 ops/s | 52920 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-final-eventsource-session-mixed-sweep/runs/agentdb-final-eventsource-session-mixed-sweep/report.md` |
| `agentdb-final-policyfast-mixed-sweep` | `mixed-agent-loop` | 11714 ops/s | n/a | 39964 ops/s | 52085 ops/s | 55834 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-final-policyfast-mixed-sweep/runs/agentdb-final-policyfast-mixed-sweep/report.md` |
| `agentdb-rocks-defaults-append-sweep` | `append-only` | 14577 ops/s | n/a | 38514 ops/s | 44801 ops/s | 48985 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-rocks-defaults-append-sweep/runs/agentdb-rocks-defaults-append-sweep/report.md` |
| `agentdb-rocks-defaults-appendfetch-sweep` | `append-fetch-hot` | 11836 ops/s | n/a | 33513 ops/s | 38569 ops/s | 41097 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-rocks-defaults-appendfetch-sweep/runs/agentdb-rocks-defaults-appendfetch-sweep/report.md` |
| `agentdb-rocks-defaults-mixed-sweep` | `mixed-agent-loop` | 11819 ops/s | n/a | 40858 ops/s | 52422 ops/s | 55998 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/agentdb-rocks-defaults-mixed-sweep/runs/agentdb-rocks-defaults-mixed-sweep/report.md` |
| `mongodb-append-only-fixed` | `append-only` | 5303 ops/s | n/a | 6173 ops/s | 20477 ops/s | 19553 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/mongodb-append-only-fixed/runs/mongodb-append-only-fixed/report.md` |
| `mongodb-mixed-same-shape` | `mixed-agent-loop` | 3526 ops/s | 2808 ops/s | 4846 ops/s | 24217 ops/s | 25603 ops/s | `/home/lishen/agentdb-bench-runs/mongodb-mixed-same-shape/runs/mongodb-mixed-same-shape/report.md` |
| `mongodb-final-mixed-current-shape` | `mixed-agent-loop` | 3867 ops/s | n/a | 6357 ops/s | 25460 ops/s | 26186 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/mongodb-final-mixed-current-shape/runs/mongodb-final-mixed-current-shape/report.md` |
| `postgres-final-mixed-current-shape` | `mixed-agent-loop` | 449 ops/s | n/a | 2106 ops/s | 3776 ops/s | 5334 ops/s | `/media/54943193-d308-4802-b135-7293aec29a1b/agentdb-bench-runs/postgres-final-mixed-current-shape/runs/postgres-final-mixed-current-shape/report.md` |

Compared with the pre-fix local RocksDB runs, `append-fetch-hot` at c=32 moved
from roughly 3.4k ops/s to 14.0k ops/s, and `mixed-agent-loop` at c=32 moved
from roughly 7.8k ops/s to 16.8k ops/s. These are not product claims yet:
MongoDB/Postgres comparison runs, longer durations, and durable quorum profiles
must be reported separately.

The `agentdb-mongodb-api` profile now uses a narrower `agentdbFetchRecent`
command instead of the heavier context-building command for the benchmark's
tail-read operation. That raises the mixed workload from roughly 1.3k ops/s to
roughly 12.5k ops/s at c=32, proving the original bad MongoDB-API result was
partly adapter overhead.

The remaining gap is not only the MongoDB wire layer. Perf and app-level timing
showed native HTTP's persistent append path was dominated by the synchronous
RocksDB `write_batch`: append record construction averaged roughly 4us, while
the RocksDB write averaged roughly 140-157us before grouping. Blind RocksDB
write-buffer tuning and a compact session-head experiment did not improve the
profile and were reverted.

Append group commit now coalesces queued append writes under contention while
preserving read-after-write semantics: each caller still awaits the RocksDB
batch that contains its append before returning. The implementation uses an
adaptive fast path: direct write when there is no queue/active writer, queued
group write when contention exists. Adaptive grouping is enabled by default and
can be disabled with `AGENTDB_APPEND_GROUP_COMMIT=0`.

The final mixed-path run also avoids a second state-lock acquisition for policy
resolution, returns append-and-fetch hot tails from the append commit path,
reuses the committed state for route backpressure headers, and relies on the
persisted event stream to recover warm-session `next_seq` instead of writing
the full session row on every warm append. The latest run also skips full
policy merge/deserialization for metadata-only tenant policy JSON, avoids eager
forwarding-body serialization for local session mutations, and opens RocksDB
with larger local write buffers/background jobs. On `li-dev`, the latest
same-shape `mixed-agent-loop` c=32 result is 56.0k ops/s for AgentDB, 26.2k
ops/s for MongoDB, and 5.3k ops/s for Postgres. That is about 2.14x MongoDB and
10.50x Postgres for this synthetic mixed agent loop. This ratio is an internal
single-node microbenchmark result, not an external product claim. A compact
`event_by_id` pointer encoding was tested in `agentdb-compact-eventptr-*` runs
and rejected because it regressed mixed throughput.

### Skew Sensitivity Check

The first skew-sensitivity run used `mixed-agent-loop` at c=32 with the same
dataset shape across backends: 10 tenants, 2 agents per tenant, 50 sessions per
agent, 3 prepared events per session, 2 records per session, 20% artifact rate,
3s warmup, and 10s measured duration. This is a short internal control, not a
publication-grade cell.

| Session selection | AgentDB | MongoDB | Postgres | AgentDB / MongoDB | AgentDB / Postgres | Notes |
|---|---:|---:|---:|---:|---:|---|
| `uniform` | 54,900 ops/s | 25,383 ops/s | 9,601 ops/s | 2.16x | 5.72x | Baseline medium fixture set. |
| `zipf`, exponent `1.2` | 44,224 ops/s | 24,284 ops/s | 2,191 ops/s | 1.82x | 20.18x | AgentDB p99 rose to 5.6ms; Postgres p99 rose to 245ms. Repeat before quoting. |
| `hotset-90-10` | 56,787 ops/s | 24,967 ops/s | 9,525 ops/s | 2.27x | 5.96x | Hotset spreads load across about 100 hot sessions. |

The repeated Zipf validation cell uses the same dataset shape and c=32, but
with 3 seeds, 10s warmup, and 60s measured windows. The combined summary is on
`li-dev` at
`/home/lishen/work/agentdb-bench-runs/zipf-60s/summaries/zipf-60s-combined/summary.md`.

| Backend | Repeats | ops/s mean | ops/s min | ops/s max | ops/s stddev | p95 mean | p99 mean |
|---|---:|---:|---:|---:|---:|---:|---:|
| AgentDB native RocksDB | 3 | 43,219 | 42,980 | 43,487 | 208 | 4.426ms | 5.774ms |
| MongoDB event log | 3 | 26,048 | 25,821 | 26,248 | 175 | 2.099ms | 2.892ms |
| Postgres event log | 3 | 2,178 | 2,175 | 2,180 | 2 | 82.498ms | 238.775ms |

The repeated 60s Zipf ratio is 1.66x MongoDB and 19.84x Postgres. This
confirms the direction of the 10s Zipf cell, but with a lower AgentDB/MongoDB
ratio than the short run. Treat this as a stronger internal comparison cell,
not a publication claim: durability is still not matched across backends,
working set still fits cache, and the selector is still flattened rather than
tenant-aware.

This cell is useful because it falsifies the simple assumption that AgentDB's
advantage always grows under skew. Severe Zipfian skew concentrates writes onto
the earliest sessions in the flattened fixture list and increases per-session
serialization pressure, so AgentDB's MongoDB ratio shrinks from 2.16x to 1.82x.
The `hotset-90-10` result is closer to the uniform result because the hot set is
large enough to keep many session append paths active. MongoDB was comparatively
stable across these short skew cells, while Postgres showed a large tail-latency
regression under Zipfian skew.

The current selector applies skew across the flattened tenant-agent-session
fixture list, so the heaviest sessions land mostly in the first tenant. That is
acceptable for single-node storage-path sensitivity, but not for a noisy-neighbor
claim. The noisy-neighbor benchmark should apply hot-session skew independently
per tenant and report tenant-level throughput and p99.

### Local Single-Node WAL Check

The default local developer profile is `single-node-wal`: AgentDB writes to the
RocksDB WAL and memtable without forcing a per-request media sync. The matched
baselines are MongoDB standalone acknowledged writes without `journal=true` and
Postgres with `synchronous_commit=off`. This profile represents fast local
tryout behavior, not host-crash durable media semantics.

The latest `li-dev` Zipf-1.2 c=32 single-seed sanity run used 1000 sessions
(10 tenants x 2 agents x 50 sessions), 10s warmup, and a 60s measured window.
AgentDB uses a zero same-session coalescing wait in this profile; a 5ms wait
was useful for fsync grouping but cut local WAL throughput to 8.4k ops/s.

Results are on `li-dev` under
`/home/lishen/work/agentdb-bench-runs/single-node-wal`.

| Backend/profile | ops/s | p50 | p95 | p99 | AgentDB ratio | Notes |
|---|---:|---:|---:|---:|---:|---|
| AgentDB `single-node-wal` | 53,947 | 0.49ms | 1.29ms | 2.08ms | 1.00x | RocksDB WAL enabled, no forced per-request media sync. |
| MongoDB `single-node-wal` | 26,017 | 1.50ms | 2.17ms | 2.86ms | 2.07x MongoDB | Standalone acknowledged writes, no `journal=true`. |
| Postgres `single-node-wal` | 13,114 | 0.96ms | 12.51ms | 35.80ms | 4.11x Postgres | `synchronous_commit=off`. |

This is the best current local setup result and should be the first number used
for developer out-of-box performance discussions. It is still a single seed and
cache-warm cell; repeat it before quoting externally.

### Working-Set Cache-Pressure Check

The first `working-set-exceeds-cache` cell uses the same mixed-agent-loop,
Zipf-1.2, c=32 shape, but prepares a larger dataset: 25 tenants x 4 agents x
40 sessions, 500 prepared events per session, 100 records per session, and 4
KiB event payloads. The report estimates 8.2 GB of event payload before
database/index overhead and about 42.0 GB of external artifact references. This
cell is intended to expose storage footprint, cache pressure, and longer
compaction/checkpoint behavior that the cache-warm 1000-session cell hides.

Results are on `li-dev` under
`/home/lishen/work/agentdb-bench-runs/working-set-cache`.

| Backend/profile | State size | ops/s | p50 | p95 | p99 | AgentDB ratio | Notes |
|---|---:|---:|---:|---:|---:|---:|---|
| AgentDB `single-node-wal` | 24 GB | 37,234 | 0.74ms | 1.66ms | 2.35ms | 1.00x | Compact binary tablet log, scoped global-index lookup, RocksDB LZ4 compression, bounded persisted hot-tail cache. |
| MongoDB `single-node-wal` | 11 GB | 23,798 | 1.67ms | 2.09ms | 2.22ms | 1.56x MongoDB | Normalized event-log collections. |
| Postgres `single-node-wal` | not captured | 6,028 | 0.80ms | 30.32ms | 114.35ms | 6.18x Postgres | `record_search` dominates tail latency. |

This cell forced several AgentDB fixes before it could complete and then beat
MongoDB:

- The persistent runtime no longer retains every `event_id` in the in-memory
  map; persisted lookups use the durable secondary index instead.
- `AGENTDB_PERSISTED_HOT_EVENT_CACHE_PER_SESSION` bounds the per-session
  in-memory hot tail. The AgentDB wrappers default it to 64 for this profile.
- RocksDB now defaults to LZ4 compression and a bounded open-file cache. This
  reduced the AgentDB state from a disk-filling run to 32 GB for the resized
  profile.
- The tablet write log now uses a compact binary command encoding instead of
  serializing `Vec<u8>` write batches as JSON arrays and immediately decoding
  them on apply. That removed a large write-path CPU and log-size tax.
- The in-memory global record index now has a `(tenant, namespace)` scope index,
  so tag-only global record searches do not scan unrelated records.

The post-fix result is the first working-set cell where AgentDB beats both
MongoDB and Postgres. A low-overhead `perf` sample after the fix shows the
remaining CPU spread across memory copies/allocation, JSON request/response
serialization and deserialization, RocksDB scans/seeks, LZ4 compression, and
ID parsing. The old dominant tablet-command JSON encode/decode and global-index
full scan are no longer the top bottlenecks.

The before/after size and throughput impact of the compact tablet-log work is
measurable on the same `working-set-exceeds-cache` shape:

| AgentDB state | State size | Throughput | p95 | p99 | Notes |
|---|---:|---:|---:|---:|---|
| Before compact tablet log | 31.65 GiB | 19,545 ops/s | 5.18ms | 9.47ms | JSON-encoded tablet commands plus earlier working-set fixes. |
| After compact tablet log and scoped global index | 24.06 GiB | 37,234 ops/s | 1.66ms | 2.35ms | Binary tablet commands and scoped global-index lookup. |

That is a 7.59 GiB footprint reduction, or 24.0% smaller state, and a 1.90x
throughput increase on this workload. Interpreting throughput as a saturated
server proxy, CPU/work per operation dropped by roughly 47.5%. The tablet-log
codec itself is more compact than the directory-level reduction suggests:
representative single-put tablet commands with a 96-byte key and 1-8 KiB value
encode to about 33-34% of the old JSON command size, because JSON represented
`Vec<u8>` as decimal byte arrays. The full state reduction is smaller because
RocksDB data files, indexes, compression, and non-log records also contribute to
the directory size.

The compact tablet log also helps `single-node-fsync`, but the gain is bounded
by media-sync latency. A comparable c=32, `fetch_last_n=2` steady-state check
improved from the previous 1,441 ops/s, p99 75.0ms to 1,545 ops/s, p99 70.3ms.
That is useful, but not a step-change because the device flush remains on the
critical path.

The follow-up context-sizing fast path was intentionally minor. On a 1000
session, 100 prepared events/session, 20 records/session, 4 KiB payload,
Zipf-1.2, c=32, 60s cell with `FETCH_LAST_N=20`, it moved AgentDB from
42,468 ops/s to 43,226 ops/s (+1.8%) and p99 from 2.07ms to 2.04ms. Keep it as
a cleanup, not a headline result.

### Matched Durability Sanity Check

The first matched-durability sanity cell used the same c=32 Zipf-1.2 60s shape
as the repeated validation cell, but with explicit `single-node-fsync` settings:
AgentDB ran RocksDB sync writes with adaptive append group commit, MongoDB used
`journal=true&w=1`, and Postgres used `synchronous_commit=on`. This is a single
seed (`43`) sanity run, not a publishable comparison.

Results are on `li-dev` under
`/home/lishen/work/agentdb-bench-runs/durable-single-zipf` and
`/home/lishen/work/agentdb-bench-runs/single-node-fsync-sweep`.

| Backend/profile | ops/s | p95 | p99 | AgentDB ratio | Notes |
|---|---:|---:|---:|---:|---|
| AgentDB `single-node-fsync`, initial | 256 | 858ms | 949ms | 0.29x MongoDB / 0.12x Postgres | RocksDB sync writes plus safe append group commit. |
| AgentDB `single-node-fsync`, tuned c=32 | 1,441 | 47ms | 75ms | 1.63x MongoDB / 0.66x Postgres | Compact append log, shared Persist fsync batching, zero same-session coalescing wait. |
| MongoDB `single-node-fsync` | 884 | 61ms | 65ms | 0.29x MongoDB | Journaled acknowledged writes. |
| Postgres `single-node-fsync` | 2,182 | 83ms | 238ms | 0.12x Postgres | `synchronous_commit=on`. |
| AgentDB `strict-fsync` | 124 | 830ms | 1,052ms | stress only | RocksDB sync writes with append group commit disabled. |

This falsifies the assumption that AgentDB automatically wins under matched
local fsync durability just because the data model is simpler. The fast
`single-node-wal` row remains useful for local developer experience, but it is
not a host-crash media-sync claim. The initial profiled `single-node-fsync`
cell showed `append_fetch` p95 at 897ms and p99 at 982ms, while metadata writes
such as `record_write` and `fork` stayed near 53-54ms p95. That pointed at the
durable append path rather than read caching as the bottleneck.

The next PR should add append-path instrumentation before tuning: per-session
queue/lock wait, group-commit queue wait, RocksDB sync-write time, and state
update time. If RocksDB sync dominates, the likely fix is a more explicit
durable log/group-commit design; if queue wait dominates, a single-writer
per-session coalescing actor is the stronger candidate.

Enable append-path profiling for AgentDB native or AgentDB MongoDB-API wrapper
runs with:

```bash
APPEND_PATH_PROFILE_MS=5000 scripts/agentdb-bench-local.sh
```

The server log then emits two low-overhead aggregate lines:

| Log line | What it separates |
|---|---|
| `append path profile` | Runtime refresh, per-session commit-lock wait, initial state-lock wait/work, durable write, post-write state-lock wait/work, and total append latency. |
| `adaptive write profile` | Direct versus grouped batches, requests/ops per batch, group-commit queue wait, and KV write time. |

The profiling is off by default. Wrapper scripts set `RUST_LOG=info` only when
`APPEND_PATH_PROFILE_MS` is present so normal benchmark logs stay compact.

The first profiled `single-node-fsync` Zipf c=32 run on `li-dev` used:

```bash
APPEND_PATH_PROFILE_MS=5000 \
  DURABILITY_PROFILE=single-node-fsync \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  scripts/agentdb-bench-local.sh
```

Run output:
`/home/lishen/work/agentdb-bench-runs/append-path-profile/runs/agentdb-zipf-durable-profiled-r1-s43/report.md`.

The throughput/latency cell was consistent with the prior `single-node-fsync`
run:
255 ops/s, p95 860ms, p99 950ms. The profile attribution changed the diagnosis:

| Stage | Representative measured-period average | Interpretation |
|---|---:|---|
| Per-session commit-lock wait | 170-210ms | Dominant cost under Zipf skew. Hot-session requests wait behind the previous same-session durable append. |
| Durable write inside append path | 31-35ms | Large, but much smaller than same-session lock wait once the workload is hot. |
| Adaptive write queue wait | 15-18ms | Batcher sees some cross-session grouping, but same-session requests cannot coalesce because the session lock is held through the durable write. |
| Adaptive KV write | 16-18ms | RocksDB sync batch cost after grouping. |
| Initial state work + update state work | Usually below 0.1ms combined | State mutation and read-side cache updates are not the bottleneck in this cell. |

This points to same-session durable append serialization, not read caching or
generic state-lock contention. The next optimization should let a per-session
writer own sequencing and coalesce multiple same-session appends into one
durable batch while preserving read-after-write semantics for each caller. A
generic RocksDB tuning pass is lower priority unless a later profile shows KV
write time dominating after same-session batching.

The first same-session coalescing run validates that direction. With simple
same-session `append_and_fetch` calls batched ahead of the durable write and a
5ms coalescing window, the same `li-dev` durable Zipf c=32 cell improved from
255 ops/s to 622 ops/s. Tail latency also dropped from p95 860ms / p99 950ms
to p95 137ms / p99 217ms. This is a 2.4x AgentDB improvement for the matched
`single-node-fsync` cell, but still below the prior MongoDB
`single-node-fsync` sanity cell at 884 ops/s.

The follow-up fsync group-commit tuning sends sync-write requests through the
batch worker instead of letting the first writer bypass batching. With the
default 1ms lower group-commit wait and 5ms same-session coalescing wait, the
same 60s cell reached 648 ops/s with p95 122ms and p99 190ms. Adding the
compact append-log fast path made simple append commits write one durable log
row per coalesced batch, but by itself did not materially change throughput
because the dominant cost was the fsync cadence, not the number of keys in each
batch. Routing ordinary `Persist::write_ops` calls through the same adaptive
batcher changed the result to 1,318 ops/s with p95 57ms and p99 88ms. Removing
the extra same-session coalescing wait improved the c=32 60s cell again to
1,441 ops/s with p95 47ms and p99 75ms.

That safe single-node fsync profile now beats MongoDB's journaled standalone
cell at c=32, but still trails Postgres's `synchronous_commit=on` c=32 cell.
The remaining gap is below the runtime API layer: Postgres is still getting
more useful work per WAL fsync than the current RocksDB-backed Persist batcher.

A high-concurrency c=256 tuning cell under the same Zipf-1.2 shape reached
2,355 ops/s for AgentDB versus 2,127 ops/s for Postgres. That shows AgentDB can
batch enough work to slightly exceed Postgres throughput in this fsync profile,
but it is not a large win: AgentDB p95/p99 were 410ms/615ms versus Postgres
228ms/386ms. The right conclusion is that the current design can approach
Postgres's WAL group-commit efficiency, while a large durable-single-node margin
would require a more specialized append-log/fsync path than RocksDB currently
provides.

The coalescing profile showed same-session batches around 1.4 events per batch
and adaptive KV batches around 8-10 requests per batch. The next remaining
bottleneck is the durable write itself. Further improvement should focus on the
durable log / fsync path and larger safe batching, not read caching.

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
| `agentdb-mongodb-api` | Implemented AgentDB adapter-overhead run through the MongoDB-compatible wire listener. |
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
| `single-node-wal` | RocksDB WAL plus memtable, no forced media sync per request | Standalone acknowledged writes without `journal=true` | `synchronous_commit=off` | Default local developer setup and fast-path comparison. |
| `single-node-fsync` | RocksDB sync writes with safe append group commit | Journaled acknowledged writes | `synchronous_commit=on` | Host-crash durable single-node sanity comparison. |
| `strict-fsync` | RocksDB sync writes with append group commit disabled | Journaled acknowledged writes | `synchronous_commit=on` | Stress cell for per-operation fsync cost; not the default comparison. |
| `durable-quorum` | Raft quorum write ack | Replica set `writeConcern=majority`, journaled | Sync replication when configured | Real safety comparison on `li-dev` or cluster test hardware. |

The default local report should lead with `single-node-wal` because that is how
developers will try the product on one machine. Production safety comparisons
should lead with `durable-quorum` once equivalent replicated MongoDB/Postgres
clusters are part of the harness.

## Benchmark Host

Use `li-dev` as the first reproducible benchmark host. Laptop runs are useful
for smoke testing the harness, but they should not be used for comparison
reports unless the report clearly labels them as laptop/dev runs.

Current `li-dev` baseline observed on 2026-05-10:

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

Recommended `li-dev` rules:

- Run from a clean checkout of latest `main`.
- Keep database data, logs, and reports outside the repo under
  `/home/lishen/agentdb-bench-runs/`.
- Refuse full comparison runs if free disk is below a configurable threshold,
  default 80 GiB. Allow smoke runs with a lower threshold.
- Keep only the latest N full runs by default and compress or delete older raw
  logs.
- Do not run nightly stability tests and full benchmarks at the same time.

## Current Scaling Finding

The local RocksDB AgentDB benchmark currently exercises a single runtime tablet
and single durable tablet log. The first `li-dev` runs were very fast at low and
moderate concurrency, but append-heavy workloads flattened or regressed after
about 8 workers:

| Run | Workload | Dataset | c=1 | c=4 | c=8 | c=16 | c=32 |
|---|---|---|---:|---:|---:|---:|---:|
| `agentdb-isolate-append` | `append-fetch-hot` | 32 sessions | 4382 ops/s | 7854 ops/s | 8243 ops/s | 3754 ops/s | 3839 ops/s |
| `agentdb-append-more-sessions` | `append-fetch-hot` | 320 sessions | 4301 ops/s | 7768 ops/s | 8397 ops/s | 3537 ops/s | 3433 ops/s |

The larger-session run did not improve the high-concurrency collapse, so the
limit was not primarily logical hot-session contention. The immediate bottleneck
was the append path holding the global runtime state mutex while waiting for the
durable RocksDB/replicated-log batch. That serialized unrelated sessions before
storage could make progress.

The `perf(runtime): narrow append state lock` change moves durable append I/O
outside the global state mutex while keeping a per-session commit lock for
same-session sequence ordering, idempotency replay, delete, and lifecycle
transitions. On the same `li-dev` shape, append-heavy throughput no longer
collapses at higher concurrency:

| Run | Workload | Dataset | c=1 | c=4 | c=8 | c=16 | c=32 |
|---|---|---|---:|---:|---:|---:|---:|
| `agentdb-append-lockfix` | `append-fetch-hot` | 320 sessions | 5634 ops/s | 10431 ops/s | 13515 ops/s | 13377 ops/s | 13961 ops/s |

Read-heavy AgentDB workloads also scale: `agentdb-isolate-fetch` reached about
36k ops/s at 16 workers.

MongoDB shows a different curve in the same local setup after adding the
compound record-search index. It is weaker at 1-4 workers but reports higher
throughput at 16-32 workers in this synthetic local driver workload:

| Run | Workload | c=1 | c=2 | c=4 | c=8 | c=16 | c=32 |
|---|---|---:|---:|---:|---:|---:|---:|
| `mongo-sweep-mixed` | `mixed-agent-loop` | 2971 ops/s | 1505 ops/s | 2658 ops/s | 7685 ops/s | 24874 ops/s | 25253 ops/s |
| `mongodb-mixed-same-shape` | `mixed-agent-loop` | 3526 ops/s | n/a | 2808 ops/s | 4846 ops/s | 24217 ops/s | 25603 ops/s |
| `agentdb-sweep-mixed` | `mixed-agent-loop` | 5082 ops/s | 3812 ops/s | 6940 ops/s | 9538 ops/s | 7727 ops/s | 7805 ops/s |
| `agentdb-mixed-lockfix` | `mixed-agent-loop` | 6194 ops/s | n/a | 15322 ops/s | 17652 ops/s | 17063 ops/s | 16761 ops/s |
| `agentdb-native-single-mixed-v2` | `mixed-agent-loop` | 6268 ops/s | n/a | 13960 ops/s | 17204 ops/s | 17706 ops/s | 17429 ops/s |
| `agentdb-mongodb-api-single-mixed-v2` | `mixed-agent-loop` | 4505 ops/s | n/a | 6058 ops/s | 15041 ops/s | 12651 ops/s | 12467 ops/s |

Interpretation: the MongoDB c=16/c=32 result should not be summarized as an
overall MongoDB-vs-AgentDB winner. It means the current standalone MongoDB
event-log baseline, driven directly through the MongoDB driver, scales better
than the current single-node/single-tablet AgentDB profile on this specific
mixed workload at high client concurrency. The AgentDB MongoDB-compatible
listener is no longer the primary explanation for the gap: native HTTP is also
below MongoDB at c=16/c=32.

The next AgentDB performance task should therefore be runtime concurrency and
tablet/cluster distribution, not more protocol-layer tuning. For local
comparison claims, report the single-tablet profile explicitly and add a
separate multi-tablet/cluster profile once the runtime state model and runner
can route a prepared dataset across tablets without rebuilding shared global
state.

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
| `scripts/agentdb-bench-repeat.sh` | Runs the selected backend wrapper once per seed with clean run IDs, then aggregates report JSON files into `bench-out/summaries/<summary-id>/summary.md`. |

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

Run the same command with
`BENCH_SCRIPT=scripts/agentdb-bench-mongodb.sh` and
`BENCH_SCRIPT=scripts/agentdb-bench-postgres.sh` before treating the Zipf row as
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
PROFILE=smoke scripts/agentdb-bench-mongodb.sh
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
- Add `scripts/agentdb-bench.sh` to build AgentDB, start databases, run a smoke
  matrix, and write `bench-out/`. Initial status: local AgentDB-only runner is
  implemented as `scripts/agentdb-bench-local.sh`; MongoDB/Postgres orchestration
  is still follow-up work.

### PR 2: Backend Adapters And Correctness Tests

- Implement `agentdb-native`, `agentdb-mongodb-api`, `mongodb-session-doc`,
  `mongodb-event-log`, `postgres-jsonb-session`, and `postgres-event-log`.
  Initial status: `agentdb-native-rocksdb`, `agentdb-mongodb-api`,
  `mongodb-event-log`, and `postgres-event-log` are implemented. MongoDB
  session-doc and Postgres JSONB-session remain follow-up work.
- Start AgentDB with local RocksDB storage for the first AgentDB profile.
- Add adapter conformance tests for dataset prepare, append, fetch, fork,
  records, search, artifact metadata, and artifact payload/reference behavior.
- Gate benchmark summaries on correctness passing.

### PR 3: Workloads And Reports

- Implement the six workloads in this document.
- Add raw JSONL, summary JSON, and Markdown report generation.
- Add a compact terminal summary for local runs.
- Add optional static HTML output only after the JSON report format is stable.

### PR 4: `li-dev` Benchmark Profile

- Add a `li-dev` profile that runs from a clean checkout of latest `main`.
- Keep benchmark data outside the repo, for example under
  `/home/lishen/agentdb-bench-runs/`.
- Capture machine metadata, DB logs, AgentDB logs, and benchmark reports.
- Add a disk-space guard and retention cleanup for old benchmark runs.
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
