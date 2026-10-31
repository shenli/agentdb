# Benchmark Evaluation History

Historical dedicated-host runs and tuning notes extracted from the benchmark
design. “Current”, “latest”, and default-setting statements below describe the
recorded runs, not today's server defaults. Some profiles used the removed
MongoDB wire listener. These results are internal evaluation evidence, not
publishable comparative claims.

Use the [benchmark methodology](../operations/agentdb-bench.md) for workload and
reporting requirements and [performance notes](../operations/performance.md) for
newer measurements and current durability defaults. In particular, the server
now defaults to WAL fsync; historical `single-node-wal` runs explicitly used
weaker durability.

## Recorded Dedicated-Host Results

These are the current single-node local RocksDB results from a dedicated
benchmark host after the
append-path lock narrowing, MongoDB-API recent-fetch changes, append
group-commit profiling/fixes, metadata-only policy fast-pathing, local-route
forwarding-body laziness, and local RocksDB default tuning. They use release
binaries, local
RocksDB metadata/index storage, local filesystem content storage, 1 tenant, 1
agent, 16-320 sessions depending on the run, 3s warmup, and 8-10s measured
duration depending on the sweep.

| Stage | Representative result | Notes |
|---|---:|---|
| Initial AgentDB append-fetch hot path | 3.4k ops/s at c=32 | Pre-lock-narrowing baseline. |
| Post lock narrowing / append-path fixes | 14.0k ops/s at c=32 | First major append-fetch improvement. |
| Mixed-path/state-lock cleanup | 41.2k ops/s at c=32 | Reduced duplicate state-lock and route serialization work. |
| Event-source/session fast path | 52.9k ops/s at c=32 | Avoided warm-session full-row writes. |
| Policy fast path and RocksDB defaults | 56.0k ops/s at c=32 | Current short-run mixed-agent-loop peak in this history. |
| MongoDB normalized event-log baseline | 26.2k ops/s at c=32 | Same synthetic mixed-agent-loop shape. |
| Postgres normalized event-log baseline | 5.3k ops/s at c=32 | Same synthetic mixed-agent-loop shape. |

Compared with the pre-fix local RocksDB runs, `append-fetch-hot` at c=32 moved
from roughly 3.4k ops/s to 14.0k ops/s, and `mixed-agent-loop` at c=32 moved
from roughly 7.8k ops/s to 16.8k ops/s. These are not product claims yet:
MongoDB/Postgres comparison runs, longer durations, and durable quorum profiles
must be reported separately.

The historical `agentdb-mongodb-api` profile (removed along with the MongoDB
wire listener) measured adapter overhead through that listener; its recorded
numbers below are kept as an evaluation record.

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

The maintained Python and TypeScript SDKs now match this measured default path:
they send `Idempotency-Key` only when the caller supplies a stable key. Ordinary
appends are therefore coalescing-eligible, while explicitly idempotent retries
retain replay semantics and intentionally bypass same-session coalescing. The
append-only coalescing result is therefore reachable through default SDK calls,
not only through the Rust benchmark client.

The final mixed-path run also avoids a second state-lock acquisition for policy
resolution, returns append-and-fetch hot tails from the append commit path,
reuses the committed state for route backpressure headers, and relies on the
persisted event stream to recover warm-session `next_seq` instead of writing
the full session row on every warm append. The latest run also skips full
policy merge/deserialization for metadata-only tenant policy JSON, avoids eager
forwarding-body serialization for local session mutations, and opens RocksDB
with larger local write buffers/background jobs. On the dedicated benchmark host,
the latest
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
with 3 seeds, 10s warmup, and 60s measured windows.

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

The latest dedicated-host Zipf-1.2 c=32 single-seed sanity run used 1000 sessions
(10 tenants x 2 agents x 50 sessions), 10s warmup, and a 60s measured window.
AgentDB uses a zero same-session coalescing wait in this profile; a 5ms wait
was useful for fsync grouping but cut local WAL throughput to 8.4k ops/s.

Results are from the dedicated benchmark host.

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

Results are from the dedicated benchmark host.

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

Results are from the dedicated benchmark host.

| Backend/profile | ops/s | p95 | p99 | AgentDB ratio | Notes |
|---|---:|---:|---:|---:|---|
| AgentDB `single-node-fsync`, initial | 256 | 858ms | 949ms | 0.29x MongoDB / 0.12x Postgres | RocksDB sync writes plus safe append group commit. |
| AgentDB `single-node-fsync`, tuned c=32 | 1,441 | 47ms | 75ms | 1.63x MongoDB / 0.66x Postgres | Compact append log, shared Persist fsync batching, zero same-session coalescing wait. |
| AgentDB `single-node-fsync`, group wait 2ms c=32 | 1,472 | 48ms | 76ms | not rerun | Safer default fsync group-commit window; improves throughput over the previous 1ms default on the smaller 4x2x25-session tuning cell. |
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

Follow-up fsync group-commit tuning on the dedicated benchmark host used a smaller c=32 Zipf-1.2
cell (4 tenants, 2 agents/tenant, 25 sessions/agent, 50 prepared
events/session, 10 records/session, 2 KiB payload, 15s measured window) to
sweep `AGENTDB_APPEND_GROUP_COMMIT_US`:

| Wait window | ops/s | p95 | p99 | Notes |
|---:|---:|---:|---:|---|
| 0us | 1,082 | 77ms | 117ms | Too little safe batching. |
| 250us | 1,097 | 71ms | 117ms | Slight throughput gain only. |
| 500us | 1,116 | 70ms | 111ms | Better, still below the default. |
| 1000us | 1,179 | 61ms | 97ms | Previous default. |
| 2000us | 1,472 | 48ms | 76ms | Best latency/throughput balance in this sweep. |
| 4000us | 1,509 | 54ms | 86ms | Slightly higher throughput, worse p99. |
| 8000us | 1,237 | 66ms | 105ms | Over-batched; regresses. |

Based on that sweep, AgentDB's default `single-node-fsync` append group-commit
window is 2ms. Operators can still override it with
`AGENTDB_APPEND_GROUP_COMMIT_US`.

Append-path instrumentation should stay in the workflow before further durable
write tuning: per-session queue/lock wait, group-commit queue wait, RocksDB
sync-write time, and state update time. If RocksDB sync dominates, the likely
fix is a more explicit durable log/group-commit design; if queue wait dominates,
a single-writer per-session coalescing actor is the stronger candidate.

Enable append/API profiling for AgentDB native or AgentDB MongoDB-API wrapper
runs with:

```bash
APPEND_PATH_PROFILE_MS=5000 tools/agentdb-bench/scripts/agentdb-bench-local.sh
AGENTDB_API_JSON_PROFILE_MS=5000 tools/agentdb-bench/scripts/agentdb-bench-local.sh
```

The server log then emits low-overhead aggregate lines:

| Log line | What it separates |
|---|---|
| `append path profile` | Runtime refresh, per-session commit-lock wait, initial state-lock wait/work, durable write, post-write state-lock wait/work, and total append latency. |
| `adaptive write profile` | Direct versus grouped batches, requests/ops per batch, group-commit queue wait, and KV write time. |
| `api json response profile` | JSON response count, response bytes, average response size, and average serialization time. |

The profiling is off by default. Wrapper scripts set `RUST_LOG=info` only when
`APPEND_PATH_PROFILE_MS` or `AGENTDB_API_JSON_PROFILE_MS` is present so normal
benchmark logs stay compact.

The first profiled `single-node-fsync` Zipf c=32 run on the dedicated benchmark host used:

```bash
APPEND_PATH_PROFILE_MS=5000 \
  DURABILITY_PROFILE=single-node-fsync \
  SESSION_SELECTION=zipf \
  ZIPF_EXPONENT=1.2 \
  CONCURRENCY=32 \
  WARMUP=10s \
  DURATION=60s \
  tools/agentdb-bench/scripts/agentdb-bench-local.sh
```

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
5ms coalescing window, the same dedicated-host durable Zipf c=32 cell improved from
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

### Local WAL profile follow-up

After the API JSON profiling hook landed, a local `single-node-wal`
`mixed-agent-loop` run was used to identify the next non-fsync bottleneck:

```bash
APPEND_PATH_PROFILE_MS=1000 \
AGENTDB_API_JSON_PROFILE_MS=1000 \
DURABILITY_PROFILE=single-node-wal \
SESSION_SELECTION=zipf \
ZIPF_EXPONENT=1.2 \
CONCURRENCY=32 \
WARMUP=2s \
DURATION=8s \
tools/agentdb-bench/scripts/agentdb-bench-local.sh
```

This local profiled run reached 30.5k ops/s with p99 5.6ms. The absolute
throughput is lower than an unprofiled run because every response is explicitly
serialized for measurement, but the stage attribution is useful:

| Stage | Representative measured-period average | Interpretation |
|---|---:|---|
| API JSON response serialization | 9-12us for ~11KB responses | Not the dominant bottleneck for the current hot loop. |
| Adaptive KV write | 25-42us | WAL-mode storage writes are small relative to state contention. |
| Simple append durable write | 55-130us | Noticeable, but usually below state-lock wait. |
| Simple append prepare state-lock wait | 220-280us | Major contention point. |
| Simple append update state-lock wait | 210-277us | Major contention point. |

Two tempting local-WAL tuning knobs were checked and rejected:

| Experiment | Result | Decision |
|---|---|---|
| `AGENTDB_SESSION_APPEND_BATCH_WAIT_US` sweep at 0/100/250/500us | 250us produced a noisy throughput bump in one 5s cell, but p99 worsened; 100us and 500us regressed. | Do not change the default. A fixed wait window is too workload-sensitive. |
| Holding the global state lock through the short WAL durable write | Regressed to 31.3k ops/s at c=32 in the same local shape. | Do not pursue. Removing one lock acquisition is not enough to offset blocking unrelated sessions. |

The local-WAL bottleneck is therefore the global `State` mutex around hot
session/event state, not JSON serialization or a simple wait-window setting.
The next meaningful implementation should split the hot append state away from
the global metadata/index state, or shard hot session state by session/tablet so
unrelated sessions do not contend on one mutex during append+fetch.

## Early Scaling Findings

The local RocksDB AgentDB benchmark currently exercises a single runtime tablet
and single durable tablet log. The first dedicated-host runs were very fast at low and
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
transitions. On the same dedicated-host shape, append-heavy throughput no longer
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
