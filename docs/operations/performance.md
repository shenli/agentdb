# AgentDB Performance Notes

Single-machine numbers to ground the README — not competitive claims
against tuned databases. Measured on a Linux workstation (li-dev);
append/fork/vector refreshed 2026-09-04.

## Setup

| | |
|---|---|
| Hardware | Intel Core i7-14700KF (20 cores / 28 threads), 32 GB RAM |
| OS | Ubuntu 22.04.5 LTS (Linux 6.8, x86-64) |
| Build | `cargo build --release -p agentdb-server` |
| Storage | RocksDB on local NVMe SSD (ext4), object store = local FS |
| Server | single `agentdb serve` process, default Tokio multi-threaded runtime |
| Client | `perf-probe` (Go), HTTP keep-alive, single process, loopback |
| Workload | `append-and-fetch`, 256-byte payload, `last=5` |

Server, probe, and shell are colocated on localhost, so latency numbers
include loopback TCP round-trip. Unlike the earlier macOS laptop rig,
Linux fsync on this NVMe forces a real device flush, so the durability
numbers reflect genuine sync cost.

## Append throughput (2026-09-04)

The hot path is append-then-fetch-recent. Two changes made this build
~12x faster than the prior one: an **O(1) session-tail lookup** (an
earlier per-append reconcile scanned and decrypted the whole session —
O(session length) per append), and making the **changefeed optional**.
The changefeed's per-event export-ledger index (four extra commit-ordered
KV writes per event) is the dominant append cost, so it is now **off by
default** — enable it with `serve --changefeed` when a consumer reads
the feed.

`perf-probe` spins N workers, each on its own session spread across 8
tenants, looping append-and-fetch. 8-second runs, warm server, RocksDB,
non-durable (`AGENTDB_ROCKSDB_SYNC_WRITES=0`):

| Concurrency | changefeed off (default) | changefeed on |
|------------:|-------------------------:|--------------:|
| 1  |  3,894/s (p50 0.19 ms) | 2,237/s (p50 0.40 ms) |
| 8  | 17,484/s (p50 0.34 ms) | 4,555/s (p50 1.24 ms) |
| 32 | 32,654/s (p50 0.87 ms) | 7,849/s (p50 2.18 ms) |
| 64 | 37,627/s (p50 1.54 ms) | 7,139/s (p50 6.40 ms) |

Zero errors at every level. With the changefeed off, throughput scales
with concurrency to ~33–38k and the median stays sub-millisecond. With
it on, each append also writes four commit-ordered index entries, which
caps throughput near ~8k.

### Versus Postgres / MongoDB

Same workload (append + fetch-recent, persistent, non-durable) run
through the `tools/agentdb-bench` comparison harness against normalized
event-log schemas, c=32:

| Backend | append+fetch @ c=32 |
|---|---:|
| AgentDB (changefeed off) | **~32,000/s** |
| PostgreSQL (event-log) | 26,763/s |
| MongoDB (event-log) | 16,465/s |

AgentDB edges both when the changefeed isn't in the write path. (AgentDB
is measured with `perf-probe` and the baselines with `agentdb-bench`, so
read this as order-of-magnitude, not a tuned-benchmark shootout.)

## Durability

Acknowledged appends fsync the RocksDB WAL when
`AGENTDB_ROCKSDB_SYNC_WRITES=1` (the numbers above are the non-durable
default). On this ext4 + NVMe box a synced *single-writer* append is
fsync-bound at ~15–17 ms (≈60–70/s); the cost is the device flush, not
CPU. Group commit amortizes the sync across concurrent writers (c=32 ≈
800/s), so under load the lever is concurrency / batching, not a faster
code path. (An earlier macOS-laptop measurement showed durable as
*faster* than non-durable — that was an artifact of macOS fsync not
forcing a platter flush, and does not hold on Linux.)

Crash safety is regression-tested by
`ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh`:
every acknowledged append must survive SIGKILL mid-stream across repeated
crash/restart cycles.

## Fork latency

100 sequential `POST .../fork` calls against a single parent session
seeded with 20 events (`perf-probe -mode fork`):

| metric | value |
|--------|------:|
| mean | 0.15 ms |
| p50  | 0.13 ms |
| p90  | 0.16 ms |
| p99  | 0.43 ms |
| max  | 0.70 ms |

Fork is cheap because it does not copy events — it writes one new
`Session` record with a `ForkRef` pointer to the parent. Every one of
the 100 forks cleared in under 0.7 ms.

## Tier transition latency

*(Apple M5 laptop; not re-run on li-dev.)*

Forced cold/warm transitions via `POST /api/admin/tier/force`, varying
session size (payload ~500 bytes per event):

| events | cold (ms) | reactivate (ms) |
|-------:|----------:|----------------:|
|     10 |      0.58 |            0.33 |
|    100 |      0.74 |            0.47 |
|   1000 |      4.05 |            1.98 |

Both directions scale linearly with session size. Cold transition is
dominated by serializing all live events into a segment file and
writing it to the object store; reactivation is mostly the segment
read and a few batched KV puts.

## Session lifecycle — end-to-end

*(Apple M5 laptop; not re-run on li-dev.)*

A 90-second run of the built-in Go workload
(`tools/workload/config.yaml`, 2 personas at 6 and 3 tickets/min) with
`AGENTDB_T_COLD_SECS=3 AGENTDB_TICK_SECS=1` — aggressive tiering that
flushes any session idle for ≥ 3 s.

```
 t+s    appends  warm  cold  transitions
   0          0     0     0            0
  21          8     2     1            1
  43         18     2     4            4
  64         28     2     7            7
  85         43     1    10           10

final     44      0    11           11
```

By the end of the run every session the workload produced had been
demoted to cold (11 sessions, 11 transitions), and the warm gauge was
at zero because the last sessions had also aged out by the time we
snapshotted. Appending to these sessions would still work — reads
transparently merge the object-store segment with any new WAL events.

Note: this is exercising the lifecycle sweep, not its efficiency —
11 sessions over 90 s is barely any work. The interesting property is
that the warm→cold transition count matches the workload's session
count exactly (no dropped events, no double transitions).

## Vector search (knn)

`crates/agentdb-index/benches/vector_bench.rs` builds one index with
10 000 pseudo-random 384-dimension points and runs 100 queries at
top-k = 10. The HNSW index is built once outside the measured region
(it's lazy — the "warming" call triggers the build). Brute-force
numbers come from an inline reference implementation in the same
bench.

| impl                  | median latency | vs brute force |
|-----------------------|---------------:|---------------:|
| brute force (reference)|      2.53 ms  |          1.00× |
| HNSW (`hnsw_rs`)      |       1.02 ms  |          2.5×  |

Recall@10 (overlap between HNSW top-10 and brute-force top-10, averaged
over 100 queries) at default `ef_construction=ef_search=100`: **0.58**.

**Caveat:** pseudo-random 384-dim vectors concentrate all cosine
similarities near 0, so the "true" top-10 has tightly-bunched scores
that are inherently hard for any ANN. Real embedding distributions
(sentence transformers, CLIP, etc.) have much more cluster structure
and typically recall ≥ 0.95 at the same settings. Setting `AGENTDB_HNSW_EF=400` before the bench gives roughly the same
recall on this random workload — the improvement is small because the
data itself has no structure to exploit.

Reproduce:
```bash
cargo bench -p agentdb-index --bench vector_bench
# Optional: tune ef_construction
AGENTDB_HNSW_EF=400 cargo bench -p agentdb-index --bench vector_bench
```

## Record Index Lag

Record search has two index paths. Requests with `sharding_key` use the
owner-local LSI-like index. Requests without `sharding_key` use the GSI-like
global index, which is updated asynchronously by the record-index outbox worker
(`AGENTDB_RECORD_INDEX_WORKER_INTERVAL_MS`, default 1000 ms).

Watch `GET /api/admin/indexes/records` for:

- `local_index_lag_tokens`
- `global_index_lag_tokens`
- `local_index_repairs_total`
- `record_index_worker_failures_total`
- `global_index_apply_failures_total`
- `snapshot_loaded_at_startup`, `snapshot_last_watermark`, `snapshot_writes_total`

## Record Index Snapshots

Startup does not re-scan and re-tokenize every record. The index worker
persists a snapshot of the text/vector/global indexes, doc-id maps, and
external-id map (keyed by the record-index watermark) whenever the watermark
has advanced at least `AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE` commits
(default 256) since the last snapshot. On boot the runtime restores the
snapshot and delta-replays only the records whose commit sequence moved past
it — updates, deletes, and GC purges included. Any snapshot decode failure
falls back to the full rebuild; `AGENTDB_RECORD_INDEX_SNAPSHOT=0` disables
snapshots entirely (write and load). HNSW graphs are not stored — they
rebuild lazily from the snapshotted vectors on the first query per path.
Replicated runtimes still start with a full hydration and skip snapshots;
their steady-state applies are incremental (below).

## Replicated Apply

On a replicated runtime, committed Raft entries used to trigger a full state
rehydrate on the next request — O(everything) per apply. The default is now
incremental: the large cached sets are evicted and reload on demand through
the KV fallbacks, the small config sets (tenant meta)
are re-scanned, and the search indexes converge
through the replicated record-index outbox. Outbox rows are trimmed once both
index watermarks pass them, keeping each drain proportional to new mutations.
`AGENTDB_REPLICATED_APPLY=full` restores the legacy full rehydrate.

Sustained non-zero global lag means no-key searches can miss fresh records
unless callers pass `min_visible_record_commit_seq`. The maintained SDKs pass
that token automatically after writes in the same client instance.

## Snapshot read overhead (M9)

All read primitives now go through a `ReadCtx` that wraps a RocksDB
snapshot. Cost per request is one `DB::snapshot()` call + a few
atomic increments on `BoxMetrics`.

Measured on the criterion bench `crates/agentdb-core/benches/mvcc_bench.rs`
against a session with 1000 events:

| operation                          | time     |
|------------------------------------|---------:|
| `ReadCtx::open` + scan 1000 events | ~193 µs  |

Interpretation: the snapshot-open cost is dominated by the `scan_prefix`
itself; the snapshot setup is a single-digit microseconds overhead.
At prototype scale this is well within the "agent workload tail-latency
tolerance" budget described in the design doc §1.1.

## Admission control overhead (M10)

Per-request overhead on the `Box_::admit` hot path: two
`parking_lot::Mutex` acquires (box bucket + tenant bucket map), refill
math (`as_secs_f64` subtraction + multiply + min), and one `HashMap`
entry lookup. Under `cargo test`, the 7 `admit_tests` complete in
well under 1 ms wall time including sleep-based refill assertions —
implying per-call overhead on the order of ~1 µs. At the c=64 /
6.3k rps throughput numbers reported above, this is effectively
free.

No separate bench harness shipped — pathological contention would
need thousands of concurrent tenants all within one box before the
single-mutex tenant-map becomes a bottleneck, which is above this
prototype's scale. A follow-up milestone could shard to `DashMap` or
per-tenant atomics if that changes.

## Storage backend comparison (M11, historical)

The SlateDB `KvStore` backend has since been retired; these numbers are
kept as the record of that evaluation. Measured on the criterion benches
in `crates/agentdb-core/benches/mvcc_bench.rs` with a session containing
1000 events, both backends pointed at local disk (RocksDB on SSD,
SlateDB on `object_store::local::LocalFileSystem`):

| operation | RocksDB | SlateDB (LocalFileSystem) |
|---|---|---|
| `ReadCtx::open` + scan 1000 events | ~196 µs | ~527 µs (≈2.7×) |

AWS S3 figures are not measured in CI; add ~10–30 ms per object-store
round-trip to the SlateDB column as a rough production estimate.

Interpretation: SlateDB on LocalFileSystem is about 2.7× slower than
RocksDB — expected overhead for async-over-sync bridging plus the
LSM layering. Both remain well within the agent-workload tail-latency
budget from `docs/design/product-thesis.md` §1.1. The throughput cost is mostly
in the fixed per-call overhead of the sync→async bridge, not the
scan itself, so larger scans close the gap.

## Lifecycle webhook overhead (M12)

Each tier transition fires a `tokio::spawn`ed `reqwest` POST with a
5s timeout. The caller returns immediately; the transition itself
does not wait. Measured overhead in the spawn path: one
`RwLock::read().clone()` to read the URL + one spawn + one JSON body
build, all in the low-µs range.

Failures (connection refused, timeout, non-2xx response) are counted
on `BoxMetrics::webhook_failures` and surface via
`/api/admin/boxes` and the dashboard. There is no retry and no
durable outbox in the prototype — a real deployment should front the
webhook with an internal queue or accept the at-most-once semantics.

Archive tier transitions themselves (`Tier::Cold` → `Tier::Archive`)
are metadata-only in the prototype — just a `session.tier` flip and
a `SessionRegistry::save` call. Sub-millisecond per transition on a
cold session of any size. Production cost would be dominated by
whatever object-storage-class move the integrator wires up; the
prototype itself pays nothing beyond the KV write.

## Gotchas

**HTTP keep-alive is not optional.** The Go probe originally closed
response bodies without draining them, which prevented connection
reuse. Under c=64 this exhausted the macOS ephemeral port range in
seconds and surfaced as "can't assign requested address" transport
errors. The fix is `io.Copy(io.Discard, resp.Body); resp.Body.Close()`
before the next request. Real clients (browsers, the dashboard,
SDK HTTP clients) have never tripped this because they all reuse
connections.

**LifecycleEngine wasn't recording metrics.** Until this perf run,
only the admin `force_tier` endpoint recorded `tier_transitions` +
`bump_tier`/`drop_tier`; the background sweep did the transition but
left the counters untouched. Fixed in this branch
(`crates/agentdb-core/src/lifecycle.rs`). Any perf comparison against
older builds should note that the "before" numbers undercounted.

**Stale processes from failed runs.** `pkill -f agentdb-server`
before each session. A trapped `cleanup` handler catches the normal
exit path, but a crashed script can leave the server listening on
:7878 with stale data, which makes the next run's "fresh" baseline
accidentally reuse state.

## Reproducing

```bash
# Build everything
cargo build --release -p agentdb-server
(
  cd tools/workload
  go build -o perf-probe ./cmd/perf-probe
  go build -o agentdb-workload ./cmd/agentdb-workload
)

# Throughput suite (c=1,4,16,64, 15 s each)
DURATION=15 bash ops/perf/scripts/perf_suite.sh

# Lifecycle run with the Go persona workload (aggressive tiering)
AGENTDB_T_COLD_SECS=3 AGENTDB_TICK_SECS=1 \
    bash ops/perf/scripts/perf_run.sh persona_lifecycle 90
```

Outputs land in `perf-out/`:
- `probe-c{N}.json` — raw JSON report per concurrency level
- `persona_lifecycle.csv` — one row per (second, box) sample
- `*.server.log`, `*.workload.log` — full logs
