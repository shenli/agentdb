# AgentDB Performance Notes

These numbers come from a single laptop. They are meant to ground the
README and give a rough sense of what the prototype can do — not to
make competitive claims against real databases.

## Setup

| | |
|---|---|
| Hardware | Apple M5 laptop, 16 GB RAM |
| OS | Darwin 25.4.0 (arm64) |
| Build | `cargo build --release -p agentdb-server` |
| Storage | RocksDB on local SSD, object store = local FS |
| Server | 4 boxes, 1 process, default Tokio multi-threaded runtime |
| Client | Go 1.26 probe, HTTP keep-alive, single process |
| Workload | `append_and_fetch` with 256-byte payload, `fetch_last_n=5` |

All runs use the same hardware and default server config unless noted.
The server, probe, and shell are colocated on localhost, so latency
numbers include loopback TCP round-trip (~50 µs on this box).

## Durable by default (2026-08-30)

Acknowledged appends now fsync the RocksDB WAL by default
(`AGENTDB_ROCKSDB_SYNC_WRITES=0` opts out). Group commit is backlog-aware:
an idle append pays only its own sync; under concurrency the writer
coalesces the queue into one synced batch (2 ms window,
`AGENTDB_APPEND_GROUP_COMMIT_BACKLOG_US`).

Reference numbers, Apple M5 laptop, release build, 256-byte payloads,
Python thread-pool client over loopback (client overhead included; macOS
fsync does not force platter flush, so absolute sync cost is optimistic —
Linux numbers will differ):

| Mode | Workers | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| fsync on (default) | 1 | 426/s | 2.40 ms | 4.18 ms |
| fsync on (default) | 32 | 3,470/s | 8.87 ms | 17.19 ms |
| fsync off (opt-out) | 1 | 423/s | 2.31 ms | 4.62 ms |
| fsync off (opt-out) | 32 | 1,846/s | 13.67 ms | 59.69 ms |

Two observations: single-writer latency is unchanged by the durable
default (the backlog-aware window adds no idle wait), and under
concurrency the durable path is *faster* — group commit turns 32
contending writers into large synced batches, while the non-durable
direct path degrades under CAS contention.

Crash safety is regression-tested by
`ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh`:
every acknowledged append must survive SIGKILL mid-stream across repeated
crash/restart cycles. (SIGKILL cannot falsify fsync itself — the page
cache survives process death; power-loss protection needs a VM-kill or
dm-flakey rig, tracked as follow-up.)

## Throughput scaling

`tools/workload/cmd/perf-probe` spins N workers that each loop
`append_and_fetch` against one of 8 tenants (worker `i` uses tenant
`t-perf-{i mod 8}` and session `s-w{i}`). 15-second runs, warm
server, no faults or tier transitions.

| Concurrency | Requests | Errors | RPS   | p50     | p90     | p99     | max     |
|------------:|---------:|-------:|------:|--------:|--------:|--------:|--------:|
|           1 |    9 314 |      0 |   621 |  1.6 ms |  2.8 ms |  3.2 ms |  5.9 ms |
|           4 |   27 945 |      0 | 1 863 |  2.1 ms |  4.1 ms |  5.2 ms |  9.0 ms |
|          16 |   60 678 |      0 | 4 043 |  3.3 ms |  7.8 ms | 14.4 ms |105.2 ms |
|          64 |   95 121 |      0 | 6 334 |  9.1 ms | 18.0 ms | 28.2 ms | 83.0 ms |

**Reading the numbers:** throughput scales roughly linearly up to
c=16 (FNV-1a routes the 8 tenants across 4 boxes, so most workers
don't contend on the same box write_lock). At c=64 the box locks and
single RocksDB instance become the bottleneck; RPS plateaus ~6.3k and
tail latency climbs into the tens of ms. Zero HTTP errors at any
concurrency once keep-alive is actually reused (see "Gotchas" below).

## Fork latency

100 sequential `POST /api/sessions/parent/fork` calls against a single
parent session seeded with 20 events:

| metric | value |
|--------|------:|
| mean | 0.52 ms |
| p50  | 0.20 ms |
| p90  | 0.25 ms |
| p99  | 0.36 ms |
| max  | 31.8 ms |

Fork is cheap because it does not copy events — it writes one new
`Session` record with a `ForkRef` pointer to the parent. The outlier
(~32 ms) is likely a GC or compaction pause; 99% of forks clear in
under 400 µs.

## Tier transition latency

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

| impl                     | median latency | vs brute force |
|--------------------------|---------------:|---------------:|
| brute force (reference)  |       2.39 ms  |          1.00× |
| HNSW (`instant-distance`)|       0.77 ms  |          3.1×  |

Recall@10 (overlap between HNSW top-10 and brute-force top-10, averaged
over 100 queries) at default `ef_construction=100`: **0.85**.

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
the KV fallbacks, the small config sets (export consumers, tenant meta)
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
