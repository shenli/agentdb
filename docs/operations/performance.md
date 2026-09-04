# AgentDB Performance Notes

These numbers come from a single machine. They are meant to ground the
README and give a rough sense of what the prototype can do — not to make
competitive claims against tuned databases. The headline append / fork /
vector numbers below were refreshed on a Linux workstation (2026-09-04);
a few architectural micro-benchmarks further down (tier transitions,
session lifecycle, M9–M12) still carry their original Apple M5 laptop
measurements and are labeled where that is the case.

## Setup

| | |
|---|---|
| Hardware | Intel Core i7-14700KF (20 cores / 28 threads), 32 GB RAM |
| OS | Ubuntu 22.04.5 LTS (Linux 6.8, x86-64) |
| Build | `cargo build --release -p agentdb-server` |
| Storage | RocksDB on local NVMe SSD (ext4), object store = local FS |
| Server | single `agentdb serve --dev` process, default Tokio multi-threaded runtime |
| Client | `perf-probe` (Go), HTTP keep-alive, single process |
| Workload | `append-and-fetch` with 256-byte payload, `last=5` |

All runs use the same hardware and default server config unless noted.
The server, probe, and shell are colocated on localhost, so latency
numbers include loopback TCP round-trip. Unlike the earlier macOS laptop
rig, `fsync` on this Linux/NVMe box forces a real device flush, so the
durability numbers below reflect genuine sync cost rather than a
page-cache write.

## Durable by default (2026-09-04)

Acknowledged appends now fsync the RocksDB WAL by default
(`AGENTDB_ROCKSDB_SYNC_WRITES=0` opts out). Group commit is backlog-aware:
an idle append pays only its own sync; under concurrency the writer
coalesces the queue into one synced batch (2 ms window,
`AGENTDB_APPEND_GROUP_COMMIT_BACKLOG_US`).

Reference numbers, li-dev (i7-14700KF, NVMe), release build, 256-byte
payloads, `perf-probe` over loopback (client overhead included; on this
Linux/NVMe box `fsync` forces a real device flush, so sync cost is not
free):

| Mode | Workers | Throughput | p50 | p99 |
|---|---:|---:|---:|---:|
| fsync on (default) | 1 | 7,492/s | 0.11 ms | 0.57 ms |
| fsync on (default) | 32 | 32,890/s | 0.76 ms | 3.46 ms |
| fsync off (opt-out) | 1 | 6,809/s | 0.12 ms | 0.60 ms |
| fsync off (opt-out) | 32 | 46,025/s | 0.58 ms | 2.23 ms |

Two observations: single-writer latency is essentially unchanged by the
durable default (the backlog-aware window adds no idle wait — 0.11 vs
0.12 ms p50), and under concurrency the durable path costs throughput —
at 32 workers `fsync` on sustains ~33k/s against ~46k/s with `fsync`
off, because each synced batch pays a real NVMe flush. Group commit
keeps that cost bounded by coalescing the 32 contending writers into
shared synced batches, so the durable p99 stays at 3.5 ms rather than
blowing out. (This is the opposite ordering from the earlier macOS
laptop run, where `fsync` looked *free* because macOS does not force a
platter flush; that comparison was misleading and is superseded here.)

Crash safety is regression-tested by
`ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh`:
every acknowledged append must survive SIGKILL mid-stream across repeated
crash/restart cycles. (SIGKILL cannot falsify fsync itself — the page
cache survives process death; power-loss protection needs a VM-kill or
dm-flakey rig, tracked as follow-up.)

## Throughput scaling

`tools/workload/cmd/perf-probe` spins N workers that each open their own
session (spread across 8 tenants) and loop `append-and-fetch` on it.
15-second runs, warm server, no faults or tier transitions.

| Concurrency | Requests | Errors | RPS    | p50     | p90     | p99     | max     |
|------------:|---------:|-------:|-------:|--------:|--------:|--------:|--------:|
|           1 |  112 379 |      0 |  7 492 | 0.11 ms | 0.18 ms | 0.57 ms |  1.7 ms |
|           4 |  211 458 |      0 | 14 097 | 0.20 ms | 0.52 ms | 0.92 ms | 17.7 ms |
|          16 |  542 257 |      0 | 36 150 | 0.33 ms | 0.74 ms | 1.85 ms | 13.2 ms |
|          32 |  493 405 |      0 | 32 890 | 0.76 ms | 1.70 ms | 3.46 ms | 30.5 ms |
|          64 |  543 939 |      0 | 36 259 | 1.41 ms | 3.07 ms | 6.16 ms | 20.6 ms |

**Reading the numbers:** throughput climbs from 7.5k rps (c=1) to ~36k
rps by c=16, then plateaus — a single RocksDB writer with backlog-aware
group commit serializes durable appends, so beyond ~16 concurrent
writers RPS holds in the 33–36k band while latency grows with the queue
depth (p50 0.33 → 1.41 ms from c=16 to c=64). Zero HTTP errors at any
concurrency once keep-alive is actually reused (see "Gotchas" below).

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
`Session` record with a `ForkRef` pointer to the parent. On li-dev every
one of the 100 forks cleared in under 0.7 ms; the earlier laptop run
showed a lone ~32 ms GC/compaction outlier, but this run had none.

## Tier transition latency

*(Apple M5 laptop; not yet re-run on li-dev.)*

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

*(Apple M5 laptop; not yet re-run on li-dev.)*

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
that are inherently hard for any ANN — the low recall here is a property
of structureless data, not the index. Real embedding distributions
(sentence transformers, CLIP, etc.) have much more cluster structure and
typically recall far higher at the same settings. Raise
`AGENTDB_HNSW_EF` to trade latency for recall; on this random workload
the gain is small because the data itself has no structure to exploit.

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
# Build the server and the probe
cargo build --release -p agentdb-server
(cd tools/workload && go build -o perf-probe ./cmd/perf-probe)

# Throughput sweep + durable-on rows (c=1,4,16,32,64, 15 s each).
# perf_suite.sh starts its own `agentdb serve --dev` on a fresh data dir.
DURATION=15 CONCURRENCY="1 4 16 32 64" bash ops/perf/scripts/perf_suite.sh

# Durability opt-out rows (fsync off)
AGENTDB_ROCKSDB_SYNC_WRITES=0 DURATION=15 CONCURRENCY="1 32" \
    OUT_DIR=perf-out-nosync bash ops/perf/scripts/perf_suite.sh

# Fork latency — needs a running server:
#   ./target/release/agentdb serve --dev --data-dir /tmp/adb-fork &
tools/workload/perf-probe -mode fork -fork-seed 20 -fork-n 100

# Vector search (HNSW vs brute force, recall@10)
cargo bench -p agentdb-index --bench vector_bench
```

Outputs land in `perf-out/`:
- `probe-c{N}.json` — raw JSON report per concurrency level
- `persona_lifecycle.csv` — one row per (second, box) sample
- `*.server.log`, `*.workload.log` — full logs
