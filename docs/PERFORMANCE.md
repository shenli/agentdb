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

## Throughput scaling

`workload/cmd/perf-probe` spins N workers that each loop
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
(`workload/config.yaml`, 2 personas at 6 and 3 tickets/min) with
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

## Gotchas

**HTTP keep-alive is not optional.** The Go probe originally closed
response bodies without draining them, which prevented connection
reuse. Under c=64 this exhausted the macOS ephemeral port range in
seconds and surfaced as "can't assign requested address" transport
errors. The fix is `io.Copy(io.Discard, resp.Body); resp.Body.Close()`
before the next request. Real clients (the Mongo driver, browsers,
the dashboard) have never tripped this because they all reuse
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
cd workload && go build -o perf-probe ./cmd/perf-probe
cd workload && go build -o agentdb-workload ./cmd/agentdb-workload
cd ..

# Throughput suite (c=1,4,16,64, 15 s each)
DURATION=15 bash scripts/perf_suite.sh

# Lifecycle run with the Go persona workload (aggressive tiering)
AGENTDB_T_COLD_SECS=3 AGENTDB_TICK_SECS=1 \
    bash scripts/perf_run.sh persona_lifecycle 90
```

Outputs land in `perf-out/`:
- `probe-c{N}.json` — raw JSON report per concurrency level
- `persona_lifecycle.csv` — one row per (second, box) sample
- `*.server.log`, `*.workload.log` — full logs
