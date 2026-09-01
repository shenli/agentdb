# Manifest-Based Tablet Attachment — Evaluation

**Status: evaluation (P2), not a committed feature.** This document records
the evidence and reasoning for the hardening-plan §7 question — *can a
cold/read-mostly tablet move without copying its complete durable history?*
— and gives a go/no-go recommendation. Per the plan, the local hot-tablet
path is not to be replaced unless the AgentDB session-native workload shows
an operational or cost win.

## The question

When a tablet moves (placement, replacement, a new replica joining), the
new owner must obtain the tablet's state. The hypothesis is that cold or
read-mostly state could be represented by immutable shared object-store
blocks plus a small authoritative manifest and the recent Raft tail, so a
new replica attaches by fetching metadata and pulling blocks on demand
instead of copying the tablet's full durable byte set.

## What happens today

A move transfers the **entire** tablet runtime state as one snapshot
payload: `Runtime::export_tablet_snapshot_payload`
(`crates/agentdb-core/src/runtime.rs`) calls
`state_machine.export_snapshot()` and `serde_json`s the whole KV state; the
scheduler ships it via `build_snapshot_transfer` → HTTP →
`install_snapshot_transfer` (`crates/agentdb-storage/src/raft_rs_log.rs`).
So movement cost is **O(total tablet state)**, independent of how much of it
is cold.

## Measured baseline

`crates/agentdb-core/tests/tablet_movement_cost_bench.rs` (an `#[ignore]`d
benchmark; run with `--ignored --nocapture`) drives the session-native
workload — create session, append events, export the movement snapshot —
and measures the full-state snapshot size and export time as the session
grows. On one run (in-memory content, RocksDB KV, ~200-byte JSON events):

Before the compact-encoding fix (finding 2 below), with `serde_json`:

| events | snapshot_bytes | export_ms | bytes/event |
|---:|---:|---:|---:|
| 100 | 902,945 | 20.7 | 9,029 |
| 500 | 4,463,990 | 101.2 | 8,928 |
| 2,000 | 17,827,863 | 420.7 | 8,914 |
| 8,000 | 71,308,571 | 1,668.6 | 8,914 |

After the compact binary encoding (same run, same workload):

| events | snapshot_bytes | export_ms | bytes/event |
|---:|---:|---:|---:|
| 100 | 263,385 | 0.5 | 2,634 |
| 500 | 1,301,785 | 2.3 | 2,604 |
| 2,000 | 5,198,785 | 9.5 | 2,599 |
| 8,000 | 20,792,785 | 38.3 | 2,599 |

The compact encoding cut the 8,000-event move from **71 MB → 20.8 MB
(3.4×)** and its serialize time from **1.67 s → 38 ms (~44×)** — the
JSON int-array form was both bloated and slow to produce. Movement cost is
still O(total state), but the constant is now ~3.4× lower and the CPU cost
is negligible.

Two findings:

1. **Movement cost grows linearly with session state** — ~79× from 100 to
   8,000 events — and is not small: a single 8,000-event session is a
   **71 MB** snapshot that takes **1.7 s just to serialize**, before any
   network transfer or apply on the target. A tablet hosting many or large
   sessions scales this up directly. This is exactly the cost a
   manifest+recent-tail attach would aim to cut to roughly the tail.
2. **The per-event footprint is heavy** (~8.9 KB for a ~200-byte payload —
   ~45×), partly because the snapshot was `serde_json`-encoded, which
   expands each `Vec<u8>` key/value into an array of decimal numbers
   (`[104,101,...]`), a ~3-4× blowup of binary KV data. This is an
   independent, cheaper optimization that does **not** require the
   storage-engine change §7 contemplates.

   **This is now done:** `TabletStateSnapshot::encode`/`decode`
   (`crates/agentdb-core/src/tablet_state.rs`) use a compact length-prefixed
   binary layout instead of JSON (with a magic+version header and a legacy-
   JSON decode fallback so rolling upgrades stay safe). Every movement and
   raft-snapshot transfer now ships the smaller encoding. See the
   before/after in the movement benchmark.

## Feasibility against the current architecture

The "manifest + shared objects" primitives partly exist:

- Object-store tiering already parks cold content (artifacts, session
  archives) in the bucket.
- **Tablet checkpoint manifests** (`TabletStoreManifest`: `storage_uri`,
  `checksum_sha256`, `latest_applied_index`) already describe a checkpoint
  *by reference*.
- `RaftSnapshotObjectStore` already stages raft snapshots in the object
  store.

The hard part is the KV engine. The tablet store is RocksDB, and "attach by
fetching blocks on demand" means either sharing RocksDB SST files from the
bucket (a RocksDB-Cloud-style remote-block cache — RocksDB does not natively
read SSTs from object storage) or a custom block layout behind the `KvStore`
trait. Either is a **storage-engine-level change** with two risks the plan
lists as key metrics:

- **Read-path p99** — a cache miss becomes an object-store GET on the read
  path.
- **Recovery after complete local-cache loss** — correctness and warm-up
  time when the block cache is empty.

The current full-snapshot install is simple and safe; lazy attach trades
that for partial-state serving, cache-miss handling, and cache-loss
recovery.

## Recommendation

The baseline shows the cost §7 targets is **real and grows without bound**
with session state (71 MB / 1.7 s at 8,000 events, linear). But
manifest-based attachment is a storage-engine change with genuine read-path
and correctness risk, so it should not be built speculatively.

**Done (cheap, high-value):**
- Movement-cost instrumentation — the scheduler logs `snapshot_bytes` +
  `export_ms` per movement, so real replacements and the S9 smoke report
  this in production.
- The **compact snapshot encoding** — a large, low-risk win with no
  storage-engine change: 3.4× smaller snapshots and ~44× faster serialize
  (see the before/after table above).

**Gate for the storage-engine change:**
- Prototype a manifest+lazy-block attach for **cold tablets only**, behind a
  flag, and measure the plan's eight metrics (bytes transferred,
  time-to-learner, time-to-p99, object-store GET count/cost, cache size +
  warm-up bandwidth, retained unflushed Raft tail, recovery-after-cache-loss,
  compaction read/write amplification) on the session-native benchmark.
- **Adopt only if** the session-native workload (`append → commit → apply →
  fetch recent tail`) shows an operational or cost win over the compact
  full-snapshot path. Generic point-put throughput is not sufficient
  evidence.

Net: **no-go on building the storage-engine change now**; the compact-encoding
win (3.4× smaller, ~44× faster moves) is delivered, and the lazy-attach
prototype stays gated behind a demonstrated need.
