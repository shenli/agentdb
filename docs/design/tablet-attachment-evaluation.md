# Manifest-Based Tablet Attachment — Evaluation

**Status: eager SST movement built (opt-in); lazy-block attach still
gated.** This document records the evidence and reasoning for the
hardening-plan §7 question — *can a cold/read-mostly tablet move without
copying its complete durable history?* — and the go/no-go outcome. The
eager rung (SST export/ingest for whole-tablet moves) is now implemented
behind `AGENTDB_SST_MOVEMENT` with a disk reservation; the full lazy-block
attach remains gated behind a demonstrated need. Per the plan, the local
hot-tablet path is not replaced unless the AgentDB session-native workload
shows an operational or cost win.

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

## RocksDB SST-ingest prototype (eager §7 attach)

The compact encoding removes the *encoding* cost, but both the old and new
logical paths still **re-insert every key on the target** via `write_batch`
— through the memtable, flush, and compaction. RocksDB can skip that: write
the source range to a self-contained SST (`SstFileWriter`) and bulk-load it
into the target with `IngestExternalFile`, which lands the file at the
bottom of the LSM with no per-key work. AgentDB already uses RocksDB
`Checkpoint` for backups; this adds the ingest half.

Prototype (`crates/agentdb-storage/src/rocks.rs` — `write_range_to_sst` /
`ingest_ssts`; benchmark `crates/agentdb-storage/tests/sst_movement_bench.rs`)
measured the target-side install of a tablet key range:

| events | range bytes | sst bytes | replay_ms (write_batch) | ingest_ms | speedup |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 227 KB | 9 KB | 0.9 | 0.6 | 1.4× |
| 5,000 | 1.14 MB | 42 KB | 4.1 | 0.6 | 7× |
| 20,000 | 4.54 MB | 167 KB | 15.0 | 0.5 | 28× |
| 80,000 | 18.16 MB | 666 KB | 61.8 | 1.0 | **59×** |

Two things stand out: **ingest time is ~flat (~1 ms) regardless of tablet
size**, because the SST is placed, not replayed — so the win over
`write_batch` grows without bound with the tablet. And the SST is
**compressed** (the transfer shrinks too; the 27× here is inflated by the
uniform test payload, but real SSTs are always ≤ the raw range). This is
the eager form of §7: move the SST (optionally staged in the object store)
instead of the full KV dump, without the on-read object-store dependency of
lazy attach.

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
  storage-engine change: 3.4× smaller snapshots and ~44× faster serialize.
- The **SST export/ingest primitives** (`RocksKvStore::write_range_to_sst` /
  `ingest_ssts`) plus a prototype benchmark proving the target-side win
  (~flat ~1 ms install, 59× at 80k events, growing).

**Built — eager SST-based whole-tablet movement (the proven win):**
Whole-tablet moves (replacement, adding a replica) now flow through
SST export → transfer → ingest instead of the logical KV dump, behind an
opt-in flag. It has the big proven payoff and *far* less risk than lazy
attach — no on-read object-store dependency, no block-cache management.
As shipped:
- **Whole-tablet moves only.** The raft snapshot-transfer path
  (`Runtime::export_tablet_snapshot_payload` →
  `install_tablet_snapshot_payload_and_rehydrate`) takes the SST path;
  shard-range moves (split/merge) keep the logical path (you can't cheaply
  carve a shard subset out of an SST, and the SST is a byte-copy that
  preserves the tablet id).
- **Idempotent, any-target install.** `RocksKvStore::replace_prefix_from_sst`
  range-deletes the tablet scope, then ingests the SST with a global seqno,
  so it overwrites even a target that already holds a stale copy of the
  range — not just a fresh replica.
- **Self-describing payload + rolling-upgrade safety.** The wire payload is
  framed with an `ADSS` magic (distinct from the logical `ADBS`), so a
  receiver routes it correctly and an old build rejects rather than
  mis-parses it. Movement is **off by default** (`AGENTDB_SST_MOVEMENT`);
  any build recognizing `ADSS` can always *decode* one, so enabling senders
  last keeps a rolling upgrade safe. The logical compact-encoded path stays
  the always-safe default and the automatic fallback (disabled, below
  threshold, empty, or non-RocksDB backend).
- **Size threshold.** `AGENTDB_SST_MOVEMENT_MIN_BYTES` (default 256 KiB);
  ingest has fixed overhead, so small tablets stay logical where they are
  already cheap.
- **Disk-space reservation.** Snapshot generate/apply never consumes the
  data disk below `AGENTDB_MOVEMENT_DISK_RESERVE_BYTES` (default 2 GiB):
  export degrades to the in-memory logical path, and install fails fast with
  an "add disk" error rather than driving the disk to zero — which would
  wedge the node into a doom loop, since even compaction and recovery need
  scratch space (TiKV's store `reserve-space` lesson). Scratch SSTs are
  written on the DB's own filesystem so the reservation measures the right
  disk.

**Still open (optional, toward §7):**
- **Object-store staging**: put the SST in the bucket and ship a manifest
  reference instead of the bytes inline, so the transfer does not ride the
  raft snapshot payload.

**Still gated — the full lazy-block attach:**
- Only worth it beyond eager SST if a workload needs to attach *without*
  even transferring the tail (serve from object-store blocks on demand).
  That is the storage-engine change with read-path-p99 and
  cache-loss-recovery risk; prototype it for **cold tablets only** and
  measure the plan's eight metrics before adopting.

Net: the compact-encoding win is delivered and the **eager SST path is
built (opt-in, with a disk-space reservation)**; the full lazy-block attach
stays gated behind a demonstrated need.
