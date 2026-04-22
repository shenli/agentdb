# Clock Model Decision — HLC

**Date:** 2026-04-21
**Status:** Design decision. Ratifies commitment listed as A5 in the [decision matrix](./2026-04-21-architecture-review-decision-matrix.md).

This note fixes AgentDB's clock model as **Hybrid Logical Clock (HLC)** and records the concrete design so that future milestones (placement unit, cross-placement snapshot reads, Mongo `$clusterTime` support) have a single reference point for time semantics.

---

## 1. What AgentDB actually needs a clock for

Most of the system does not need a clock at all; it needs **ordering** inside a well-defined scope. Enumerate carefully because the weakest sufficient mechanism should win.

| Concern | Actual need | Today's mechanism | Clock required? |
|---|---|---|---|
| Session-local append order | Strict per-session | `seq: u64` incremented under session lock | No |
| Fork snapshot semantics | Child sees parent up to fork point | `parent_seq_at_fork` | No |
| Lifecycle transitions (warm→cold→archive) | "Idle for N seconds" | Wall-clock `last_active_at` | Only coarse wall clock |
| Event `created_at` (audit / replay) | Human-readable timestamp | Wall-clock ns | Only coarse wall clock |
| SSE event ordering to dashboard | Loose causality | Sequence of emission | No |

Two concerns do require a real clock:

1. **Cross-placement snapshot reads.** `ReadCtx` today is a single-node RocksDB snapshot + a set of pinned segment paths. Once queries span multiple placements, "the snapshot as of time T" must have a globally meaningful definition — otherwise each placement picks its own T and the merged result is incoherent.

2. **Cross-placement fanout retrieval with time filters.** Queries of the shape "events in range [T1, T2] across a tenant's sessions" need every placement to apply the same time axis, or merge results are meaningless.

Both of these need **causality-respecting ordering with bounded drift from wall clock**. Neither needs external consistency (the real-world commit order). This is exactly what HLC provides.

---

## 2. Options considered

| Option | Gives | Does not give | Cost |
|---|---|---|---|
| Per-session seq only | Intra-session total order | Any cross-session or cross-placement order | 0 |
| Wall clock + NTP | Coarse order on events | Causality under NTP drift | 0, but semantics lie |
| **HLC** | **Causal order, bounded drift** | **External consistency** | **~12 B / event, RPC propagation** |
| Global TSO (PD-style) | Global strict order | — | TSO RTT per write, central bottleneck |
| TrueTime / commit-wait | External consistency | — | Atomic-clock infrastructure |

**HLC wins on fit:** agent workloads are not banking transactions, so external consistency is not worth paying for; per-session seq is insufficient once queries cross placements; NTP is too loose because AgentDB promises snapshot reads (§3.3 in `agent-db.md`) and wall-clock NTP-only silently violates causality.

HLC is production-validated: CockroachDB uses it end-to-end, YugabyteDB's distributed transactions are built on it, MongoDB 3.6+ exposes it as `$clusterTime`. Jepsen has tested all three. The algorithm itself comes from Kulkarni et al. 2014 ("Logical Physical Clocks").

---

## 3. Concrete design

### 3.1 Timestamp shape

```
HlcTimestamp {
  physical_ns:  u64,   // wall-clock component, nanoseconds since epoch
  logical:      u32,   // tie-breaker counter
}
```

12 bytes on the wire and at rest. Storage overhead on a 10 KB event is 0.12% — negligible.

Physical component uses nanoseconds to survive high-throughput append bursts without the logical counter saturating.

### 3.2 Clock state

Single per-process HLC: `AtomicU128` (or two `AtomicU64`s with a CAS loop). Every append, every RPC send, every RPC receive touches it. No locks; CAS loop is the contention budget.

Locating it: in `agentdb-core` as `HlcClock` struct, owned by the runtime, referenced by `Box_` (and future `Placement`). One per process, not per box/placement.

### 3.3 Allocation on append

Inside `append_and_fetch` the flow becomes:

```
1. Route to placement (hash(session_id)).
2. Acquire per-session lock.
3. hlc_ts = hlc.now()           // updates global HLC atomically
4. seq   = session.next_seq()
5. Write event { seq, hlc_ts, ... }
```

`hlc.now()` implementation (textbook HLC):

```rust
fn now(&self) -> HlcTimestamp {
  loop {
    let prev = self.load();
    let now_ns = wall_clock_ns();
    let phys = max(prev.physical_ns, now_ns);
    let logical = if phys == prev.physical_ns {
      prev.logical + 1
    } else {
      0
    };
    let next = HlcTimestamp { physical_ns: phys, logical };
    if self.cas(prev, next) { return next; }
  }
}
```

### 3.4 Propagation on RPC

Every outbound RPC carries the caller's current HLC:

```
Request  { hlc_caller: HlcTimestamp, ... }
Response { hlc_callee: HlcTimestamp, ... }
```

Every inbound HLC does a merge:

```rust
fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
  loop {
    let prev = self.load();
    let now_ns = wall_clock_ns();
    let phys = max(prev.physical_ns, remote.physical_ns, now_ns);
    let logical = if phys == prev.physical_ns && phys == remote.physical_ns {
      max(prev.logical, remote.logical) + 1
    } else if phys == prev.physical_ns {
      prev.logical + 1
    } else if phys == remote.physical_ns {
      remote.logical + 1
    } else {
      0
    };
    let next = HlcTimestamp { physical_ns: phys, logical };
    if self.cas(prev, next) { return next; }
  }
}
```

Propagation points in the current prototype:

- `agentdb-wire` OP_MSG handler: read `$clusterTime`, call `hlc.update(remote)`.
- `agentdb-api` REST handlers: read `X-AgentDB-HLC` header (native) or pull from request body.
- `agentdb-core` inter-box calls: forward HLC through request struct.
- `agentdb-storage` segment reads: segments encode their own max HLC in the header; readers update HLC on segment open.

### 3.5 Snapshot reads

`ReadCtx` gains one field:

```
ReadCtx {
  rocks_snapshot: rocksdb::Snapshot,
  pinned_segments: Vec<SegmentPath>,
  hlc_cutoff: HlcTimestamp,        // NEW
}
```

Read filtering:

- KV reads: post-filter by `event.hlc_ts <= ctx.hlc_cutoff`.
- Segment reads: segment header records `max_hlc` at write time. Segments with `max_hlc > ctx.hlc_cutoff` may still contain visible events; the in-segment scan post-filters. Segments entirely above the cutoff can be skipped wholesale once the writer tracks `min_hlc` too (small follow-up).

`ReadCtx::new()` allocates `hlc_cutoff = hlc.now()` at open time. Cross-placement queries propagate the cutoff to every placement via the fanout coordinator.

### 3.6 Mongo `$clusterTime` mapping

Mongo's `$clusterTime` is already effectively an HLC timestamp: `(timestamp: BsonTimestamp, signature: ...)`, where `BsonTimestamp` is `(seconds_since_epoch: u32, counter: u32)`. AgentDB maps:

- On response: set `$clusterTime.clusterTime` from current HLC (`physical_ns / 1e9` for seconds + logical for counter).
- On request: read `$clusterTime` from inbound OP_MSG, feed to `hlc.update()`. Subject to max-offset check (§3.7).

Result: any Mongo 3.6+ driver that already tracks causal consistency works unchanged against AgentDB.

Signature field is left empty in v1 (no auth-bound clock signing). Mongo drivers accept missing signatures when the connection is trusted.

### 3.7 Max-offset protection

A rogue or misconfigured node can send HLC with physical component hours in the future. Naively accepting it poisons the global clock — every subsequent `hlc.now()` returns a fake-future time.

Mitigation:

```rust
const MAX_OFFSET_NS: u64 = 10_000_000_000; // 10 seconds

fn update_guarded(&self, remote: HlcTimestamp) -> Result<HlcTimestamp, ClockError> {
  let now_ns = wall_clock_ns();
  if remote.physical_ns > now_ns + MAX_OFFSET_NS {
    return Err(ClockError::OffsetTooLarge {
      remote: remote.physical_ns,
      local: now_ns,
    });
  }
  Ok(self.update(remote))
}
```

Configurable via `AGENTDB_HLC_MAX_OFFSET_SECS`. Violations are logged, incremented in `BoxMetrics.hlc_offset_rejects`, and the offending request is failed (not just the clock update — the whole request, because the caller can't be trusted on time).

This is the CockroachDB `max_offset` mechanism. Empirically sufficient.

---

## 4. What HLC does NOT guarantee

Being explicit so marketing and docs don't overpromise:

- **Not external consistency.** Client A commits transaction T1 at real time t=1.0s; client B commits T2 at t=1.5s. A reader with a locally skewed clock may observe T2 ordered before T1. For banking workloads this is a showstopper; for agent workloads it is acceptable (no correctness invariant depends on real-world commit order).
- **Not a concurrency primitive.** HLC orders reads; it does not prevent concurrent writes. Write conflicts are still resolved by per-session locks (or opt-in cross-session coordination when that ships).
- **Not a clock repair mechanism.** HLC bounds the *effect* of a bad clock to the max-offset window. It does not detect or fix the underlying NTP failure. Operational monitoring is mandatory.
- **Not tamper-proof.** The max-offset check defends against accidents, not adversaries. A malicious client inside the authentication boundary can drift HLC up to `max_offset` per request. If we ever need tamper-proof time, that is signed `$clusterTime` — a v2+ problem.

---

## 5. Failure modes and mitigations

| Failure | Effect | Mitigation |
|---|---|---|
| Wall clock steps backward (NTP correction) | HLC physical doesn't regress; logical advances; `hlc.now()` may be "ahead" of wall clock briefly | No action; HLC absorbs it |
| Wall clock steps forward (NTP correction) | HLC physical catches up; logical resets | No action |
| Sustained NTP drift (wall clock 5s slow) | HLC physical pinned to incoming remotes; skew metric rises | Alert when `hlc.physical - wall_clock > 5s` for sustained period |
| Process restart | HLC restarts at wall clock | On startup, read last persisted HLC from storage metadata; set local HLC to `max(last, wall_clock)`. Prevents rare duplicate timestamps |
| Rogue node sends HLC = now + 1h | Max-offset check rejects the request | `hlc_offset_rejects` counter increments; investigate rogue node |
| All nodes drift together (no canary) | Undetected | Cross-check against external NTP in monitoring; already standard ops |
| Single-node flips clock to past | Next outgoing HLC is normal (takes max with remote); next incoming HLC correctly rejected by peer if future | No action beyond existing guard |

---

## 6. Operational

- **Metric `hlc_physical_offset_ns`** — current `hlc.physical - wall_clock`. Gauge. Alert threshold: 5s sustained.
- **Metric `hlc_offset_rejects`** — counter of rejected inbound HLC updates. Alert threshold: >0 on sustained basis (would indicate either a rogue peer or a real clock problem).
- **Admin endpoint `GET /api/admin/hlc`** — returns current HLC, wall clock, offset, reject count. For on-call inspection.
- **Integration tests** required:
  - Node with 5s-slow clock joins a healthy cluster — clock converges, no rejects.
  - Node with 1h-future clock tries to push time — all updates rejected, HLC unchanged on healthy nodes.
  - Rapid restart cycle — no duplicate HLCs emitted.
- **Benchmarks** should include an HLC-propagation hot path variant to confirm CAS contention isn't a throughput bottleneck at the target append rate.

---

## 7. What this decision unlocks and what it defers

**Unlocks:**

- Placement-unit refactor (C1 in the matrix) can use HLC for cross-placement `ReadCtx`.
- Fanout retrieval with time filters (B4) has a well-defined time axis.
- Mongo `$clusterTime` support — drivers that already do causal consistency work immediately.

**Does not solve (explicitly deferred):**

- **Opt-in cross-placement transactions (§3.3).** HLC orders reads; distributed commit still requires a coordination protocol (2PC variant) when that milestone comes. HLC is a prerequisite, not a solution.
- **Signed `$clusterTime` for adversarial settings.** Out of scope for v1.
- **Clock-skew-triggered node eviction.** CockroachDB does this; AgentDB monitors but does not evict in v1.

---

## 8. References

- Kulkarni, Demirbas, Madappa, Avva, Leone. "Logical Physical Clocks." OPODIS 2014.
- CockroachDB docs: *Cluster clocks and the `--max-offset` flag.*
- MongoDB `$clusterTime` specification, server 3.6+.
- Jepsen analyses of CockroachDB and YugabyteDB (HLC correctness under partition).

---

## 9. Scope commitment

This design is v1-final. Changes to the timestamp shape, propagation rules, or max-offset semantics require a new design note. Implementation follows the A-bucket sequence in the decision matrix: HLC wiring is prerequisite to placement-unit work (C1) and can be developed in parallel with BoTE (A1) and tracing (A2).
