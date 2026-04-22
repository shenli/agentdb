# Back-of-Envelope Capacity — Pre-Flight Gate for v2 Implementation

**Date:** 2026-04-21
**Purpose:** Gate before M14 begins. Establish binding constraints; if any flip the priority order in the v2 implementation plan, re-plan before coding.
**Plan reference:** `docs/superpowers/plans/2026-04-21-v2-design-implementation.md` §2

## Caveat — these are assumptions, not measurements

No specific customer workload has been specified. This BoTE uses the design doc's Type III scale assumptions (`agent-db.md` §2.8) as baseline and derives everything from there. **When a real customer workload is specified, redo this BoTE first; it may re-rank milestones.**

## Assumed workload

A "v2 target deployment" sits between the small-production case (single-node, 10³ tenants) and the full Type III ceiling (10⁷ tenants).

| Dimension | v2 target | Type III ceiling | Notes |
|---|---|---|---|
| Total tenants | **10⁶** | 10⁷ | Design ceiling; v2 aims for the mid-scale |
| Active tenants at steady state | **10⁴** (1%) | 10⁵ | Per §2.8: "99% at 0 QPS" |
| Concurrent sessions per active tenant | **~10** | ~10 | Agent-style workload |
| Concurrent sessions cluster-wide | **10⁵** | 10⁶ | active_tenants × sessions_each |
| Events per active session per second | **1** | ~10 (bursty) | Agent iteration rate |
| Bytes per event | **10 KB** | 10 KB – 1 MB | Design §2.1 |
| Steady-state event rate | **10⁵ events/sec** | 10⁷ events/sec | |
| Steady-state write throughput | **1 GB/sec** | 100 GB/sec | |
| Daily data volume | **~85 TB/day** | ~8.5 PB/day | |

The v2 target is aggressive but not unrealistic. The Type III ceiling is the marketing-slide scale; v2 should prove the architecture works at the target without collapsing toward the ceiling.

## Per-layer capacity check

### S3 PUT rate (likely binding)

S3 per-prefix PUT limit: **~3500 PUT/sec**. At 10⁵ events/sec:

- One PUT per event → need 30 prefixes fanned out (10⁵ / 3500)
- **With WAL batching at 100 events/batch → 1000 PUT/sec → single-prefix capable.** Much healthier budget.

**Implication:** WAL batching is NOT optional. At v2 target, it's strictly required.
**Plan impact:** batched-WAL work must land before clustered production scale. It's absent from the M14–M27 plan as a named milestone. Either roll it into M20 (tablet refactor touches the write path anyway) or add M16.5.

### HNSW index memory

Assumed embedding: 1536-dim float32 = **6 KB/vector**.

- 10⁵ concurrent sessions × 10³ events each × ~10% vectorizable = 10⁷ vectors
- Total raw: 60 GB of vectors
- HNSW overhead ~1.3× = 80 GB index state
- Per 100-tablet cluster: **800 MB per tablet** — fits comfortably in memory
- Per 10-tablet cluster: 8 GB per tablet — still fine on modern nodes

**Not binding** for v2 target. At Type III ceiling (100× more), it's 80 GB/tablet → needs on-disk HNSW or tablet split.

### Network throughput

- Inbound writes: 1 GB/sec
- Outbound: WAL write (1 GB/s) + segment upload (async, 1 GB/s) + index state push (async, low rate)
- ~2.5 GB/s egress steady state per cluster

**Not binding** with 10 Gbps interconnect. **Binding at 1 Gbps.** Flag: document that v2 clustered production requires 10 Gbps networking.

### HLC CAS contention

10⁵ events/sec × atomic CAS = **100 μs/sec total contention cost**. Negligible. HLC is not a bottleneck.

### Postgres heartbeat load

- 100 nodes × 1 heartbeat/10s = **10 writes/sec** on `nodes` table
- Postgres: trivial (RDS db.m5.large handles 5000 writes/sec)

**Not binding** up to ~1000 nodes. Beyond that, federate into sub-clusters (already in §9 non-goals as a guideline).

### Lease renewal load

- Tablets per cluster: ~100 at v2 target
- Renewal every 10s per tablet = **10 writes/sec** on `tablet_ownership`
- Merges with heartbeat load; still trivial

### Failover blast radius

- MTBF per node (industry rule of thumb): ~30 days
- 100 nodes → ~3 failures/day
- 30s failover window × 3/day → **90s/day of partial unavailability at cluster scale**
- Per-session impact: 30s window × 1/(30 days) = **0.001% availability hit**
- **Acceptable for agent workloads** (design §2.1: p99 < 100 ms; LLM dominates).

### Index rebuild cost (when non-persistent)

Current prototype: rebuild on restart from all events in KV + segments.

- 10⁵ concurrent sessions × 10³ events = 10⁸ events per cluster
- Deserialization + HNSW insert at ~50K events/sec per tablet
- 100-tablet cluster: 10⁶ events/tablet → **20 seconds per tablet to rebuild**
- Sequential restart across cluster: 100 tablets × 20s = **33 minutes of degraded service**

**This is intolerable.** Persistent + incremental indexing (M16) is P0, confirmed by BoTE.

## Binding constraints — summary

| Binding? | Constraint | Implication |
|---|---|---|
| **Yes** | S3 PUT rate without batching | WAL batching must ship; not named in milestone list — add to M20 or M16.5 |
| **Yes** | Index rebuild on restart | M16 (persistent indexing) is P0, confirmed |
| Borderline | Network at 1 Gbps | Document 10 Gbps requirement for clustered prod |
| No | HNSW memory | Within limits at v2 target |
| No | HLC CAS | Not a contention point |
| No | Postgres heartbeat/lease writes | Trivial at target scale |

## Plan impact

1. **Add WAL batching to the plan.** Either:
   - (a) Roll into **M20** explicitly (the tablet-refactor milestone's scope already includes the write path).
   - (b) Insert new **M16.5 WAL batching** between M16 and M18.
   - Recommend (a) — smaller plan disturbance; M20 is already XL so it absorbs it.
2. **M16 is confirmed P0**, not optional. BoTE independently derives the ~33-minute cold-start which is a dealbreaker.
3. **Document the 10 Gbps networking assumption** in §14 (deployment modes) — cluster-prod config note.
4. **No milestone re-ordering.** BoTE does not flip M14→M15→M16 order. The foundation phase remains HLC → tracing+agent+hedged → persistent-indexing → archive-read, in that order.

## What was not modeled

- Fanout-query recall math (belongs in M24)
- Slot-level data migration duration for split operations (belongs in M25)
- Cold-start storm (99% cold tenants waking up simultaneously — a design §2.8 scenario); deferred, probably not v2
- Multi-region (non-goal, confirmed §9)

## Conclusion — proceed with M14

BoTE confirms the plan is directionally correct and identifies one scope addition (WAL batching inside M20) and one policy note (10 Gbps for clustered prod). No reshuffle.

**M14 (HLC wiring) is unblocked. Start development.**
