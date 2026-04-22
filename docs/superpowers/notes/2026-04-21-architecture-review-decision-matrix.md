# Architecture Review — Decision Matrix

**Date:** 2026-04-21
**Context:** Companion to [`2026-04-20-placement-unit-review-response.md`](./2026-04-20-placement-unit-review-response.md). Consolidates items from both the placement-unit expert review and the Jeff-Dean-style critique into a two-axis decision matrix: implementation cost × regret cost of not doing it.

The goal is to separate "important and cheap" from "important and expensive." The first bucket gets picked up immediately. The second gets planned carefully. The Google-style review instinct to treat everything as P0 is a trap — the matrix breaks it.

---

## The matrix

```
                   Regret if skipped
                   Low             Medium              High
              ┌──────────────┬──────────────────┬───────────────────────┐
  Impl: Low   │  —           │  later / optional│  ▲ DO NOW              │
              ├──────────────┼──────────────────┼───────────────────────┤
  Impl: Med   │  skip        │  schedule        │  must-do, plan it     │
              ├──────────────┼──────────────────┼───────────────────────┤
  Impl: High  │  skip        │  defer, evaluate │  ■ BIG MILESTONE       │
              └──────────────┴──────────────────┴───────────────────────┘
```

A special row sits across this: **"decide now, implement later."** Items where writing down a commitment is cheap (low impl *right now*) but the regret of leaving the commitment open is high. Clock model, failure model, hot-session split semantics all live here.

---

## Bucket A — DO NOW (low impl cost, high regret)

These are the items the user's P0/P1 list under-prioritized because they look like "details" but are in fact the cheapest-per-value items on the whole board.

### A1. BoTE numbers for the target workload — FIRST

Not because it's the most important by itself, but because **it reorders every other bucket.** Spreadsheet, one afternoon. Target: "at 10⁷ tenants × 10² sessions/tenant/day × 10³ events/session × 10⁴ bytes/event, what is the per-layer load?" Output: binding constraints named explicitly.

If BoTE says S3 PUT rate is binding → WAL batching jumps to A-bucket.
If BoTE says index rebuild cost is binding → indexing persistence (C3) becomes the first milestone.
Until BoTE is done, priorities are vibes.

### A2. Distributed tracing (OpenTelemetry)

OpenTelemetry SDK is a drop-in. Once queries fan out across placements + indexes + object store, the difference between having and not having a trace ID is the difference between "grep logs and guess" and "open Jaeger." Low cost, high debuggability payoff. There is no reason to defer this.

### A3. Hedged / tied reads in the cold-segment path

Two reads fired at T and T+δ, first result wins, loser gets cancelled. ~100 lines in the cold-read path. Object storage tails are fat (seconds, occasionally tens of seconds on S3). Without hedging, p99.9 is unbounded and the whole "object storage is primary" bet is exposed.

The prototype's `scan_session_events` / segment-resolve path is the right place to add this. Per-tier timeout (warm: skip hedge, cold: 50 ms δ, archive: 200 ms δ) is plenty.

### A4. Agent schema fields (actor_id on events, agent_id on sessions)

Adding fields now is free. Adding them retrofit later means data migration across all warm + cold + archive segments. The user's own reasoning in the selective-acceptance section: "加字段成本很低、删也没啥事、以后想补会贵." By that reasoning this is A, not C.

Does **not** include: agent registration endpoints, agent-level quotas, agent lifecycle. Those stay deferred (bucket D).

### A5. Explicit "NOT doing" commitments (design statements)

Writing down what we're explicitly not doing is free and prevents drift:

- **Clock model: HLC, not TrueTime, not a global TSO.** Commit to hybrid logical clocks as the design target. TrueTime-class mechanisms are rejected unless a concrete workload proves HLC insufficient.
- **Failure model: object storage is the durable source of truth; node death = replay from object storage within a bounded failover window.** Per-placement Paxos / Raft is rejected as v1 unless the replay window turns out too long under measurement.
- **Native typed RPC as v2+ work.** Mongo wire stays primary for the short term as an adoption layer; the long-term reframe (native RPC primary, Mongo as translator) is documented but not scheduled.

These three statements together prevent the Google-style review from silently escalating scope over the next several months.

---

## Bucket B — SCHEDULE (medium impl, medium-to-high regret)

Middle of the matrix. Real work, not tiny, but bounded scope. Slot into milestones in order.

### B1. Tenant-implicit API surface

**Scope:** Wire layer pulls tenant from SCRAM auth (Mongo) / bearer token (REST). Request payloads drop `tenant_external_id`. Internal code still threads tenant_id for quota + isolation.
**Regret:** Without it, the placement unit refactor is incomplete — hot-tenant routing stays coupled to the API.
**Dependency:** Lands alongside B2, since both touch the same wire handlers.

### B2. Archive read semantics uniform

**Scope:** Complete M12. Three remaining paths: `fetch_last_n` on forked-archived parents, Mongo `find` on archived sessions, locality-bound planner paths. All three must resolve against segments.
**Regret:** Current semantics are inconsistent per-API. User-visible correctness bug class.
**Dependency:** Shares the segment-resolve code path with A3 (hedged reads). Sensible to do them together.

### B3. Per-session write lock

**Scope:** Replace `Box_::write_lock` Mutex with a sharded lock map keyed by session id. Admission control moves to a per-placement semaphore. LRU eviction for the lock map.
**Regret:** Box-wide serialization is a hard throughput ceiling per placement; two active sessions in the same placement serialize needlessly.
**Note:** This is borderline A-bucket (impl cost is low). Kept in B because it only makes sense after placements exist as a distinct concept — otherwise you're just renaming the box lock.

### B4. Fanout retrieval recall (over-fetch factor)

**Scope:** When fanning out a top-K vector query across M placements, each placement returns top-(K·f) for some over-fetch factor f. Coordinator ranks the K·f·M candidates and returns global top-K. Tune f per query type.
**Regret:** Without this, fanout retrieval trades quality for parallelism silently. Mid-priority until retrieval becomes load-bearing for a real workload.
**Dependency:** Lands with the fanout-merge coordinator in C2.

### B5. Micro-sharded segments for tail control

**Scope:** Split large per-session segments into fixed-size chunks on the object store. Reduces the tail impact of any single slow object read.
**Regret:** Reduces p99.9 further beyond what A3 hedging achieves. Medium because A3 already recovers most of the win.
**Defer to:** After A3 ships and measured tail numbers are in hand. Decide whether to do this based on whether hedging is sufficient.

---

## Bucket C — BIG MILESTONE (high impl cost, high regret)

Real engineering efforts. Each needs its own spec + plan.

### C1. Placement unit refactor (M13-scale)

**Scope:** Key layout change (remove `box_id` + `tenant_internal` from event keys, add `placement_id`). Routing rewrite (`box_registry` → `placement_registry`). Session-to-placement binding. Fanout coordinator. Includes B1, B2, B3 as sub-work.
**Regret:** Without it, hot-tenant is a permanent scale ceiling.
**Estimate:** One full milestone. Sequence after A-bucket and A1 BoTE.

### C2. Persistent + incremental indexing (M12.5-scale)

**Scope:** HNSW backend swap (instant-distance → hnswlib or usearch) for incremental insert. Serialization to object storage. Recovery on restart. BM25 inverted-index persistence.
**Regret:** Current index architecture is essentially unusable for retrieval at production scale — every restart pays a full rebuild.
**Note:** Probably the *first* C-bucket milestone, because C1's placement-local indexes inherit this work. Do C2 before C1.

### C3. Hot-session split mechanism

**Scope:** Policy (when to split — load, size), mechanism (how a session splits — new placement, partition boundary), semantics (fork chain across split? fetch_last_n across split?).
**Regret:** Runaway agents §2.8 names as a known failure mode. Without a split mechanism, session-as-locality becomes session-as-bottleneck.
**Design now:** Write down the split semantics. Add to A-bucket as a design note.
**Implement later:** After C1 lands. Real implementation needs placement infrastructure in place.

---

## Bucket D — DEFER (high impl cost, low-to-medium regret in near term)

Items worth noting but not worth committing to now.

### D1. Native typed RPC surface as primary interface

Mongo wire works for adoption. A typed RPC primary is a v2+ product decision. Documenting the long-term shift (in A5) is sufficient for now.

### D2. Full agent-as-resource semantics (endpoints, registration, quota)

Everything beyond the field-level changes in A4. Waits for (a) a clear product direction on multi-agent, or (b) a concrete workload that shows field-level agent support is insufficient.

### D3. Adaptive placement (system auto-migrates sessions for locality)

From the previous note, Option C in placement-declaration-surface. Out of scope for v1 and likely v2.

### D4. Affinity-key user hint

Session-level user-supplied placement routing. P2 per yesterday's note. Revisit when the sample-app benchmark produces cross-session coordination workload data.

---

## Explicit rejections (from the Google-style review)

Writing these down so we don't accidentally re-adopt them under scope drift:

- **TrueTime-class clock infrastructure.** HLC is the design target. Revisit only if HLC breaks under measured load.
- **Per-placement Paxos / Raft consensus for writes.** Object storage is the durable source of truth; replay window is the failover mechanism. Revisit only if replay window proves too long.
- **Global secondary indexes spanning placements.** §3.2 locality rejects this by design; fanout-merge handles cross-placement queries.
- **Full MongoDB wire protocol compatibility.** Defined subset only. Every un-implemented command is an intentional non-goal.

---

## Recommended sequence

Given the matrix:

```
Week 1–2   A1  BoTE numbers (rerank this list after)
            A5  Doc commitments (HLC, OS-as-truth, RPC-as-v2)
            A2  Distributed tracing
            A3  Hedged/tied reads in cold path
            A4  Agent schema fields

Milestone 1 C2  Persistent + incremental indexing
            B2  Archive reads uniform   (pairs with A3)
            B4  Fanout recall           (design only at this stage)

Milestone 2 C1  Placement unit refactor — full
            B1  Tenant-implicit API       (in C1)
            B3  Per-session lock          (in C1)
            B4  Fanout recall impl        (in C1)

Milestone 3 C3  Hot-session split — design lands in Milestone 2, impl here
            B5  Micro-sharded segments  (only if A3 numbers justify)

Deferred    D1  Native RPC primary
            D2  Full agent-as-resource
            D3  Adaptive placement
            D4  Affinity-key hint
```

**Rule of thumb:** don't start Milestone 1 until A-bucket is complete and BoTE numbers have re-ranked the matrix. The whole point of A1 is to prevent building the wrong C-bucket item first.
