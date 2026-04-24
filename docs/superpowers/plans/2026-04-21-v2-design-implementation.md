# AgentDB v2 — Implementation Plan

**Date:** 2026-04-21
**Status (2026-04-24):** substantially complete. M14 (HLC), M15
(observability + agent schema), M16 (persistent indexing), M17
(archive reads), M18 (control-plane meta store — SQLite-only by
decision), M19 (SCRAM + tenant-implicit API), M20 (shard/tablet
routing + Location Proxy), M21 (lease + fencing), M22 (scheduler
+ drain + stale-lease reaper), M23 (per-session locking +
admission), M24 (fanout + over-fetch), M25 (hot-tablet detect +
shed + dynamic tablet create), M26 (deployment modes: `--dev`,
docker-compose, Helm skeleton), and M27 (operational surface:
/health, admin CLI, error codes + doc URLs, runbooks) all land in
the prototype. Additional post-v2 hardening shipped along the way:
REST + Mongo TLS, admin audit log, per-tenant rate limits,
slow-query log, point-in-time backup + scheduler + retention,
admin-token RBAC scopes, tabbed dashboard, hot-reload,
Mongo update/delete, and materialized (folded) reads.

**Single remaining v2 gap:** cross-node Mongo wire forwarding.
Tracked in `2026-04-23-m20-cross-node-forwarding.md`. Clients
targeting a session on a peer node receive
`code: 10107 NotWritablePrimary` with the owner's address so
they can reconnect; a server-side forwarding proxy is deferred
because the Mongo wire protocol is binary + stateful (SASL
session, cursor continuations) and each command needs a
forwarding policy.

Smaller explicit deferrals are captured in
`README.md § Still out of scope` — those stay tracked for
operator visibility rather than becoming invisible tech debt.

**Source of truth for design:** [`docs/agent-db-v2.md`](../../agent-db-v2.md)
**Supporting notes:**
- [`notes/2026-04-20-placement-unit-review-response.md`](../notes/2026-04-20-placement-unit-review-response.md)
- [`notes/2026-04-21-architecture-review-decision-matrix.md`](../notes/2026-04-21-architecture-review-decision-matrix.md)
- [`notes/2026-04-21-hlc-clock-decision.md`](../notes/2026-04-21-hlc-clock-decision.md)

This is the execution plan for the v2 architectural refactor. It translates the design doc's §11 phase sketch into concrete, milestone-sized units that are independently mergeable. Each milestone gets its own detailed plan file (same repo convention as M1–M12) when it is picked up; this document is the meta-plan — sequence, dependencies, scope, acceptance.

---

## 1. Guiding principles (inherited from design)

Every milestone below honors these. Any milestone that cannot is a sign the design or the milestone is wrong, not that the principle is wrong.

1. **Same binary, three deployment modes** (design §14). Dev/single-node/clustered differ only in config. No compile-time feature flags separating dev and prod.
2. **No component on the data-plane hot path** (design §13 invariant a). Postgres and the scheduler are off the critical path.
3. **Fencing tokens, not consensus** (design §5.5). Lease + epoch replaces per-tablet Paxos/Raft for v2.
4. **Object storage is the durable source of truth** (design §5.1). Every other store is a cache or an index.
5. **Each milestone leaves the tree runnable.** No half-merged state; if a milestone is on a long-lived branch, it is still `cargo build && cargo test` green.
6. **No milestone may violate a §13 invariant**, even temporarily. If a PR needs to, the design changes first.

---

## 2. Pre-flight gate — back-of-envelope capacity analysis

Before starting M14, do the numbers. This is not a milestone; it is a **gate**.

Inputs:
- Target tenants (10⁶? 10⁷?)
- Target sessions per tenant per day
- Target events per session, bytes per event
- Target p99, p99.9 latency budgets
- S3 PUT/GET rate limits for the intended bucket sharding

Outputs (written to `docs/superpowers/notes/2026-04-21-bote-capacity.md` when complete):
- Binding constraint identified (S3 PUT rate? index rebuild cost? network? CPU?)
- Per-tablet throughput target
- Per-node tablet-count target
- HLC write-rate: does atomic CAS contend?
- Failover-window budget check against the 30s lease-expiry design

**Why this gates everything:** BoTE can re-order the milestone list. If the binding constraint is S3 PUT rate, then WAL batching jumps to M14.5 and blocks everything else. If the constraint is index rebuild cost, M16 jumps ahead of M15. Do not guess — measure.

---

## 3. Dependency graph

```
BoTE (gate)
  │
  ▼
┌──────────────────────────────────────────┐
│  Foundation — no tablet-unit dependency  │
│                                          │
│   M14  HLC wiring                        │
│   M15  Tracing + hedged reads + agent    │
│   M16  Persistent + incremental index    │
│   M17  Archive-read semantic completion  │
└──────────────────────────────────────────┘
  │
  ▼
┌──────────────────────────────────────────┐
│  Control plane + identity                │
│                                          │
│   M18  Postgres Layer-1 schema           │
│   M19  Tenant-implicit API + SCRAM       │
└──────────────────────────────────────────┘
  │
  ▼
┌──────────────────────────────────────────┐
│  Tablet refactor (the big one)           │
│                                          │
│   M20  Shard / Tablet / Node routing     │
│         (Location Proxy, key layout)     │
│   M21  Lease + fencing                   │
│   M22  Scheduler + failover              │
│   M23  Per-session locking + admission   │
│   M24  Fanout coordinator + recall       │
└──────────────────────────────────────────┘
  │
  ▼
┌──────────────────────────────────────────┐
│  Scale + ops                             │
│                                          │
│   M25  Hot-tablet auto-split             │
│   M26  Deployment modes (dev / compose)  │
│   M27  Operational surface               │
└──────────────────────────────────────────┘
```

- **M14–M17** are independent of each other inside their phase; can be parallelized across workers.
- **M20** is the single largest milestone and is the critical path.
- **M21–M24** can be parallelized once M20 lands.
- **M26–M27** are cross-cutting and can start as early as M18 in draft form.

---

## 4. Milestones

Each entry: scope → acceptance → estimate (S ≈ 1 wk, M ≈ 2 wk, L ≈ 3–4 wk) → key risk.

### M14 — HLC wiring

**Scope:**
- `HlcClock` struct: `(physical_ns: u64, logical: u32)`, atomic CAS updates, textbook HLC algorithm.
- Max-offset guard (`AGENTDB_HLC_MAX_OFFSET_SECS`, default 10); rejected updates increment `hlc_offset_rejects`.
- Event schema gains `hlc_ts` field — **segment format breaking change**; include schema-version bump and migration path for existing prototype segments.
- Wire propagation: `X-AgentDB-HLC` header on REST; Mongo `$clusterTime` ↔ HLC mapping on OP_MSG.
- `ReadCtx` gains `hlc_cutoff`; read filters `event.hlc_ts <= ctx.hlc_cutoff`.
- Metrics: `hlc_physical_offset_ns` gauge, `hlc_offset_rejects` counter.
- Admin: `GET /api/admin/hlc` returns current HLC, wall clock, offset.

**Acceptance:** new events carry HLC; snapshot reads filter correctly; Mongo `$clusterTime` round-trips; max-offset guard rejects an injected 1-hour-future HLC; integration test with 5s clock-skew convergence.

**Estimate:** M
**Risk:** segment format migration. Mitigate: schema-version header + forward-compat reader.

---

### M15 — Observability foundation + agent schema

**Scope:**
- **Part A — OpenTelemetry.** Tracer wiring through `agentdb-wire`, `agentdb-api`, `agentdb-core`, `agentdb-storage`. Every request carries a trace ID; fanout spans are children. OTel SDK drop-in; exporter configurable (OTLP endpoint default, console for dev).
- **Part B — Hedged reads.** Cold-segment read path fires a secondary request after per-tier δ (50 ms cold, 200 ms archive). Winner is taken; loser is cancelled. Metrics: `hedged_triggers`, `hedged_winners{primary|secondary}`.
- **Part C — Agent schema fields.** `actor_id` on `Event`; `agent_id` on `Session`. Secondary index on `agent_id` for cross-session queries. No API-level resource (see §2.3 of design).

**Acceptance:** traces visible in Jaeger from wire to storage; hedged-read secondary-wins count > 0 under slow-S3 simulation; agent-filtered query returns expected sessions.

**Estimate:** M
**Risk:** low.

---

### M16 — Persistent + incremental indexing

**Scope:**
- Swap `instant-distance` for a library with incremental insert + serialization (candidates: `hnswlib-rs`, `usearch`). Decision: sign off after a one-day spike comparing build speed, query speed, incremental-insert support, thread safety.
- HNSW snapshot serialization to object storage under `indexes/{tablet_id}/hnsw-{epoch}.bin`.
- Incremental insert in the write path (under session lock).
- Recovery: on tablet startup, load latest snapshot + replay WAL events emitted after snapshot's HLC.
- BM25 inverted-index also persisted; same snapshot-plus-replay pattern.
- Persistence runs async via a periodic job (hourly default) and before tablet drain.

**Acceptance:** restart a tablet holding 10M vectors → ready for queries in < 10s (vs current O(minutes) rebuild). Recall@10 unchanged vs pre-swap baseline.

**Estimate:** L
**Risk:** backend library choice. Mitigate: spike first, don't commit to the library until acceptance numbers clear.

---

### M17 — Archive-read semantic completion

**Scope:**
- `fetch_last_n` on a forked-archived parent reads through segments (today returns child-only).
- Mongo `find` on archived sessions reads through segments (today returns empty).
- Locality-bound planner paths resolve against segments for archive (today returns empty).
- Unified cold/archive read helper that the above three paths share.

**Acceptance:** test matrix — all API surfaces × all tiers return semantically correct results; no path returns empty purely because of tier.

**Estimate:** S–M
**Risk:** low. Largely completion of M12 rather than new design.

---

### M18 — Metadata Layer 1 (Postgres control plane)

**Scope:**
- Postgres schema: `tenants`, `shards` (16384-row assignment array), `tablets` (assignment + epoch + lease), `nodes` (membership, heartbeat), `config`.
- Embedded migrations via `refinery` or `sqlx::migrate!`; idempotent, forward-only.
- **SQLite dialect** adapter that serves the same schema for dev mode (Postgres dialect for prod). Dialect differences hidden behind a trait.
- `LISTEN/NOTIFY` plumbing on the Postgres side (no consumer yet in this milestone; wiring only).
- Smoke test: start fresh Postgres → migrations apply → tables exist; start fresh SQLite → migrations apply → tables exist.

**Acceptance:** migrations apply cleanly on empty Postgres 14/15/16 and on fresh SQLite; repeat starts are no-ops; schema dump is version-controlled.

**Estimate:** M
**Risk:** SQLite/Postgres dialect drift (e.g., `LISTEN/NOTIFY` has no SQLite equivalent — use polling fallback for SQLite, acceptable in dev mode).

---

### M19 — Tenant-implicit API + SCRAM

**Scope:**
- SCRAM-SHA-256 authentication in `agentdb-wire`; user record maps 1:1 to tenant.
- `$db` in OP_MSG treated as sub-namespace inside the authenticated tenant, not the tenant selector.
- Native REST: Authorization bearer token → tenant lookup.
- Request payloads stop carrying `tenant_external_id`; handlers pull tenant from auth context.
- Breaking-change migration note for any existing clients.

**Acceptance:** Mongo driver with SCRAM works against `tenant-alpha`; attempting to address `tenant-beta` data via `$db = "tenant-beta"` fails with auth error; old payload format rejected cleanly with a documented error code.

**Estimate:** M
**Risk:** auth-protocol bugs. Mitigate: fuzz SCRAM handshake, cross-test with 3 popular Mongo drivers.

---

### M20 — Shard / Tablet / Node routing refactor (critical path)

**Scope (largest single milestone):**
- `Shard` concept: `S = 16384` fixed, `shard = hash(session_id) % S` via FNV-1a/XXHash3 with fixed seed.
- `Tablet` as runtime unit owning an arbitrary subset of shards (not necessarily contiguous).
- `Node` as host of one or more tablets.
- Key layout change: `{tablet_id}/...` replaces `{box_id}/...`.
- **Location Proxy** component (§3.6 of design): computes shard, reads cached assignment, refreshes on staleness, exposes metrics.
- Fanout coordinator skeleton (no recall tuning yet — that's M24).
- Migration tool for existing box-based prototype data into the new shard/tablet layout.
- All existing tests ported to new routing; no regressions.

**Acceptance:** prototype test suite passes under new routing; migration tool converts a sample prototype cluster's data without loss; placement by `hash(session_id)` distributes sessions evenly across a 16-tablet synthetic cluster.

**Estimate:** L–XL (plan for 4–6 weeks, not 2–3)
**Risk:** highest of the plan. Data migration from prototype format is the biggest unknown. Mitigate: write the migration tool and test on prototype data *before* starting the routing refactor.

---

### M21 — Lease + fencing tokens

**Scope:**
- Lease table in Postgres: `tablet_id`, `holder`, `expires_at`, `epoch`.
- Renewal loop per tablet: every `LEASE_RENEW_SECS` (default 10), conditional UPDATE.
- Owner verifies lease before every write batch; expired → stop writes immediately, surrender tablet.
- Proactive stop-writing when Postgres is unreachable longer than `LEASE_DURATION - LEASE_RENEW` (default 20s).
- Epoch tagging on object-storage paths: `wal/{tablet_id}/epoch={N}/...`, `segments/{tablet_id}/epoch={N}/...`.
- Epoch tagging on Postgres conditional writes: `WHERE current_epoch = $self_epoch`.
- Readers honor only latest epoch for any logical position.
- Chaos-test harness: induced network partition between owner and Postgres; verify no split-brain writes survive.

**Acceptance:** fault-injection test — partition owner, force scheduler reassignment, reconnect old owner → no split-brain writes persist (old-epoch writes either fail at Postgres conditional update or are filtered at read time).

**Estimate:** M
**Risk:** medium; correctness-critical. Invest in fault-injection harness early.

---

### M22 — Scheduler + failover orchestration

**Scope:**
- Scheduler leader election via `pg_advisory_lock` on a fixed lock id.
- Failure detection: periodic scan of `nodes` table for stale `last_heartbeat`.
- Tablet reassignment policy: prefer cross-AZ target (§2.7), respect node weights, avoid target already hosting the tablet's peers (anti-affinity if configured).
- Reassignment must wait `old_lease_expiry + SAFETY_MARGIN_SECS` (default 5) before assigning new owner.
- `agentdb drain` CLI command: migrate this node's tablets to peers, then exit.
- Rolling upgrade: documented runbook + CI test with two binary versions coexisting.

**Acceptance:** kill a node in the test cluster → tablets failover to other nodes within p95 < 30s, data intact. Drain a node gracefully → peers absorb tablets without failover window.

**Estimate:** M–L
**Risk:** medium; scheduling policies have operational surprises. Mitigate: integration test with multi-AZ sim.

---

### M23 — Per-session locking + tablet admission

**Scope:**
- Replace `Box_::write_lock` `Mutex` with `DashMap<SessionId, Arc<Mutex<()>>>` + LRU eviction of cold entries.
- Tablet-level admission: bounded-capacity semaphore.
- Ordered acquisition for rare cross-session atomic writes (ascending session id).
- Metrics: `lock_contention_wait_ns` histogram, `tablet_admission_queue_depth` gauge.

**Acceptance:** concurrent writes to N different sessions on the same tablet run in parallel (measured); single-session writes still serialize correctly; cross-session test deadlock-free under stress.

**Estimate:** S–M
**Risk:** low. Straightforward refactor but correctness-critical for the hot path.

---

### M24 — Fanout coordinator with recall tuning

**Scope:**
- Over-fetch factor `f` (default 2, configurable).
- Each of `M` tablets returns top-`K·f` candidates.
- Coordinator reranks `K·f·M` candidates globally; returns top-K.
- HLC cutoff propagates to all fanout targets.
- Recall@10 benchmark on the sample-app workload.
- Per-query-type `f` overrides (e.g., text-heavy queries may need lower `f`, vector-heavy may need higher).

**Acceptance:** recall@10 ≥ baseline (single-tablet) at `f = 2` across 16-tablet fanout. Degradation plot at `f = 1, 2, 4, 8` documented.

**Estimate:** M
**Risk:** medium; recall math may require tighter tuning than expected.

---

### M25 — Hot-tablet auto-split

**Scope:**
- Detection policy: tablet load (QPS, admission queue depth) and size (warm bytes) crossing configurable thresholds.
- Split mechanism: scheduler picks a shard-subset boundary, creates a new tablet, migrates selected shards' data.
- Shard-level data migration: copy warm state, reassign shard_assignment rows atomically, cut over.
- Auto-split policy runs periodically in the scheduler.
- Per-session override path (design §2.5) for pathological single-session cases.

**Acceptance:** synthetic hot tablet → scheduler detects → split executes → resulting tablets serve correctly; migrated shards' sessions see zero downtime.

**Estimate:** M–L
**Risk:** medium. Split-during-active-writes is the subtle case. Hot-session sub-partitioning (design §10.1 second half) is explicitly v3+; don't scope-creep here.

---

### M26 — Deployment modes

**Scope:**
- `--dev` mode: SQLite backend for Postgres-role, filesystem backend for object-storage-role. Same migration code path. Single-binary. Zero-config launch.
- Config surface: `agentdb dump-config` with per-value provenance (default / env / file / flag).
- `examples/compose/docker-compose.yml`: 3 AgentDB nodes + Postgres + MinIO, fully configured.
- `examples/helm/`: Helm chart skeleton for k8s (not production-hardened, but working).
- Quickstart tutorial in `README.md`: `agentdb start --dev` → `mongosh` → first event, under 2 minutes.

**Acceptance:** `agentdb start --dev` ready in < 1s; `docker compose up` in `examples/compose` brings up a 3-node cluster in < 30s; tutorial steps complete without additional documentation lookups.

**Estimate:** M
**Risk:** low. Mostly packaging and integration.

---

### M27 — Operational surface polish

**Scope:**
- `/health/live` — process + HLC advancing.
- `/health/ready` — Postgres reachable + object storage reachable + all owned tablets lease-holding.
- Admin CLI subcommands (design §15.5): `tablet list / failover / inspect / flush-cache / dump-config / show-lease / drain / hlc`.
- Error-code system: every error response carries stable `code` + `doc` URL.
- First batch of runbooks in `docs/runbooks/`: `E0047 lease_expired`, `E0052 hlc_offset_too_large`, `E0061 tablet_reassignment_stuck`, `E0073 s3_unreachable`, `E0081 postgres_unreachable`. Structure: *Symptom → Diagnose → Verify → Remediate → Prevent*.
- Observability verifies (not reimplements): pointer to how to plug into Jaeger, Prometheus, Datadog. Sample configs.

**Acceptance:** every error surface in integration tests has a runbook URL that resolves. Admin CLI can be used by a fresh operator with only the runbook for orientation.

**Estimate:** M
**Risk:** low.

---

## 5. Cross-cutting work (not milestoned)

Some work happens across multiple milestones. Tracked but not blocked-on:

- **CI matrix**: each milestone adds its integration tests; CI stays green continuously.
- **Benchmarks**: the existing sample-app benchmark (`benchmarks/`) is updated per milestone to exercise new paths. Regressions block merge.
- **Docs**: each milestone updates `ARCHITECTURE.md` (prototype-state-tracking doc) and the relevant section of `agent-db-v2.md` if implementation reveals the design is underspecified.
- **Security review**: SCRAM handshake (M19), fencing tokens (M21), auth/authz (general) get a focused review before merging M19/M21 and then again at v2 readiness.

---

## 6. Explicit non-goals for v2

Carried forward from design §9 and §10; do not let any milestone creep into these:

- TrueTime, global TSO, per-tablet consensus → §10.6, v3+.
- Multi-region active-active → never.
- Full agent-as-resource endpoints → §10.4, v2+.
- User-supplied affinity key → §10.5, v2+.
- Adaptive placement → §9, v2+.
- Variable `S` at runtime → §9, never in a running cluster.
- Separate dev vs prod binaries → §9, never.

---

## 7. Risk register (top three)

1. **M20 is the critical path and it is large.** Data migration from the prototype's `box_id`-keyed format is the load-bearing unknown. Mitigation: write the migration tool first, against prototype data, before any refactor of the routing code.
2. **HNSW backend swap (M16) is externally-dependent.** If neither `hnswlib-rs` nor `usearch` meets our needs, we fall back to building incremental HNSW on top of `instant-distance`'s graph structure — that is an R&D-grade task. Mitigation: spike in week 1; abort to fallback if spike fails.
3. **BoTE may invalidate the milestone order.** If the binding constraint is S3 PUT rate, WAL batching is missing from this plan entirely and becomes a prerequisite. Mitigation: BoTE is the gate; re-plan if the numbers force it.

---

## 8. Rough timeline

Assuming one full-time contributor with occasional reviews, S = 1 week, M = 2 weeks, L = 3–4 weeks, XL = 5–6 weeks:

| Phase | Milestones | Rough weeks |
|---|---|---|
| BoTE | gate | 0.5–1 |
| Foundation | M14, M15, M16, M17 | 2 + 2 + 4 + 1.5 = **~9.5** |
| Control plane + identity | M18, M19 | 2 + 2 = **4** |
| Tablet refactor | M20, M21, M22, M23, M24 | 5 + 2 + 3 + 1.5 + 2 = **~13.5** |
| Scale + ops | M25, M26, M27 | 3 + 2 + 2 = **7** |
| **Total** | | **~35 weeks (≈8 months)** |

With two contributors able to parallelize within phases, wall-clock time drops to ~5 months. With three, ~4 months but coordination overhead rises.

These are engineering estimates, not commitments. BoTE may shorten or lengthen the path.

---

## 9. Plan update discipline

This plan is a living document. When a milestone changes shape (scope, estimate, or ordering), update this file in the same PR that contains the change — not later.

Each milestone's detailed plan file lands in `docs/superpowers/plans/YYYY-MM-DD-m{N}-{name}.md` before implementation starts, following the repo's existing M1–M12 convention.
