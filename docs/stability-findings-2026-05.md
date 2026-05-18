# AgentDB Stability Findings — 2026-05

Catalog of bugs surfaced by the May 2026 testing campaign. Status as of
2026-05-18 after PRs #272 / #273 / #274 landed on `main` (sha `8b89250`).

This doc is the handoff to whoever picks up the open work. Each
finding is self-contained — a reader should not need to dig through
verification artifacts to know what's broken and where to start.

## Status legend

- 🔴 **P0** — data loss / corruption / safety violation
- 🟠 **P1** — availability loss; manual recovery required
- 🟡 **P2** — observable degradation; auto-recovers
- 🟢 **P3** — minor anomaly worth noting
- ✅ **CLOSED** — fix landed and validated end-to-end on the SUT

## Index

| ID | Severity | Status | One-liner |
|---|---|---|---|
| F1 | P1 | ✅ CLOSED (PR #273) | `register_skill` / `bind_skill` bypass idempotency persist fallback on cache miss |
| F2 | P1 | ✅ CLOSED (PR #273) | Record-index outbox head-of-line-blocks the whole queue when one record is purged out from under it |
| F4 | P1 | ✅ CLOSED (PRs #272 + #274) | 3-node deterministic-surface + chaos: append seq=N times out, retries, then is never visible |
| F_seq_visibility | P0-candidate | ✅ CLOSED (PR #274) | Post-F1/F2: `--chaos` returned 2xx with seq never visible. Was suspected cache-fallback returning unverified records — PR #274 added typed `ConvergencePending`-style retry + visibility repair |
| F_postgres_pool_starvation | P1 (upgraded) | 🟠 OPEN | 60s sqlx slow-statement WARNs on routine SELECT on `placement_scopes` / `tablet_replicas` / `cells` / `tablet_ranges`. Reproduces on 3-node, not just 6-node. Root cause of F_LEUSS. PR #274's pool-size bumps did not eliminate it. |
| F_LEUSS | P1 | 🟠 OPEN (NEW) | Lease Expiry Under SQL Starvation. The 60s sqlx stall exceeds `AGENTDB_SCHEDULER_LEASE_MS=30s`; reaper clears expired tablet ownership; cluster goes ownerless; smoke fails with `tablet N metadata owner was not observable within 90s`. Blocks `--full-hardening` and `--two-cell` runs. |
| F_replacement_wedge | P1 | 🟠 OPEN (BLOCKED) | Tablet replacement deadlocks after rolling-restart in full-hardening (`raft-rs node N did not become leader` ↔ `tablet 1 already has a pending membership change`). **Cannot be independently validated** until F_LEUSS is fixed, because `--full-hardening` now dies at the changefeed phase before reaching `--replacement`. |
| F_rocksdb_lock_flake | P3 | 🟠 OPEN (NEW) | `raft_rs_failure_injection_recovers_old_leader_with_snapshot_after_leader_loss` panics with `RocksDb LOCK: No locks available` under parallel `cargo test`; passes with `--test-threads=1`. CI flake risk. |

## Reproducibility notes (read once)

- **SUT base for validation runs:** `main` at sha `8b89250` ("Fix chaos
  ownership lease stability (#274)"), which carries PRs #272 + #273 +
  #274. Local worktree:
  `/Users/lishen/work/agentdb-stability-2026-05`. Remote worktree:
  `ssh li-dev:/home/lishen/work/agentdb-stability-2026-05`.
- **Environment that reproduces compose-bound findings:** Linux dev
  box (`ssh li-dev`), Docker compose v5.1.3, the SUT's own
  `ops/smoke/scripts/real-cluster-smoke-compose.sh` stack (Postgres +
  MinIO at PG=55432, MinIO=19000), `tools/agentdb-cluster-smoke`
  binary, AgentDB nodes as loopback processes on per-node port
  offsets. Toxiproxy 2.12.0 + Go 1.22 + libfaketime + leiningen +
  Elle + wrk2 + cargo-fuzz also installed for the latest run.
- **Environment that reproduces F1/F2 (closed):** Standalone Rust
  sibling-crate harness in-process against `agentdb-core`, AND
  in-tree integration tests added in PR #273. Both <1 second.
- **What still does NOT reproduce locally on macOS without Docker:**
  every compose-bound finding (F4, F_seq_visibility,
  F_postgres_pool_starvation, F_LEUSS, F_replacement_wedge).
- **Verification campaign reference:** session dirs at
  `~/work/distributed-testing-skills/verification/agentdb-projectwide-lidev-v2/test-sessions/20260518T055437Z/`
  (latest) and earlier sessions under sibling directories.

---

## F1 — `register_skill` / `bind_skill` bypass idempotency persist fallback

**Status:** ✅ CLOSED — landed as PR #273 ("Complete stability F1/F2 fixes")

**Severity:** P1 (safety / idempotency violation in HA shape)

**What happened**

Original fix in `fab7d9d` wired the new `lookup_idempotency_record`
helper into the three `append` call sites only; missed four sites in
`register_skill` and `bind_skill` (`runtime.rs:3579, 3587, 3794, 3802`)
which kept calling `self.idem.lookup(full)` directly. Cache-cold
retries in multi-node HA could create divergent skill/binding rows.

**Validation**

`crates/agentdb-core/tests/runtime_restart.rs::register_and_bind_skill_replay_via_persist_after_cache_drop`
passes on `main` at `8b89250`. Re-verified 2026-05-18.

---

## F2 — Record-index outbox stalls behind purged record

**Status:** ✅ CLOSED — landed as PR #273

**Severity:** P1 (durability + liveness; silent write-invisibility)

**What happened**

Three `return Ok(applied)` sites in `apply_record_index_outbox_once`
at `runtime.rs:7573, 7588, 7620` head-of-line-blocked the whole
mutation queue when a record was purged out from under it. The
state-gap-but-persist-present case (the `53b9c7e` work) was handled,
but the "both gone" case was not.

**Validation**

`crates/agentdb-core/tests/runtime_restart.rs::record_index_outbox_skips_fully_purged_record_without_blocking_queue`
passes on `main`. New counter `record_index_outbox_skipped_total`
visible on `RecordIndexStats`. Re-verified 2026-05-18.

---

## F4 — 3-node deterministic-surface + chaos: append seq=N invisible after retry

**Status:** ✅ CLOSED — by combination of PR #272 ("Prevent append seq
reuse after stale runtime state") and PR #274 ("Repair idempotent
append replay visibility" + "Return raft leader retry hints for
follower writes")

**Severity (was):** P1

**What happened**

3-node `deterministic-surface --consistency --record-index
--changefeed` failed at append seq=N with `raft-rs command did not
commit for tablet 1 before timeout` → retry → `expected appended seq
N to become visible`. Same failure family as the split/merge `seq=14
vs 15` regression but at smaller scale without split/merge.

**Validation**

Re-run on `main` at `8b89250`: the 60s sqlx stall that triggers the
raft-commit-timeout retry STILL fires (see F_postgres_pool_starvation
below), but the retried seq now becomes visible on all replicas and
the smoke reports `real cluster smoke passed`. The visibility
correctness symptom is gone.

**Note**

The underlying instability that triggered F4 (the sqlx stall) is
still present. PR #272/#274 fixed the post-retry correctness, not the
root-cause stall. See F_postgres_pool_starvation below.

---

## F_seq_visibility — `--chaos` 2xx-with-invisible-seq

**Status:** ✅ CLOSED — landed as PR #274 ("Repair idempotent append
replay visibility" + new `ConvergencePending`-style typed retry)

**Severity (was):** P0-candidate

**What happened**

After PR #273 fixed F1/F2, `--chaos` runs exposed a stronger failure:
the append retry-after-convergence-error path returned 2xx but the
seq was never observed at any replica within the post-commit poll
budget. Suspected interaction with the idempotency persist-fallback
returning a cached `(seq, event_id)` whose underlying raft entry had
not committed.

**Validation**

3-node `--chaos --chaos-iterations 6` on `main` at `8b89250`: 14
appends across 6 `kill -9` iterations; 5 hit the retry-after-
convergence path; every retried seq was confirmed visible at the
default 50-attempt visibility-poll budget. Smoke reports
`real cluster smoke passed`. Default budget PASS — no need to
disambiguate at 100× budget.

PR #274 also shipped a typed retryable error the client can react to:

```
INTERNAL "idempotency replay for session ... seq N is not yet visible"
  retryable:true suggestion:"retry the append with the same Idempotency-Key"
```

This is the API-level mitigation the original bug doc requested.

---

## F_postgres_pool_starvation — sqlx slow statements on routine meta SELECTs

**Status:** 🟠 OPEN (severity upgraded from P2 to P1)

**Severity:** P1 — root cause of F_LEUSS (which is itself P1)

**What happened (refined from the original doc)**

`sqlx::query` WARN at 60.008 s (very precise) on routine SELECT on
`placement_scopes` / `tablet_replicas` / `cells` / `tablet_ranges`.
The warning is on **statement runtime** (the query actually ran 60s),
not pool acquire. Fires on 3-node loads, not just 6-node (the
original doc's hypothesis was wrong about the cluster-size threshold).

The chain it drives:

1. Pool starvation or table contention causes a routine SELECT to
   stall 60s
2. The stalled SELECT is on the `renew_local_tablet_ownership` path
   (`bin/agentdb-server/src/main.rs:3756`), which runs every 10s
   with `AGENTDB_SCHEDULER_LEASE_MS=30s`
3. 60s stall ≫ 30s lease → renewal misses its window → lease expires
4. Reaper runs every 10s with 5s safety margin → after lease + 5s =
   35s past expiry it sweeps and clears ownership
   (`clear_expired_ownership` in `crates/agentdb-meta/src/tablets.rs:173`)
5. Cluster goes ownerless. Smoke's `wait_tablet_owner` (90s timeout)
   fires. → This is F_LEUSS.

It also drives `raft-rs command did not commit for tablet 1 before
timeout` on every cluster scenario in the latest campaign (the F4
symptom was fixed; the underlying instability was not).

**What PR #274 changed (and why it wasn't enough)**

- `AGENTDB_META_ANY_POOL_MAX_CONNECTIONS`: 16 → 32 (default)
- `AGENTDB_META_PG_POOL_MAX_CONNECTIONS`: 4 → 16 (default)
- compose Postgres `max_connections`: 200 → 400 (default)
- Atomic rebalance assignment (good for ownership stability)

These raise pool sizes, which would help if the symptom were *pool
acquire* timing out. But the symptom is *statement runtime* — the
query itself takes 60s. Bigger pool doesn't make individual queries
faster. Pool sizing addresses the wrong layer.

**Hypothesis on root cause**

For a Postgres SELECT on a small table to consistently take 60.008s:

1. **Lock contention is unlikely for routine reads.** Postgres
   `SELECT` (non-`FOR UPDATE`) doesn't block on row-level locks. It
   would only block on `ACCESS EXCLUSIVE` (DDL, `LOCK TABLE`,
   autovacuum full).
2. **Postgres CPU / IO saturation** is plausible — the smoke binary
   drives a lot of concurrent writes, and the 60s precision suggests
   a per-statement deterministic cap somewhere. Worth checking
   `statement_timeout` and any application-side `set_statement_timeout`.
3. **Connection storm** — if the 32 pool slots are all blocked on
   slow statements, new acquires queue. But the WARN is on
   "slow_statement", not "slow_acquire", so the statement itself
   is the long pole.
4. **Auto-vacuum or checkpoint** holding briefly + the renewal SELECT
   coinciding with it. Postgres `log_lock_waits` + `log_autovacuum_min_duration`
   would surface this.

The 60.008s precision is suspicious. It is NOT Postgres
`statement_timeout` (which defaults to 0). It IS extremely close to
`sqlx`'s default slow-query threshold of `Duration::from_secs(60)`
for a `acquire_slow_threshold` style warning — but that's pool
acquire, not statement timing. Need to check the exact log message
format to disambiguate.

**Suggested investigation path (in order)**

1. **Disambiguate "statement" vs "acquire" stall.** Look at the
   actual WARN message structure. If it's `sqlx::pool::acquire`,
   it's pool acquire time. If it's `sqlx::query`, it's statement
   time. The bug doc previously said both; we need one clear answer.
   Easy: search `cargo run -p agentdb-server` logs for the WARN
   target name. Grep `crates/agentdb-meta/src/` for any tracing
   config that sets these thresholds.
2. **Capture Postgres-side evidence.** Run the smoke with the compose
   Postgres configured with `log_min_duration_statement=1000`,
   `log_lock_waits=on`, `log_autovacuum_min_duration=0`, then grep
   the Postgres log during a reproduction. Likely candidates:
   autovacuum running, advisory locks, schema migration overlap.
3. **Cache `placement_scopes` in memory.** The table almost never
   changes. Re-reading it on every renewal tick (every 10s × N
   nodes) is wasteful and exposes the renewal to whatever the
   statement-time worst case is. Add a TTL'd in-process cache with
   invalidation on writes.
4. **Bound the renewal path with `tokio::time::timeout`.** Wrap each
   meta query in the renewal critical path with
   `tokio::time::timeout(Duration::from_secs(5), ...)`. If the query
   exceeds 5s, log a structured error and use the previous-tick's
   cached value rather than blocking. This bounds the worst-case so
   the lease never expires under SQL starvation.
5. **Move the renewal off the synchronous-read path.** The renewal
   only needs to write a row (`UPDATE tablets ... SET
   lease_expires_at_ms = ...`), not read first. Refactor to write
   optimistically and reconcile separately.

**Mitigations (in increasing order of safety)**

- **Tactical (config-only):** Raise `AGENTDB_SCHEDULER_LEASE_MS` to
  ≥120s (4× the worst observed query time) and
  `AGENTDB_REAPER_SAFETY_MARGIN_MS` to ≥60s. This stops the reaper
  from clearing ownership during a 60s stall. Doesn't fix the root
  cause, but unblocks F_replacement_wedge validation and stabilizes
  6-node + 2-cell runs immediately.
- **Targeted (small patch):** Add `tokio::time::timeout(5s, …)` on
  every meta query in `renew_local_tablet_ownership` and friends.
  Surfaces the underlying slowness as a typed warning instead of a
  lease-expiry chain.
- **Structural:** Cache `placement_scopes` in memory; refactor
  renewal to a write-only fast path.

**Owner candidates**

- `crates/agentdb-meta/src/placement_scopes.rs::list_all` (the
  observed-slow query)
- `crates/agentdb-meta/src/tablets.rs::clear_expired_ownership` +
  reaper config in `bin/agentdb-server/src/main.rs:3884`
- `bin/agentdb-server/src/main.rs:4061
  ::renew_local_tablet_ownership` (the renewal critical path)
- compose Postgres config in `examples/cluster-smoke/docker-compose.yml`

---

## F_LEUSS — Lease Expiry Under SQL Starvation (NEW)

**Status:** 🟠 OPEN

**Severity:** P1 (availability loss; `--full-hardening` and `--two-cell`
cluster runs cannot complete)

**What happened**

Direct downstream of F_postgres_pool_starvation. Under sustained 60s
sqlx slow statements:

- `AGENTDB_SCHEDULER_LEASE_MS=30s` is exceeded by the stalls.
- The scheduler's `renew_local_tablet_ownership` cannot refresh the
  lease (the renewal SELECT is one of the stalled queries).
- The reaper sweep on the scheduler leader node clears the expired
  ownership (`scheduler cleared expired tablet ownership
  node_id=node-e tablets=[1|2]`).
- Cluster goes ownerless. Smoke binary's `wait_tablet_owner` (90s
  timeout) fires with `Error: tablet N metadata owner was not
  observable within 90s`.

Observed on `--full-hardening --nodes 6 --two-cell` during the
changefeed second-stream phase, before the smoke ever reaches the
`--replacement` step. This is what now blocks F_replacement_wedge
from being validated.

**Reproducer**

```bash
ssh li-dev 'cd /home/lishen/work/agentdb-stability-2026-05 && \
  ops/smoke/scripts/real-cluster-smoke-compose.sh \
    --full-hardening --nodes 6 --two-cell'
```

~6 minutes per phase; F_LEUSS fires during second-stream changefeed.

**Mitigations (same order of safety as F_postgres_pool_starvation)**

- **Tactical:** Raise `AGENTDB_SCHEDULER_LEASE_MS` ≥ 120s and
  `AGENTDB_REAPER_SAFETY_MARGIN_MS` ≥ 60s. This is a one-line config
  change that lets `--full-hardening` complete on starvation-prone
  environments while the structural fix is designed.
- **Structural:** Whatever fixes F_postgres_pool_starvation closes
  F_LEUSS as a side-effect.

**Why this is its own finding rather than a child of F_postgres_pool_starvation**

The chain is observable independently — even if pool starvation is
only momentary (say 35s instead of 60s), the 30s lease + 5s margin
means the reaper still fires. A robust system should renew leases
even when the meta layer is briefly slow. So this finding stays
separate; even partial fixes to pool starvation might still leave
F_LEUSS reachable.

**Owner candidates**

- `bin/agentdb-server/src/main.rs:4061::renew_local_tablet_ownership`
- `bin/agentdb-server/src/main.rs:33-37` (default lease + reaper
  constants — raising defaults is the tactical fix)

---

## F_replacement_wedge — Tablet replacement deadlocks (BLOCKED)

**Status:** 🟠 OPEN, **BLOCKED on F_LEUSS** to validate

**Severity:** P1

**What happened**

Original signature: `--full-hardening --nodes 6 --two-cell` cycled
between `raft-rs node N did not become leader` and `tablet 1 already
has a pending membership change`, never auto-recovering. Hypothesis:
membership-change not idempotent under retry.

**Why it cannot be validated right now**

The latest campaign's `--full-hardening` run died at the changefeed
second-stream phase with the F_LEUSS signature, BEFORE the
`--replacement` step in the smoke's fixed sequence
(`consistency → record-index → changefeed → restart_persistence →
checkpoint_replacement → split_merge → az_failure → rolling_restart
→ replacement`). PR #274's "Realign rebalance ownership after
assignment" is a candidate fix, but cannot be assessed.

**Re-validation path**

When F_postgres_pool_starvation (or at least F_LEUSS) is fixed:

```bash
ssh li-dev 'ops/smoke/scripts/real-cluster-smoke-compose.sh \
  --full-hardening --nodes 6 --two-cell'
```

Check whether the original cycle reappears in the `--replacement`
phase.

---

## F_rocksdb_lock_flake — Test-infra CI flake (NEW)

**Status:** 🟠 OPEN

**Severity:** P3 (CI flake; not a SUT correctness issue)

**What happened**

`raft_rs_failure_injection_recovers_old_leader_with_snapshot_after_leader_loss`
panics with `RocksDb LOCK: No locks available` when run as part of
parallel `cargo test -p agentdb-core --tests`. Passes with
`--test-threads=1`. Same pattern observed in `cargo test -p
agentdb-api --tests` (re-ran with `--test-threads=2` and PASSed).

**Hypothesis**

Two test cases under the same crate open a `RocksKvStore` against the
same path concurrently. RocksDB's per-directory file lock conflicts.
Either tests share a TempDir registry (the `open_kv` pattern in
`crates/agentdb-core/tests/runtime_restart.rs`) without locking, or
two unrelated tests happen to mint the same path.

**Suggested fix**

- Audit `cfg(test)` `open_kv` / TempDir code paths in
  `crates/agentdb-core/tests/` for shared-registry concurrency.
- Either guard with a mutex or use `cargo test
  --test-threads=1` for the affected test binaries in CI.

---

## How to update this doc

When you ship a fix for an OPEN finding, in the same commit:

1. Flip the `Status` line from `🟠 OPEN` (or whatever) to
   `✅ CLOSED (<commit-or-PR>)`.
2. Update the Index table at the top.
3. Add a "Validation" sub-section under the finding citing the
   run that confirmed the fix.

If a finding turns out to be a docs / config issue rather than a
code bug, leave it in the catalog but flip status to
`✅ DOCUMENTED` or `✅ CONFIGURED` so future readers don't re-
investigate.

## Source of evidence

Full session logs, per-scenario findings, and harness Cargo crates
live locally under
`~/work/distributed-testing-skills/verification/` (gitignored;
not in this repo). Most recent campaign:
`agentdb-projectwide-lidev-v2/test-sessions/20260518T055437Z/`.
