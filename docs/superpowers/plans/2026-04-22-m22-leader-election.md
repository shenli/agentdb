# Plan: M22 — scheduler leader election (scoped)

Date: 2026-04-22
Scope: *foundation* of M22's scheduler. Adds a singleton-leader
lease in the control plane so exactly one process in the cluster
runs scheduler work at a time. The loop that *uses* leadership
(stale-node → tablet reassignment, drain orchestration, rolling-
upgrade coordination) is the next PR — this one only lights up
leader election itself and surfaces the result through an admin
endpoint.

## Why

Now that PR #27 registers nodes and PR #28 surfaces staleness,
every node in the cluster has all the information needed to run a
scheduler. But if every node runs one, they'd fight — two nodes
reassigning the same tablet simultaneously would produce an epoch
race that's harmless for correctness (M21's CAS guarantees that)
but wasteful and confusing for operators reading logs.

The design doc (§3.4 and M22 scope) calls for leader election. On
Postgres this is `pg_advisory_lock` on a fixed id; SQLite has no
equivalent, so we use a singleton-row lease with the same
CAS-on-renew pattern M21 already uses for tablet leases. The
mechanism is small enough that rolling our own is simpler than
pulling a dep.

## Scope

In: `crates/agentdb-meta/migrations/002_scheduler_leader.sql` (new),
`crates/agentdb-meta/src/scheduler.rs` (new),
`crates/agentdb-core/src/scheduler.rs` (new),
`bin/agentdb-server/src/main.rs`,
`crates/agentdb-api/src/admin_scheduler.rs` (new),
`crates/agentdb-api/src/lib.rs`.

1. **Migration 002** — adds table `scheduler_leader` with a
   single-row constraint:
   ```sql
   CREATE TABLE scheduler_leader (
       id             INTEGER PRIMARY KEY CHECK (id = 1),
       node_id        TEXT    NOT NULL,
       epoch          INTEGER NOT NULL DEFAULT 0,
       expires_at_ms  INTEGER NOT NULL,
       acquired_at_ms INTEGER NOT NULL
   );
   ```
   The `CHECK (id = 1)` clause makes it impossible to insert more
   than one row — any second INSERT fails the check constraint.

2. **`agentdb_meta::scheduler`** — three functions:
   - `try_acquire(pool, node_id, lease_duration_ms, now_ms) -> Option<LeaderLease>`:
     returns `Some` if no leader exists, or the existing leader's
     lease has expired, and the CAS (on the old row's epoch) wins.
     `None` if a live leader holds it. On first-ever call, inserts
     the row. On takeover, bumps `epoch` by one.
   - `renew(pool, node_id, expected_epoch, new_expires_at_ms) -> MetaResult<LeaderLease>`:
     conditional update, same shape as `tablets::assume_ownership`.
     Fails with `MetaError::Conflict` if someone stole the role.
   - `get(pool) -> MetaResult<Option<LeaderView>>`: raw read of the
     row, whether live or expired. Callers decide based on
     `expires_at_ms < now`.
   - Unit tests: fresh-acquire, contention between two node ids
     (only first wins), expired-then-takeover, renew-after-stolen-
     fails-with-Conflict.

3. **`agentdb_core::scheduler`** — wrapper that:
   - Struct `LeaderElection` holding `meta`, `node_id`, config,
     plus an `AtomicBool is_leader` and an `AtomicI64 current_epoch`.
   - `LeaderElection::start(meta, node_id, config) -> Arc<Self>`:
     spawns a tokio task that alternates:
     - If not leader: try_acquire every `attempt_interval` (default
       2s). On success, flip `is_leader` to true, store epoch.
     - If leader: renew every `renew_interval` (default 5s). On
       success, refresh expiry. On Conflict, flip `is_leader` to
       false and go back to the try-acquire loop.
   - Accessors `is_leader() -> bool` and `current_leader_epoch() -> i64`.
   - Config: `LeaderElectionConfig { lease_duration, renew_interval,
     attempt_interval }`, with `from_env` reading
     `AGENTDB_LEADER_LEASE_SECS` / `_RENEW_SECS` / `_ATTEMPT_SECS`
     (defaults 30 / 5 / 2).
   - One unit test: use two in-memory meta stores pointing at the
     same DB? Actually SQLite `::memory:` is per-connection; use a
     shared temp-file DB for the two-process simulation.

4. **Server wiring** — main.rs spawns `LeaderElection::start` once
   per process, stores the handle as an `Arc<LeaderElection>` in
   `AppState` (or adds it as an axum Extension like PR #28 did for
   the meta store). The latter avoids AppState churn.

5. **`GET /api/admin/scheduler`** — returns:
   ```json
   {
     "this_node_id": "smoke-a",
     "this_node_is_leader": true,
     "leader": {
       "node_id": "smoke-a",
       "epoch": 7,
       "expires_at_ms": 1776884000000,
       "acquired_at_ms": 1776883994000,
       "is_expired": false
     }
   }
   ```
   `leader` is nullable (no leader has ever been elected on a fresh
   cluster). `is_expired` is computed server-side against wall clock.

## Out of scope

- **Stale-node → tablet reassignment.** Next PR. Requires the
  leader loop to *do* something with leadership, plus reassignment
  policy (cross-AZ preference, anti-affinity, SAFETY_MARGIN wait).
- **Drain CLI** (`agentdb drain`). Uses leadership to coordinate
  the drain; separate PR.
- **Rolling-upgrade runbook** + CI test. Separate PR.
- **Leader-guarded endpoints.** None yet — no endpoint currently
  requires leader-only gating. When we add one, the gate reads
  `LeaderElection::is_leader()` and 503s if false.

## Acceptance

- Single server boot: `GET /api/admin/scheduler` returns this node
  as leader within `attempt_interval` seconds.
- Two servers pointing at the same `meta.sqlite`: exactly one
  reports `this_node_is_leader: true`; the other's `leader` field
  still shows the first node.
- Kill the current leader; the second server flips to
  `this_node_is_leader: true` within
  `lease_duration + attempt_interval` seconds.
- `cargo test --workspace` green; `cargo clippy ... -D warnings` clean.
- OpenAPI snapshot regenerated for the new endpoint + response types.

## Shape of the commit

Single PR, single commit:
`feat(meta,server): scheduler leader election via SQLite (M22 foundation)`.
