# Plan: M22 — stale-lease tablet reaper (scoped)

Date: 2026-04-22
Scope: the *smallest useful thing the leader does*. PR #33 elected a
leader; this PR gives the leader one job — clear expired tablet
leases. Full M22 scheduler (cross-AZ reassignment, anti-affinity,
`agentdb drain`, rolling upgrade runbook) remains for follow-ups.

## Why

Today the `scheduler_leader` row tells us who's in charge, but that
leader does nothing. If a node dies, its `tablets` rows stay pinned
to the dead owner with an expired `lease_expires_at_ms`. Nothing
reads that staleness, so:

- `/api/admin/tablets` shows a dead owner indefinitely — operator
  has to eyeball the `lease_expires_at_ms` against wall clock.
- No other node can claim the tablet through the normal acquire
  path without a CAS race (the epoch at the server is still the
  dead owner's last epoch).
- Tenant routing isn't affected (hash-based), but *control-plane
  cleanliness* is.

The reaper is the simplest thing that helps: the leader
periodically scans tablets, and for each tablet whose lease has been
dead longer than `safety_margin`, it clears the owner. That frees
the row for a future acquire and keeps the admin view honest.

## Scope

**Meta (`crates/agentdb-meta/src/tablets.rs`):**

- `clear_expired_ownership(pool, safety_margin_ms, now_ms) ->
  MetaResult<Vec<i64>>`: single bulk UPDATE that clears
  `owner_node_id` (sets to NULL) and `lease_expires_at_ms = 0`, and
  bumps `epoch`, for every row where
  `owner_node_id IS NOT NULL AND lease_expires_at_ms + safety_margin_ms < now_ms`.
  Returns the list of tablet_ids that were cleared (for logging).
  - SQL does the selection + the update; atomic at the DB boundary.
  - No per-row CAS needed: a tablet whose lease expired minutes ago
    is not being renewed; the current node's cached epoch doesn't
    matter.

**Core (`crates/agentdb-core/src/reaper.rs`, new):**

- `StaleTabletReaper` struct holding `meta`, `is_leader_fn`,
  `config`, and an `AtomicU64 reassignments_total`.
- `ReaperConfig { tick: Duration, safety_margin: Duration }`, with
  `from_env` reading `AGENTDB_REAPER_TICK_SECS` (default 10) and
  `AGENTDB_REAPER_SAFETY_MARGIN_SECS` (default 5).
- `StaleTabletReaper::start(meta, election, config) -> Arc<Self>`:
  spawns a tokio task that ticks every `config.tick`. Each tick:
  1. Check `election.is_leader()`. If `false`, sleep and continue.
  2. Call `tablets::clear_expired_ownership`. On success, bump
     `reassignments_total` by returned count; log at INFO with
     the cleared ids.
  3. On error (meta unreachable), log WARN and retry next tick —
     same philosophy as lease renewal.
- `Arc<Self>` clonable into admin handlers; `reassignments_total()`
  accessor.

**Server (`bin/agentdb-server/src/main.rs`):**

- Read `AGENTDB_REAPER_TICK_SECS` + `_SAFETY_MARGIN_SECS`. Build
  `ReaperConfig`.
- `StaleTabletReaper::start(meta.clone(), election.clone(), cfg)`
  — handle lives on the stack for `Arc::clone` into router layer.
- Layer the reaper as an axum Extension so the admin endpoint can
  read its counter.

**Admin (`crates/agentdb-api/src/admin_scheduler.rs`):**

- Extend `SchedulerStatus` with
  `reassignments_total: u64`. Read from
  `Option<Extension<Arc<StaleTabletReaper>>>` — absent in tests
  returns 0 rather than 503 (the reaper is an optional background
  detail; election is the core concept).

## Out of scope

- **Reassignment policy.** Cross-AZ preference, anti-affinity,
  node-weight awareness, pick-new-owner algorithms — all design
  §2.7 stuff. The reaper only *clears* ownership; any new owner
  acquires via the normal `BoxRegistry::new_with_meta` boot path.
- **\`agentdb drain\` CLI.** Separate PR.
- **Rolling-upgrade runbook + CI.** Separate PR.
- **Promotion of "suspect" nodes to "dead" in `nodes` table.** The
  reaper doesn't touch `nodes`. `is_stale` on `/api/admin/nodes`
  already flags stale heartbeats for operators.
- **Metric on followers.** Only the leader's counter advances;
  followers always report 0. That's correct: the follower did
  nothing.

## Acceptance

- Integration test: seed a meta store with an expired-lease tablet,
  spawn a reaper pointed at it, wait one tick, confirm the tablet's
  `owner_node_id` is NULL and `reassignments_total == 1`.
- Integration test: seed a meta store with a **live** lease,
  spawn the reaper, confirm no reassignment happens.
- Integration test: reaper that can't become leader (is_leader_fn
  returns false) performs zero reassignments even when leases are
  expired.
- `cargo test --workspace` green, `cargo clippy ... -D warnings`
  clean, OpenAPI snapshot regenerated.
- Smoke: boot two servers with a 5s lease duration; kill the
  leader-owning-tablets node; wait >10s; confirm
  `/api/admin/tablets` shows owner=null for the dead owner's
  tablets and `/api/admin/scheduler` shows the survivor's
  `reassignments_total` > 0.

## Shape of the commit

Single PR, single commit:
`feat(core): stale-lease tablet reaper (M22 continuation)`.
