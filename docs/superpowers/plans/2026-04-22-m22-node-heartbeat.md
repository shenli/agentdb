# Plan: M22 start — node registration + heartbeat

Date: 2026-04-22
Scope: *narrow start* of M22. Wires the `nodes` table that M18
created into the running server. Full scheduler-with-failover from the
M22 scope is explicitly deferred to follow-up PRs (leader election,
reassignment orchestration, tablet migration, rolling-upgrade CI).

## Why

After PR #26 the server registers itself as a lease holder (the
`tablets` table), but it does **not** register itself as a node (the
`nodes` table). That means:

- The scheduler that M22 will add next has no membership view to scan.
- There's no operator-visible list of which processes are in the
  cluster.
- The SQLite-mode dev story is incomplete — you can see leases but
  not nodes.

This PR wires the second half of the M18 membership story the same
way PR #26 wired the lease half.

## Scope

In: `bin/agentdb-server/src/main.rs`, `crates/agentdb-api/src/admin.rs`,
`crates/agentdb-api/src/lib.rs`.

1. **Register on boot.** Call `agentdb_meta::nodes::upsert(pool,
   node_id, address, "alive", now_ms)` immediately after the lease
   acquire block. `address` is a new env `AGENTDB_NODE_ADDRESS`,
   default to the value of `AGENTDB_ADDR` (already the REST http
   address). If a node already exists with this id, upsert refreshes
   it to `"alive"` and bumps the heartbeat.

2. **Periodic heartbeat.** Spawn a tokio task that calls
   `agentdb_meta::nodes::heartbeat(pool, node_id, now_ms)` every
   `AGENTDB_HEARTBEAT_SECS` (default 5). Forget the handle, same
   pattern as the renewal and lifecycle jobs. Heartbeat failures are
   logged but do not abort the loop (a transient meta hiccup
   shouldn't kill the server — matches the lease-renewal philosophy).

3. **Admin endpoint `GET /api/admin/nodes`.** Returns a JSON array of
   every row in `nodes` (not paginated — this is the control plane,
   it's small). Each entry carries `node_id`, `address`, `state`,
   `last_heartbeat_ms`, `joined_at_ms`. Used by operators to answer
   "who is in the cluster right now" without reaching into SQLite.

4. **AppState carries `meta: Arc<MetaStore>`.** The admin handler
   needs it, and existing callers thread `AppState` around already.
   Integration tests that only use the registry don't need the meta
   — so we either `Option<Arc<MetaStore>>` it or make tests provide
   an in-memory one. Decision: use
   `Option<Arc<MetaStore>>` so test setups don't churn. The nodes
   endpoint returns 503 when meta is absent rather than panicking;
   that's the same shape as the lease-not-live readiness check.

## Out of scope

- Leader election (`pg_advisory_lock`-ish pattern) — M22 proper.
- Stale-node detection → tablet reassignment — M22 proper.
- Drain CLI, rolling-upgrade runbook — M22 proper.
- Pulling node state into `/health/ready` — incremental addition on
  top of the heartbeat loop; separate PR.

## Acceptance

- Fresh boot → `nodes` table has one row for the running process with
  `state = "alive"` and a recent `last_heartbeat_ms`.
- After waiting `AGENTDB_HEARTBEAT_SECS * 2`, the row's
  `last_heartbeat_ms` has advanced.
- `curl /api/admin/nodes` returns a list containing the current
  process. JSON fields match the ones in the sqlx row.
- Two servers started with distinct `AGENTDB_NODE_ID` values (pointed
  at the same meta.sqlite) both appear in the list.
- `cargo test --workspace` green; `cargo clippy --workspace
  --all-targets -- -D warnings` clean.

## Test plan

- Unit: `admin_nodes::tests::list_nodes_returns_registered_rows` using
  a test AppState with an in-memory MetaStore and two upserted rows.
- Unit: `admin_nodes::tests::list_nodes_returns_503_when_meta_absent`
  to confirm the graceful-absent path.
- Smoke: boot server, `curl /api/admin/nodes`, confirm heartbeat
  advances, kill + restart, confirm node row persists across restart.

## Shape of the commit

Single PR, single commit, title
`feat(server): node registration + heartbeat + GET /api/admin/nodes (M22 start)`.
