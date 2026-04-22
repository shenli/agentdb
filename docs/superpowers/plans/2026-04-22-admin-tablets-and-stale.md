# Plan: admin surface — `/api/admin/tablets` + `is_stale` on nodes

Date: 2026-04-22
Scope: narrow extension of the operator surface. Makes the other half
of the M18 control-plane schema (tablets rows) directly visible from
HTTP, and surfaces staleness on the existing nodes endpoint.

## Why

After PR #27 operators can answer "who is in the cluster" with
`GET /api/admin/nodes`. They still can't answer, without reaching
into SQLite:

- "What tablets exist and who owns them?"
- "Which of those nodes is still heartbeating?"

Both questions get routine during incident response. Adding a tablets
endpoint and a per-row staleness flag closes the gap without building
a scheduler.

## Scope

In: `crates/agentdb-meta/src/tablets.rs`,
`crates/agentdb-api/src/admin_tablets.rs` (new),
`crates/agentdb-api/src/admin_nodes.rs` (extend),
`crates/agentdb-api/src/lib.rs` (register route + utoipa).

1. **`tablets::list_all(pool)`** — returns every row ordered by
   `tablet_id`. Symmetric with `nodes::list_all` added in PR #27.
   Unit test verifies id ordering and field shape.

2. **`GET /api/admin/tablets`** — new admin handler returning
   `Vec<TabletView>`. Fields: `tablet_id`, `owner_node_id` (nullable),
   `epoch`, `lease_expires_at_ms`, `created_at_ms`. Uses the same
   `Option<Extension<Arc<MetaStore>>>` pattern as `/api/admin/nodes`
   so tests without meta get a graceful 503. Two unit tests cover
   the present + absent paths.

3. **`GET /api/admin/nodes?stale_threshold_ms=N`** — optional query
   parameter (default 15000 ms, three times the default heartbeat
   period). Server computes `is_stale = now_ms - last_heartbeat_ms
   > threshold` for each row and returns it as a new field on
   `NodeView`. Default preserves the pre-PR response shape except
   for the extra boolean — tests and callers that ignore it stay
   green.

## Out of scope

- Stale-node → tablet reassignment (M22 proper, depends on scheduler).
- Leader election / advisory lock pattern (M22 proper).
- Tablet state aggregation (warm bytes, QPS, etc) — those live in
  the runtime `Box_`, not meta. Separate admin endpoint later.
- Admin CLI — M27.

## Acceptance

- `cargo test --workspace` green.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- OpenAPI snapshot regenerated and consistent.
- Smoke: boot server, `curl /api/admin/tablets` returns rows for each
  box. Kill + wait past `stale_threshold_ms`, restart, hit
  `/api/admin/nodes?stale_threshold_ms=100` and see the stale row
  flagged. (Manual step, documented in PR body.)

## Shape of the commit

Single PR, single commit:
`feat(admin): GET /api/admin/tablets + is_stale flag on /api/admin/nodes`.
