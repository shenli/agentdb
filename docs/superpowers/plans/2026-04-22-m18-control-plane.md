# M18 Metadata Layer 1 — Postgres Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Introduce the global control-plane storage layer per v2 design §3.3 ("Metadata storage — two layers"): HA Postgres for tenant registry, shard assignment, tablet assignment, node membership, cluster config. This PR **lands the schema + migrations only**; runtime consumers follow in M19+.

## Scope for this PR (narrow)

- New crate `agentdb-meta` so Postgres (heavy) deps don't pollute every other crate's build.
- Dialect support: SQLite (primary for dev mode + CI); Postgres wired as a separate code path for prod. Both share the same migration SQL where possible, with dialect-specific variants where needed.
- Schema:
  - `tenants` — `(internal_id PRIMARY KEY, external_id UNIQUE, created_at)` ; tenant registry.
  - `shards` — 16384 rows keyed by shard id, mapping shard → tablet_id (nullable until tablet is created; NULL = default-by-hash).
  - `tablets` — `(tablet_id, owner_node_id, epoch, lease_expires_at, created_at)`; single row per tablet.
  - `nodes` — `(node_id, address, last_heartbeat_at, state)`.
  - `config` — `(key, value, updated_at)` — `(text, text)` KV; used for `S` (shard count), hash seed, `max_offset_secs`, etc.
- Embedded migrations via `refinery` or a hand-rolled apply-on-first-open path, idempotent.
- Basic CRUD helpers for each table (enough for M19 to consume).
- Unit tests: migrations apply on empty DB; repeated opens are no-ops; CRUD round-trips.

## Out of scope for this PR

- Runtime wiring — `Box_` and friends don't touch the control plane yet. Read/write code paths unchanged.
- Postgres-specific features (`LISTEN/NOTIFY`, advisory locks). Plumbed as stubs; landed in M19 when there's a consumer.
- Automatic shard-assignment provisioning. v2-plan-level; not this PR.

## Dialect choice

`sqlx` with `sqlite` + `postgres` features gives unified queries. Downside: heavier build. For M18, enable both features but gate Postgres code paths behind a feature flag or runtime-selected pool so CI can run SQLite-only.

## Steps

- [ ] **A1** Add `agentdb-meta` crate scaffolding (Cargo.toml, empty lib.rs).
- [ ] **A2** Add `sqlx` (with sqlite + runtime-tokio-rustls), `refinery` (or hand-rolled migrations).
- [ ] **B1** Write SQL migrations in `crates/agentdb-meta/migrations/`.
- [ ] **B2** Test that migrations apply cleanly on fresh SQLite.
- [ ] **B3** Test that repeated opens are no-ops.
- [ ] **C1** CRUD helpers: `tenants::get_by_external(pool, ext) -> Option<Tenant>`, `tenants::create(pool, ext) -> Tenant`, similar for others.
- [ ] **C2** Round-trip tests for each table.
- [ ] **D1** `config::get` / `config::set` key-value helpers.
- [ ] **E1** Document cross-dialect caveats in the crate README or docs.

## Acceptance

- `cargo test -p agentdb-meta` green.
- Fresh SQLite file → apply migrations → all tables exist → round-trip CRUD for each.
- No changes to existing crates; zero regression risk on runtime.

## Risks

- **Build time cost of `sqlx`.** Mitigation: isolate in `agentdb-meta` so it's not a transitive dep elsewhere until M19 pulls it in.
- **Migration format portability.** Mitigation: SQLite only for now; Postgres-specific features land in M19.
