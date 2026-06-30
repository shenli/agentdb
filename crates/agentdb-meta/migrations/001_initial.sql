-- M18 — Metadata Layer 1 (control plane) initial schema.
--
-- Cross-dialect SQL targeting both SQLite and Postgres. Kept
-- intentionally narrow — just what M19+ need to consume. See
-- docs/architecture/ARCHITECTURE.md for the design rationale.

CREATE TABLE IF NOT EXISTS tenants (
    internal_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id     TEXT NOT NULL UNIQUE,
    created_at_ms   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenants_external_id ON tenants(external_id);

-- Tablets — runtime units that own shard ranges. `owner_node_id` is
-- NULL until the scheduler assigns one. `epoch` monotonically
-- increases on every ownership change so fencing-token reads can
-- reject stale-epoch writes (M21). `lease_expires_at_ms` is the
-- lease expiry; 0 = no live lease.
CREATE TABLE IF NOT EXISTS tablets (
    tablet_id            INTEGER PRIMARY KEY,
    owner_node_id        TEXT,
    epoch                INTEGER NOT NULL DEFAULT 0,
    lease_expires_at_ms  INTEGER NOT NULL DEFAULT 0,
    created_at_ms        INTEGER NOT NULL
);

-- Shards — 16384 rows keyed by shard id, mapping shard → tablet.
-- NULL `tablet_id` means "default routing by hash" (the common case
-- until an override or tablet split is applied). Rows are seeded at
-- cluster init.
CREATE TABLE IF NOT EXISTS shards (
    shard_id     INTEGER PRIMARY KEY,
    tablet_id    INTEGER,
    updated_at_ms INTEGER NOT NULL
);

-- Session-level routing overrides. Empty in the common case; entries
-- added only when a session is explicitly migrated away from its
-- hash-computed tablet (hot-session escape hatch per design §2.5).
CREATE TABLE IF NOT EXISTS session_overrides (
    session_id      TEXT PRIMARY KEY,
    tablet_id       INTEGER NOT NULL,
    created_at_ms   INTEGER NOT NULL
);

-- Cluster nodes — membership + heartbeat. `state` is a free-form
-- string ("alive", "draining", "dead") for operator visibility.
CREATE TABLE IF NOT EXISTS nodes (
    node_id            TEXT PRIMARY KEY,
    address            TEXT NOT NULL,
    state              TEXT NOT NULL,
    last_heartbeat_ms  INTEGER NOT NULL,
    joined_at_ms       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_heartbeat ON nodes(last_heartbeat_ms);

-- Small K-V for cluster-wide config: shard count `S`, hash seed,
-- max-offset seconds, etc.
CREATE TABLE IF NOT EXISTS config (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);

-- Schema-migration tracking table. Lets the runtime verify which
-- migrations have been applied. One row per migration.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version       INTEGER PRIMARY KEY,
    applied_at_ms INTEGER NOT NULL,
    description   TEXT NOT NULL
);
