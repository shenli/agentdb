-- M18 — Metadata Layer 1 initial schema (Postgres dialect).
--
-- SQLite dialect lives in 001_initial.sql. This file is
-- byte-for-byte equivalent in intent; the differences are
-- AUTOINCREMENT → BIGSERIAL, INTEGER → BIGINT (Postgres
-- INTEGER is 32-bit and can't hold ms-epoch timestamps).

CREATE TABLE IF NOT EXISTS tenants (
    internal_id     BIGSERIAL PRIMARY KEY,
    external_id     TEXT NOT NULL UNIQUE,
    created_at_ms   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenants_external_id ON tenants(external_id);

CREATE TABLE IF NOT EXISTS tablets (
    tablet_id            BIGINT PRIMARY KEY,
    owner_node_id        TEXT,
    epoch                BIGINT NOT NULL DEFAULT 0,
    lease_expires_at_ms  BIGINT NOT NULL DEFAULT 0,
    created_at_ms        BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS shards (
    shard_id      BIGINT PRIMARY KEY,
    tablet_id     BIGINT,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS session_overrides (
    session_id      TEXT PRIMARY KEY,
    tablet_id       BIGINT NOT NULL,
    created_at_ms   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id            TEXT PRIMARY KEY,
    address            TEXT NOT NULL,
    state              TEXT NOT NULL,
    last_heartbeat_ms  BIGINT NOT NULL,
    joined_at_ms       BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_heartbeat ON nodes(last_heartbeat_ms);

CREATE TABLE IF NOT EXISTS config (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at_ms   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version       BIGINT PRIMARY KEY,
    applied_at_ms BIGINT NOT NULL,
    description   TEXT NOT NULL
);
