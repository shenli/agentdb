-- M26 Phase 1: canonical session locator (Postgres dialect).
--
-- Decouples session identity from tenant-hash routing: every
-- session has exactly one row keyed by its globally-unique
-- external id, recording the tenant that owns it, the tablet
-- that currently hosts it, and the shard slot it lives in.
--
-- This row is *populated* in this PR but not yet *consulted*
-- on the read/write path — wiring follows in subsequent
-- M26 PRs (per-session lock, fanout reads, archive-complete
-- index). See docs/ARCHITECTURE.md.
CREATE TABLE session_locators (
    session_id            TEXT    PRIMARY KEY,
    tenant_internal_id    BIGINT  NOT NULL,
    -- nullable: a session may be created before the tablet
    -- it lives on is recorded (rare, but possible during
    -- migration). Routing falls back to tenant-hash when null.
    tablet_id             BIGINT,
    shard_id              INTEGER NOT NULL,
    created_at_ms         BIGINT  NOT NULL
);
CREATE INDEX idx_session_locators_tenant ON session_locators (tenant_internal_id);
CREATE INDEX idx_session_locators_tablet ON session_locators (tablet_id);
