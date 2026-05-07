-- M16 — placement scopes, tablet lifecycle, and compressed shard ranges.
-- Postgres dialect.

CREATE TABLE IF NOT EXISTS placement_scopes (
    placement_scope_id TEXT PRIMARY KEY,
    kind               TEXT NOT NULL,
    shard_count        BIGINT NOT NULL,
    hash_seed          TEXT,
    state              TEXT NOT NULL,
    epoch              BIGINT NOT NULL DEFAULT 0,
    created_at_ms      BIGINT NOT NULL
);

INSERT INTO placement_scopes
    (placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms)
VALUES
    ('default', 'shared_pool', 16384, NULL, 'serving', 0, 0)
ON CONFLICT (placement_scope_id) DO NOTHING;

ALTER TABLE tablets ADD COLUMN placement_scope_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE tablets ADD COLUMN state TEXT NOT NULL DEFAULT 'serving';
ALTER TABLE tablets ADD COLUMN raft_group_id TEXT;
ALTER TABLE tablets ADD COLUMN encryption_policy_version BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS tablet_ranges (
    placement_scope_id TEXT NOT NULL,
    start_shard        BIGINT NOT NULL,
    end_shard          BIGINT NOT NULL,
    tablet_id          BIGINT NOT NULL,
    map_epoch          BIGINT NOT NULL DEFAULT 0,
    updated_at_ms      BIGINT NOT NULL,
    PRIMARY KEY (placement_scope_id, start_shard, end_shard)
);

CREATE INDEX IF NOT EXISTS idx_tablet_ranges_lookup
    ON tablet_ranges (placement_scope_id, start_shard, end_shard);

CREATE INDEX IF NOT EXISTS idx_tablet_ranges_tablet
    ON tablet_ranges (tablet_id);

CREATE TABLE IF NOT EXISTS tablet_replicas (
    tablet_id     BIGINT NOT NULL,
    node_id       TEXT NOT NULL,
    role          TEXT NOT NULL,
    health        TEXT NOT NULL,
    applied_index BIGINT NOT NULL DEFAULT 0,
    match_index   BIGINT NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tablet_id, node_id)
);
