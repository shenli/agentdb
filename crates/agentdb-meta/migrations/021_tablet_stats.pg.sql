-- M21 - per-node tablet runtime storage statistics.
-- Postgres dialect.

CREATE TABLE IF NOT EXISTS tablet_stats (
    tablet_id       BIGINT NOT NULL,
    node_id         TEXT NOT NULL,
    key_count       BIGINT NOT NULL,
    key_bytes       BIGINT NOT NULL,
    value_bytes     BIGINT NOT NULL,
    storage_bytes   BIGINT NOT NULL,
    applied_index   BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL,
    PRIMARY KEY (tablet_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_tablet_stats_updated
    ON tablet_stats (updated_at_ms);

CREATE INDEX IF NOT EXISTS idx_tablet_stats_tablet
    ON tablet_stats (tablet_id, storage_bytes DESC);
