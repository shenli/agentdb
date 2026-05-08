-- M21 - per-node tablet runtime storage statistics.
--
-- Local nodes publish best-effort tablet applied-state size signals here.
-- The scheduler reads these rows for policy-driven split/merge decisions;
-- stale or missing rows must never block correctness.

CREATE TABLE IF NOT EXISTS tablet_stats (
    tablet_id       INTEGER NOT NULL,
    node_id         TEXT NOT NULL,
    key_count       INTEGER NOT NULL,
    key_bytes       INTEGER NOT NULL,
    value_bytes     INTEGER NOT NULL,
    storage_bytes   INTEGER NOT NULL,
    applied_index   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    PRIMARY KEY (tablet_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_tablet_stats_updated
    ON tablet_stats (updated_at_ms);

CREATE INDEX IF NOT EXISTS idx_tablet_stats_tablet
    ON tablet_stats (tablet_id, storage_bytes DESC);
