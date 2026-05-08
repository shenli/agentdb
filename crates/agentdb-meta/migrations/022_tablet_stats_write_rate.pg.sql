-- M22 - write-rate signals for tablet runtime storage statistics.
-- Postgres dialect.

ALTER TABLE tablet_stats ADD COLUMN sample_interval_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tablet_stats ADD COLUMN write_ops_delta BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tablet_stats ADD COLUMN write_ops_per_sec_milli BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_tablet_stats_write_rate
    ON tablet_stats (write_ops_per_sec_milli DESC);
