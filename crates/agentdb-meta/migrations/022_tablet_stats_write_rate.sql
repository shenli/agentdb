-- M22 - write-rate signals for tablet runtime storage statistics.
--
-- Rate is derived from applied-index growth between local heartbeat samples.
-- Stored as milli-ops/sec to keep the metadata dialects integer-only.

ALTER TABLE tablet_stats ADD COLUMN sample_interval_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tablet_stats ADD COLUMN write_ops_delta INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tablet_stats ADD COLUMN write_ops_per_sec_milli INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_tablet_stats_write_rate
    ON tablet_stats (write_ops_per_sec_milli DESC);
