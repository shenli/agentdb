-- M23 - compaction-debt signal for tablet runtime statistics.
--
-- Counts committed/applied log entries still retained in the tablet-local log.

ALTER TABLE tablet_stats ADD COLUMN compaction_debt_ops INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_tablet_stats_compaction_debt
    ON tablet_stats (compaction_debt_ops DESC);
