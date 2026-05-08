-- M23 - compaction-debt signal for tablet runtime statistics.
-- Postgres dialect.

ALTER TABLE tablet_stats ADD COLUMN compaction_debt_ops BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_tablet_stats_compaction_debt
    ON tablet_stats (compaction_debt_ops DESC);
