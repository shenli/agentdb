-- M19 — tablet state movement work queue for split/merge.

CREATE TABLE IF NOT EXISTS tablet_movements (
    movement_id            TEXT PRIMARY KEY,
    movement_type          TEXT NOT NULL CHECK (movement_type IN ('split', 'merge')),
    source_tablet_id       BIGINT NOT NULL,
    destination_tablet_id  BIGINT NOT NULL,
    placement_scope_id     TEXT NOT NULL,
    start_shard            BIGINT NOT NULL,
    end_shard              BIGINT NOT NULL,
    required_applied_index BIGINT NOT NULL CHECK (required_applied_index >= 0),
    status                 TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'cancelled')),
    created_at_ms          BIGINT NOT NULL,
    updated_at_ms          BIGINT NOT NULL,
    completed_at_ms        BIGINT
);

CREATE INDEX IF NOT EXISTS idx_tablet_movements_status
    ON tablet_movements(status, updated_at_ms);

CREATE INDEX IF NOT EXISTS idx_tablet_movements_destination
    ON tablet_movements(destination_tablet_id, status);
