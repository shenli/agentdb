-- M18 - retained tablet snapshot catalog.
-- Postgres dialect.

CREATE TABLE IF NOT EXISTS tablet_snapshots (
    snapshot_id          TEXT PRIMARY KEY,
    tablet_id            BIGINT NOT NULL,
    source_node_id       TEXT NOT NULL,
    source_raft_node_id  BIGINT NOT NULL,
    target_node_id       TEXT NOT NULL,
    target_raft_node_id  BIGINT NOT NULL,
    snapshot_index       BIGINT NOT NULL,
    snapshot_term        BIGINT NOT NULL,
    size_bytes           BIGINT NOT NULL,
    sha256_hex           TEXT NOT NULL,
    storage_uri          TEXT,
    status               TEXT NOT NULL,
    created_at_ms        BIGINT NOT NULL,
    updated_at_ms        BIGINT NOT NULL,
    expires_at_ms        BIGINT
);

CREATE INDEX IF NOT EXISTS idx_tablet_snapshots_tablet_target
    ON tablet_snapshots (tablet_id, target_node_id, snapshot_index DESC);

CREATE INDEX IF NOT EXISTS idx_tablet_snapshots_expiry
    ON tablet_snapshots (status, expires_at_ms);
