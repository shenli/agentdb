-- M18 - retained tablet snapshot catalog.
--
-- Snapshot bytes may live in raft-engine, local staging, or object storage.
-- This table is the control-plane catalog for operational visibility,
-- retention, and eventual object-store deletion.

CREATE TABLE IF NOT EXISTS tablet_snapshots (
    snapshot_id          TEXT PRIMARY KEY,
    tablet_id            INTEGER NOT NULL,
    source_node_id       TEXT NOT NULL,
    source_raft_node_id  INTEGER NOT NULL,
    target_node_id       TEXT NOT NULL,
    target_raft_node_id  INTEGER NOT NULL,
    snapshot_index       INTEGER NOT NULL,
    snapshot_term        INTEGER NOT NULL,
    size_bytes           INTEGER NOT NULL,
    sha256_hex           TEXT NOT NULL,
    storage_uri          TEXT,
    status               TEXT NOT NULL,
    created_at_ms        INTEGER NOT NULL,
    updated_at_ms        INTEGER NOT NULL,
    expires_at_ms        INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tablet_snapshots_tablet_target
    ON tablet_snapshots (tablet_id, target_node_id, snapshot_index DESC);

CREATE INDEX IF NOT EXISTS idx_tablet_snapshots_expiry
    ON tablet_snapshots (status, expires_at_ms);
