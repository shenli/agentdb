-- Consolidated initial control-plane schema (single migration).
-- Derived by applying the historical 001-031 migrations and dumping the
-- resulting schema: there are no deployed clusters to migrate, so the
-- incremental history is collapsed into this one file. The runner still
-- creates and tracks schema_migrations itself, so it is omitted here.

CREATE TABLE cells (
    cell_id                    TEXT PRIMARY KEY,
    region                     TEXT,
    domain_class               TEXT NOT NULL DEFAULT 'standard',
    deployment_profile         TEXT NOT NULL DEFAULT 'single_az_cluster',
    capacity_class             TEXT NOT NULL DEFAULT 'stable',
    metadata_isolation_level   TEXT NOT NULL DEFAULT 'shared_logical',
    state                      TEXT NOT NULL DEFAULT 'serving',
    metadata_namespace         TEXT,
    object_store_namespace     TEXT,
    rollout_version            INTEGER NOT NULL DEFAULT 0,
    created_at_ms              INTEGER NOT NULL,
    updated_at_ms              INTEGER NOT NULL
);
CREATE TABLE cluster_root (
    id                   INTEGER PRIMARY KEY,     -- always 1
    cluster_id           TEXT NOT NULL,
    bootstrap_generation BIGINT NOT NULL,
    created_at_ms        BIGINT NOT NULL,
    updated_at_ms        BIGINT NOT NULL
);
CREATE TABLE config (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);
CREATE TABLE node_operational (
    node_id TEXT PRIMARY KEY,
    reported_at_ms BIGINT NOT NULL,
    summary TEXT NOT NULL
);
CREATE TABLE nodes (
    node_id            TEXT PRIMARY KEY,
    address            TEXT NOT NULL,
    state              TEXT NOT NULL,
    last_heartbeat_ms  INTEGER NOT NULL,
    joined_at_ms       INTEGER NOT NULL
, mongo_address TEXT, availability_zone TEXT, region TEXT, cell_id TEXT NOT NULL DEFAULT 'default', capacity_class TEXT NOT NULL DEFAULT 'stable');
CREATE TABLE placement_scopes (
    placement_scope_id TEXT PRIMARY KEY,
    kind               TEXT NOT NULL,
    shard_count        INTEGER NOT NULL,
    hash_seed          TEXT,
    state              TEXT NOT NULL,
    epoch              INTEGER NOT NULL DEFAULT 0,
    created_at_ms      INTEGER NOT NULL
, cell_id TEXT NOT NULL DEFAULT 'default');
CREATE TABLE scheduler_leader (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    node_id         TEXT    NOT NULL,
    epoch           INTEGER NOT NULL DEFAULT 0,
    expires_at_ms   INTEGER NOT NULL,
    acquired_at_ms  INTEGER NOT NULL
);
CREATE TABLE shards (
    shard_id     INTEGER PRIMARY KEY,
    tablet_id    INTEGER,
    updated_at_ms INTEGER NOT NULL
);
CREATE TABLE store_incarnations (
    node_id           TEXT NOT NULL,
    store_incarnation TEXT NOT NULL,
    state             TEXT NOT NULL,           -- 'active' | 'retired'
    admitted_at_ms    BIGINT NOT NULL,
    retired_at_ms     BIGINT,
    PRIMARY KEY (node_id, store_incarnation)
);
CREATE TABLE tablet_movements (
    movement_id            TEXT PRIMARY KEY,
    movement_type          TEXT NOT NULL CHECK (movement_type IN ('split', 'merge', 'manual_split', 'manual_merge')),
    source_tablet_id       INTEGER NOT NULL,
    destination_tablet_id  INTEGER NOT NULL,
    placement_scope_id     TEXT NOT NULL,
    start_shard            INTEGER NOT NULL,
    end_shard              INTEGER NOT NULL,
    required_applied_index INTEGER NOT NULL CHECK (required_applied_index >= 0),
    status                 TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'cancelled')),
    created_at_ms          INTEGER NOT NULL,
    updated_at_ms          INTEGER NOT NULL,
    completed_at_ms        INTEGER
);
CREATE TABLE tablet_ranges (
    placement_scope_id TEXT NOT NULL,
    start_shard        INTEGER NOT NULL,
    end_shard          INTEGER NOT NULL,
    tablet_id          INTEGER NOT NULL,
    map_epoch          INTEGER NOT NULL DEFAULT 0,
    updated_at_ms      INTEGER NOT NULL,
    PRIMARY KEY (placement_scope_id, start_shard, end_shard)
);
CREATE TABLE tablet_replicas (
    tablet_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tablet_id, node_id)
);
CREATE TABLE tablet_snapshots (
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
CREATE TABLE tablets (
    tablet_id            INTEGER PRIMARY KEY,
    owner_node_id        TEXT,
    epoch                INTEGER NOT NULL DEFAULT 0,
    lease_expires_at_ms  INTEGER NOT NULL DEFAULT 0,
    created_at_ms        INTEGER NOT NULL
, handoff_target_node_id TEXT, placement_scope_id TEXT NOT NULL DEFAULT 'default', state TEXT NOT NULL DEFAULT 'serving', raft_group_id TEXT, encryption_policy_version INTEGER NOT NULL DEFAULT 0);
CREATE TABLE tenant_policies (
    tenant_id                   TEXT PRIMARY KEY,
    placement_class             TEXT NOT NULL,
    placement_scope_id          TEXT NOT NULL,
    encryption_class            TEXT NOT NULL,
    key_policy_id               TEXT,
    encryption_policy_version   INTEGER NOT NULL DEFAULT 0,
    policy_version              INTEGER NOT NULL DEFAULT 0,
    updated_at_ms               INTEGER NOT NULL
, cell_id TEXT NOT NULL DEFAULT 'default', tier TEXT NOT NULL DEFAULT 'free', priority INTEGER NOT NULL DEFAULT 0);

CREATE INDEX idx_nodes_cell
    ON nodes (cell_id);
CREATE INDEX idx_nodes_last_heartbeat ON nodes(last_heartbeat_ms);
CREATE INDEX idx_placement_scopes_cell
    ON placement_scopes (cell_id);
CREATE INDEX idx_tablet_movements_destination
    ON tablet_movements(destination_tablet_id, status);
CREATE INDEX idx_tablet_movements_status
    ON tablet_movements(status, updated_at_ms);
CREATE INDEX idx_tablet_ranges_lookup
    ON tablet_ranges (placement_scope_id, start_shard, end_shard);
CREATE INDEX idx_tablet_ranges_tablet
    ON tablet_ranges (tablet_id);
CREATE INDEX idx_tablet_snapshots_expiry
    ON tablet_snapshots (status, expires_at_ms);
CREATE INDEX idx_tablet_snapshots_tablet_target
    ON tablet_snapshots (tablet_id, target_node_id, snapshot_index DESC);
CREATE INDEX idx_tenant_policies_cell_scope
    ON tenant_policies (cell_id, placement_scope_id);
CREATE INDEX idx_tenant_policies_scope
    ON tenant_policies (placement_scope_id);
-- triggers in sqlite schema: 0 (pg NOTIFY has no sqlite equivalent)

-- Seed rows the historical migrations inserted on first boot: the default
-- placement scope and the default cell. (The 027 tablet_replicas backfill was
-- a one-time data migration off a legacy staging table and has no fresh-DB
-- equivalent, so it is intentionally omitted.)
INSERT OR IGNORE INTO placement_scopes
    (placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms)
VALUES
    ('default', 'shared_pool', 16384, NULL, 'serving', 0, 0);

INSERT OR IGNORE INTO cells
    (cell_id, region, domain_class, deployment_profile, capacity_class,
     metadata_isolation_level, state, metadata_namespace, object_store_namespace,
     rollout_version, created_at_ms, updated_at_ms)
VALUES
    ('default', NULL, 'standard', 'single_az_cluster', 'stable',
     'shared_logical', 'serving', 'default', 'default', 0, 0, 0);

-- Wrapped-DEK key catalog (encryption-at-rest, cluster-shared). One row per
-- (tenant_id, purpose, version) holding a DEK wrapped under the KEK; never
-- plaintext key material. Shared across nodes so every node serves the same
-- per-tenant key; INSERT ... ON CONFLICT DO NOTHING coordinates the
-- provisioning race across nodes.
CREATE TABLE IF NOT EXISTS dek_catalog (
    tenant_id        TEXT NOT NULL,
    purpose          TEXT NOT NULL,           -- metadata|content|index|export|snapshot
    version          INTEGER NOT NULL,
    wrapping_key_ref TEXT NOT NULL,
    nonce            BLOB NOT NULL,
    ciphertext       BLOB NOT NULL,
    status           TEXT NOT NULL,           -- active|read_only|retired|revoked
    created_at_ms    BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, purpose, version)
);
