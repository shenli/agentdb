-- M24 — logical isolation-domain metadata and placement policy fields.
-- Postgres dialect.

CREATE TABLE IF NOT EXISTS cells (
    cell_id                    TEXT PRIMARY KEY,
    region                     TEXT,
    domain_class               TEXT NOT NULL DEFAULT 'standard',
    deployment_profile         TEXT NOT NULL DEFAULT 'single_az_cluster',
    capacity_class             TEXT NOT NULL DEFAULT 'stable',
    metadata_isolation_level   TEXT NOT NULL DEFAULT 'shared_logical',
    state                      TEXT NOT NULL DEFAULT 'serving',
    metadata_namespace         TEXT,
    object_store_namespace     TEXT,
    rollout_version            BIGINT NOT NULL DEFAULT 0,
    created_at_ms              BIGINT NOT NULL,
    updated_at_ms              BIGINT NOT NULL
);

INSERT INTO cells
    (cell_id, region, domain_class, deployment_profile, capacity_class,
     metadata_isolation_level, state, metadata_namespace, object_store_namespace,
     rollout_version, created_at_ms, updated_at_ms)
VALUES
    ('default', NULL, 'standard', 'single_az_cluster', 'stable',
     'shared_logical', 'serving', 'default', 'default', 0, 0, 0)
ON CONFLICT (cell_id) DO NOTHING;

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS cell_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS capacity_class TEXT NOT NULL DEFAULT 'stable';

ALTER TABLE placement_scopes ADD COLUMN IF NOT EXISTS cell_id TEXT NOT NULL DEFAULT 'default';

ALTER TABLE tenant_policies ADD COLUMN IF NOT EXISTS cell_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE tenant_policies ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'free';
ALTER TABLE tenant_policies ADD COLUMN IF NOT EXISTS priority BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_nodes_cell
    ON nodes (cell_id);

CREATE INDEX IF NOT EXISTS idx_placement_scopes_cell
    ON placement_scopes (cell_id);

CREATE INDEX IF NOT EXISTS idx_tenant_policies_cell_scope
    ON tenant_policies (cell_id, placement_scope_id);
