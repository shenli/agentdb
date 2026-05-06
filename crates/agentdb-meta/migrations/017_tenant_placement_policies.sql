-- M17 — tenant placement and encryption policy metadata.
--
-- Runtime policy remains in agentdb-core today; this table is the control-plane
-- source of truth for storage placement, shard-map scope, and key policy refs.

CREATE TABLE IF NOT EXISTS tenant_policies (
    tenant_id                   TEXT PRIMARY KEY,
    placement_class             TEXT NOT NULL,
    placement_scope_id          TEXT NOT NULL,
    encryption_class            TEXT NOT NULL,
    key_policy_id               TEXT,
    encryption_policy_version   INTEGER NOT NULL DEFAULT 0,
    policy_version              INTEGER NOT NULL DEFAULT 0,
    updated_at_ms               INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenant_policies_scope
    ON tenant_policies (placement_scope_id);
