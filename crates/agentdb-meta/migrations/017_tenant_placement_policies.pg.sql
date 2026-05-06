-- M17 — tenant placement and encryption policy metadata.
-- Postgres dialect.

CREATE TABLE IF NOT EXISTS tenant_policies (
    tenant_id                   TEXT PRIMARY KEY,
    placement_class             TEXT NOT NULL,
    placement_scope_id          TEXT NOT NULL,
    encryption_class            TEXT NOT NULL,
    key_policy_id               TEXT,
    encryption_policy_version   BIGINT NOT NULL DEFAULT 0,
    policy_version              BIGINT NOT NULL DEFAULT 0,
    updated_at_ms               BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenant_policies_scope
    ON tenant_policies (placement_scope_id);
