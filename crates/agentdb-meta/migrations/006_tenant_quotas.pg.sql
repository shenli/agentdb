CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id      TEXT PRIMARY KEY,
    max_qps        BIGINT NOT NULL CHECK (max_qps > 0),
    burst          BIGINT NOT NULL CHECK (burst > 0),
    updated_at_ms  BIGINT NOT NULL
);
