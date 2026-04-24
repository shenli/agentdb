CREATE TABLE IF NOT EXISTS users (
    user_id          TEXT PRIMARY KEY,
    tenant_external  TEXT NOT NULL,
    iterations       BIGINT NOT NULL,
    salt_b64         TEXT NOT NULL,
    stored_key_b64   TEXT NOT NULL,
    server_key_b64   TEXT NOT NULL,
    created_at_ms    BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_external);
