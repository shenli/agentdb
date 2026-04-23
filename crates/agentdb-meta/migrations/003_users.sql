-- M19: SCRAM-SHA-256 credentials. Stored format follows RFC 5802:
-- (iterations, salt, StoredKey, ServerKey). Password never touches
-- disk; plaintext is only in memory during user creation and
-- verification. Secrets are base64-encoded TEXT for inspectability
-- via `sqlite3 meta.sqlite` — a real deployment would prefer BLOB.

CREATE TABLE IF NOT EXISTS users (
    user_id          TEXT PRIMARY KEY,
    tenant_external  TEXT NOT NULL,
    iterations       INTEGER NOT NULL,
    salt_b64         TEXT NOT NULL,
    stored_key_b64   TEXT NOT NULL,
    server_key_b64   TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_external);
