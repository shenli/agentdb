CREATE TABLE IF NOT EXISTS admin_tokens (
    token_id         TEXT PRIMARY KEY,
    token_hash_b64   TEXT NOT NULL,
    description      TEXT NOT NULL,
    created_at_ms    BIGINT NOT NULL,
    revoked_at_ms    BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_admin_tokens_hash
    ON admin_tokens(token_hash_b64);
