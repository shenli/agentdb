-- Admin-plane bearer tokens (post-M25 security patch).
-- One row per mint. Plaintext token is returned ONCE at mint
-- time; disk carries only the SHA-256 hash (base64). Revocation
-- is soft — revoked_at_ms set, row kept for audit.

CREATE TABLE IF NOT EXISTS admin_tokens (
    token_id         TEXT PRIMARY KEY,
    token_hash_b64   TEXT NOT NULL,
    description      TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL,
    revoked_at_ms    INTEGER
);

-- Hash lookup must be fast (hit on every authenticated admin
-- request); the UNIQUE index is also a correctness guard
-- against collisions (astronomically unlikely but free).
CREATE UNIQUE INDEX IF NOT EXISTS idx_admin_tokens_hash
    ON admin_tokens(token_hash_b64);
