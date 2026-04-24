CREATE TABLE IF NOT EXISTS admin_audit (
    id               BIGSERIAL PRIMARY KEY,
    ts_ms            BIGINT NOT NULL,
    actor_token_id   TEXT,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    query            TEXT,
    status           INTEGER NOT NULL,
    client_ip        TEXT
);

CREATE INDEX IF NOT EXISTS idx_admin_audit_ts_desc
    ON admin_audit(ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_admin_audit_actor
    ON admin_audit(actor_token_id);
