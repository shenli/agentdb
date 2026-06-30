-- Admin-plane audit log. One row per request that reaches the
-- admin-auth middleware (on /api/admin/*), including 401s so
-- break-glass investigations can see probing attempts.
--
-- `actor_token_id` is NULL when:
--   - admin auth is disabled entirely (dev mode), or
--   - the bootstrap mint path (first POST /api/admin/tokens)
--     succeeded without auth, or
--   - a request failed auth (no actor was established).
--
-- `query` is the URL query string sans leading '?', NULL if
-- absent. `client_ip` is pulled from X-Forwarded-For when a
-- reverse proxy fronts the node; NULL otherwise (our axum
-- server doesn't expose the socket peer in the middleware path
-- today — see docs/architecture/architecture.md § admin audit).
--
-- Retention: unmanaged — operators prune by vacuuming the
-- meta store. At ~100B/row, 10M rows ≈ 1 GiB. Future work:
-- a time- or count-based retention knob.

CREATE TABLE IF NOT EXISTS admin_audit (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms            INTEGER NOT NULL,
    actor_token_id   TEXT,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    query            TEXT,
    status           INTEGER NOT NULL,
    client_ip        TEXT
);

-- Listing is always "newest first within an optional time
-- window"; the descending ts index makes that cheap.
CREATE INDEX IF NOT EXISTS idx_admin_audit_ts_desc
    ON admin_audit(ts_ms DESC);

-- Forensic lookups ("every request token X made") should not
-- have to scan the whole log.
CREATE INDEX IF NOT EXISTS idx_admin_audit_actor
    ON admin_audit(actor_token_id);
