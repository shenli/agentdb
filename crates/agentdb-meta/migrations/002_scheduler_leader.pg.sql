-- Postgres dialect of 002_scheduler_leader.sql.
-- Postgres INTEGER is 4-byte so we use BIGINT; the singleton
-- CHECK is identical.

CREATE TABLE IF NOT EXISTS scheduler_leader (
    id              BIGINT PRIMARY KEY CHECK (id = 1),
    node_id         TEXT   NOT NULL,
    epoch           BIGINT NOT NULL DEFAULT 0,
    expires_at_ms   BIGINT NOT NULL,
    acquired_at_ms  BIGINT NOT NULL
);
