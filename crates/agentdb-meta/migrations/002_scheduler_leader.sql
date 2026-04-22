-- M22 — singleton scheduler-leader lease.
--
-- Exactly one process in the cluster runs scheduler work (tablet
-- reassignment, drain orchestration) at a time. We model the
-- "scheduler role" as a single-row lease with the same CAS-on-renew
-- semantics as `tablets` (see M21). SQLite has no advisory-lock
-- equivalent to pg_advisory_lock; the CHECK constraint below makes
-- it impossible to have two rows.

CREATE TABLE IF NOT EXISTS scheduler_leader (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    node_id         TEXT    NOT NULL,
    epoch           INTEGER NOT NULL DEFAULT 0,
    expires_at_ms   INTEGER NOT NULL,
    acquired_at_ms  INTEGER NOT NULL
);
