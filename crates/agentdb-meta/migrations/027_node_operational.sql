-- Metadata self-hosting stage 1: per-tablet observed state leaves SQL.
-- tablet_replicas becomes placement INTENT only (tablet -> node -> role);
-- observed health/progress and all tablet stats now ride one JSON summary
-- row per node, replaced at heartbeat cadence.
CREATE TABLE node_operational (
    node_id TEXT PRIMARY KEY,
    reported_at_ms BIGINT NOT NULL,
    summary TEXT NOT NULL
);
ALTER TABLE tablet_replicas RENAME TO tablet_replicas_stage1_legacy;
CREATE TABLE tablet_replicas (
    tablet_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tablet_id, node_id)
);
INSERT INTO tablet_replicas (tablet_id, node_id, role, state, updated_at_ms)
    SELECT tablet_id, node_id, role,
           CASE WHEN health = 'draining' THEN 'draining' ELSE 'active' END,
           updated_at_ms
      FROM tablet_replicas_stage1_legacy;
DROP TABLE tablet_replicas_stage1_legacy;
DROP TABLE tablet_stats;
