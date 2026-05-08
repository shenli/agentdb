-- Postgres LISTEN / NOTIFY trigger for tablet replica metadata.
--
-- Raft registry/runtime bootstrap subscribes to the tablets channel and
-- refreshes from metadata. Replica rows decide whether a node should open or
-- close a local Raft group, so they must emit the same tablet-scoped hint.

CREATE OR REPLACE FUNCTION agentdb_notify_tablet_replicas() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('agentdb_tablets_changed',
                      COALESCE(NEW.tablet_id::text, OLD.tablet_id::text));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agentdb_tablet_replicas_notify ON tablet_replicas;
CREATE TRIGGER trg_agentdb_tablet_replicas_notify
    AFTER INSERT OR UPDATE OR DELETE ON tablet_replicas
    FOR EACH ROW EXECUTE FUNCTION agentdb_notify_tablet_replicas();
