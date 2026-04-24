-- Postgres LISTEN / NOTIFY channels for change propagation.
--
-- Three channels: `shards_changed`, `nodes_changed`,
-- `tablets_changed`. A small trigger on each table fires a
-- NOTIFY with the PK of the row that changed so downstream
-- proxies (LocationProxy, NodeAddressProxy) can refresh
-- on-demand instead of polling.
--
-- SQLite runtime falls back to the existing periodic refresh
-- loops; this migration is a no-op there.

CREATE OR REPLACE FUNCTION agentdb_notify_shards() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('agentdb_shards_changed',
                      COALESCE(NEW.shard_id::text, OLD.shard_id::text));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agentdb_shards_notify ON shards;
CREATE TRIGGER trg_agentdb_shards_notify
    AFTER INSERT OR UPDATE OR DELETE ON shards
    FOR EACH ROW EXECUTE FUNCTION agentdb_notify_shards();

CREATE OR REPLACE FUNCTION agentdb_notify_nodes() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('agentdb_nodes_changed',
                      COALESCE(NEW.node_id, OLD.node_id));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agentdb_nodes_notify ON nodes;
CREATE TRIGGER trg_agentdb_nodes_notify
    AFTER INSERT OR UPDATE OR DELETE ON nodes
    FOR EACH ROW EXECUTE FUNCTION agentdb_notify_nodes();

CREATE OR REPLACE FUNCTION agentdb_notify_tablets() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('agentdb_tablets_changed',
                      COALESCE(NEW.tablet_id::text, OLD.tablet_id::text));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agentdb_tablets_notify ON tablets;
CREATE TRIGGER trg_agentdb_tablets_notify
    AFTER INSERT OR UPDATE OR DELETE ON tablets
    FOR EACH ROW EXECUTE FUNCTION agentdb_notify_tablets();
