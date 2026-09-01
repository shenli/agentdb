-- Change-notification triggers must not fire on liveness touches.
--
-- Every node heartbeats `nodes.last_heartbeat_ms` and every tablet owner
-- renews `tablets.lease_expires_at_ms` on a sub-second cadence. The
-- migration-009 triggers notified on EVERY row touch, so a healthy
-- cluster broadcast a steady notification storm and every listener
-- (route-snapshot, raft registry, peer-URL refreshers on every node)
-- re-pulled the full routing set per heartbeat — the exact
-- "control-plane work amplified to hot-loop cadence" defect the S8
-- statement budget exists to catch.
--
-- Redefine the trigger functions (names unchanged; the existing
-- triggers pick up the new bodies) to swallow UPDATEs whose only
-- change is the liveness column. INSERT/DELETE and any real change
-- (address, state, owner, epoch, placement) still notify.

CREATE OR REPLACE FUNCTION agentdb_notify_nodes() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND to_jsonb(NEW) - 'last_heartbeat_ms' = to_jsonb(OLD) - 'last_heartbeat_ms' THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('agentdb_nodes_changed',
                      COALESCE(NEW.node_id, OLD.node_id));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION agentdb_notify_tablets() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND to_jsonb(NEW) - 'lease_expires_at_ms' = to_jsonb(OLD) - 'lease_expires_at_ms' THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('agentdb_tablets_changed',
                      COALESCE(NEW.tablet_id::text, OLD.tablet_id::text));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
