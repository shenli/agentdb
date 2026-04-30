-- Wake metadata-backed route snapshot refreshes immediately when a session
-- locator is inserted, updated, or deleted.
CREATE OR REPLACE FUNCTION agentdb_notify_session_locators() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('agentdb_session_locators_changed',
                      COALESCE(NEW.session_id, OLD.session_id));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agentdb_session_locators_notify ON session_locators;
CREATE TRIGGER trg_agentdb_session_locators_notify
    AFTER INSERT OR UPDATE OR DELETE ON session_locators
    FOR EACH ROW EXECUTE FUNCTION agentdb_notify_session_locators();
