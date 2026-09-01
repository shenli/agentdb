-- Metadata self-hosting stages 2-3 (postgres variant): drop the locator
-- NOTIFY trigger with its table, then same as sqlite.
DROP TRIGGER IF EXISTS trg_agentdb_session_locators_notify ON session_locators;
DROP FUNCTION IF EXISTS agentdb_notify_session_locators();
DROP TABLE IF EXISTS admin_tokens;
DROP TABLE IF EXISTS session_locators;
DROP TABLE IF EXISTS tenants;
DROP TABLE IF EXISTS session_overrides;
DROP TABLE IF EXISTS tenant_quotas;
DROP TABLE IF EXISTS admin_audit;
DROP TABLE IF EXISTS users;
