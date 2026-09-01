-- Metadata self-hosting stages 2-3: tenant-scale metadata (bearer tokens,
-- session locators, the cluster tenant registry) now lives as records
-- under the reserved system tenant inside AgentDB. The SQL tables are
-- dropped (pre-1.0: no data migration), along with tables that never had
-- consumers: session_overrides, tenant_quotas, admin_audit, users.
DROP TABLE IF EXISTS admin_tokens;
DROP TABLE IF EXISTS session_locators;
DROP TABLE IF EXISTS tenants;
DROP TABLE IF EXISTS session_overrides;
DROP TABLE IF EXISTS tenant_quotas;
DROP TABLE IF EXISTS admin_audit;
DROP TABLE IF EXISTS users;
