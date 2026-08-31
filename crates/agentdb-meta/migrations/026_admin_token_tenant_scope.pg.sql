-- Tenant-scoped bearer tokens: a token with scope = 'tenant' carries the
-- one tenant it may access on the data plane. NULL for admin/readonly
-- scopes. Enforcement lives in the API middleware.
ALTER TABLE admin_tokens ADD COLUMN tenant_id TEXT;
