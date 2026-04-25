-- M26 Phase 1b correction: store the external tenant id on
-- the locator directly (Postgres dialect).
ALTER TABLE session_locators ADD COLUMN tenant_external_id TEXT;
