-- Admin-token scoping for RBAC.
--
-- Two scopes for this PR:
--   'admin'    — full access to every /api/admin/* endpoint
--   'readonly' — GETs only; POST/PUT/DELETE return 403
--
-- Default is 'admin' so every existing token keeps working —
-- no downtime, no re-mint required on upgrade. Operators opt
-- into readonly by minting with an explicit scope.
--
-- Future scopes (e.g. 'tablets_only', 'audit_only') can layer
-- on by widening the allowed value set; the column is TEXT
-- rather than a CHECK-constrained enum so we can roll forward
-- without another migration.

ALTER TABLE admin_tokens
    ADD COLUMN scope TEXT NOT NULL DEFAULT 'admin';
