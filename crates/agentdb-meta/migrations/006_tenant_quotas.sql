-- Global per-tenant rate limits.
--
-- Unlike `max_tenant_qps` on a box (which caps ONE tenant's
-- QPS against ONE box), this table caps a tenant's aggregate
-- QPS on this node. Multi-node deployments divide the quota
-- by node count manually today — cluster-global coordination
-- is out of scope for the prototype.
--
-- Key is the external tenant id (string), matching what
-- operators type at the CLI and what appears in
-- `/api/tenants/:tenant_id/...` URLs.
--
-- `max_qps` and `burst` are both required and must be > 0.
-- "No quota" is represented by the absence of a row; clearing
-- a quota deletes the row, which drops the in-memory bucket.

CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id      TEXT PRIMARY KEY,
    max_qps        INTEGER NOT NULL CHECK (max_qps > 0),
    burst          INTEGER NOT NULL CHECK (burst > 0),
    updated_at_ms  INTEGER NOT NULL
);
