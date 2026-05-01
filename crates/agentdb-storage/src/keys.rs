//! Tenant-first logical key encoders for runtime objects.
//!
//! See `docs/ARCHITECTURE.md` for the runtime/storage layout this module
//! supports. Every key starts with the namespace, then the tenant_id, then the
//! object identity. This guarantees that a tenant-scoped prefix scan returns
//! only that tenant's data and that range delete is a single prefix operation.

pub fn tenant_record_key(tenant_id: &str) -> String {
    format!("record/tenant/{tenant_id}")
}

pub fn tenant_alias_key(external_key: &str) -> String {
    format!("record/tenant_alias/{external_key}")
}

pub fn agent_record_key(tenant_id: &str, agent_id: &str) -> String {
    format!("record/agent/{tenant_id}/{agent_id}")
}

pub fn agent_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("record/agent_alias/{tenant_id}/{alias}")
}

pub fn session_record_key(tenant_id: &str, session_id: &str) -> String {
    format!("record/session/{tenant_id}/{session_id}")
}

pub fn session_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("record/session_alias/{tenant_id}/{alias}")
}

pub fn session_by_agent_key(tenant_id: &str, agent_id: &str, session_id: &str) -> String {
    format!("record/session_by_agent/{tenant_id}/{agent_id}/{session_id}")
}

pub fn event_key(tenant_id: &str, session_id: &str, seq: u64) -> String {
    // Zero-pad seq to 20 digits so lexicographic order matches numeric order.
    format!("event/{tenant_id}/{session_id}/{seq:020}")
}

pub fn event_by_id_key(tenant_id: &str, event_id: &str) -> String {
    format!("event_by_id/{tenant_id}/{event_id}")
}

pub fn idempotency_key(tenant_id: &str, operation: &str, target: &str, key: &str) -> String {
    format!("idempotency/{tenant_id}/{operation}/{target}/{key}")
}

pub fn artifact_record_key(tenant_id: &str, artifact_id: &str) -> String {
    format!("record/artifact/{tenant_id}/{artifact_id}")
}

/// Secondary index keyed by sha256 hex digest. Populated on every artifact
/// write so a future dedup pass can land without rewriting the batched-write
/// path; the current API does not expose lookup by sha256.
pub fn artifact_by_sha256_key(tenant_id: &str, sha256_hex: &str) -> String {
    format!("record/artifact_by_sha256/{tenant_id}/{sha256_hex}")
}

/// Primary key for a searchable record. Disjoint from
/// `record/{tenant,agent,session,artifact}/...` because the segment after
/// `record/` is `searchable`, and disjoint from
/// `record/searchable_by_namespace/...` because the prefix `record/searchable/`
/// ends in `/` while the index prefix continues with `_`.
pub fn record_key(tenant_id: &str, record_id: &str) -> String {
    format!("record/searchable/{tenant_id}/{record_id}")
}

/// Secondary index over `(tenant_id, namespace_path)` for prefix-scanning
/// every record under a namespace. `namespace_path` is the slash-joined
/// namespace components (matching `SearchableRecord::namespace_path()`).
pub fn record_by_namespace_key(tenant_id: &str, namespace_path: &str, record_id: &str) -> String {
    format!("record/searchable_by_namespace/{tenant_id}/{namespace_path}/{record_id}")
}

/// Primary key for a skill record.
pub fn skill_record_key(tenant_id: &str, skill_id: &str) -> String {
    format!("record/skill/{tenant_id}/{skill_id}")
}

/// Uniqueness index for `(tenant_id, skill_name, version)`.
pub fn skill_unique_key(tenant_id: &str, name: &str, version: &str) -> String {
    format!("record/skill_unique/{tenant_id}/{name}/{version}")
}

/// Agent-to-skill binding row. `skill_id` is enough because each skill row
/// carries its exact `(name, version)`.
pub fn agent_skill_binding_key(tenant_id: &str, agent_id: &str, skill_id: &str) -> String {
    format!("record/agent_skill/{tenant_id}/{agent_id}/{skill_id}")
}
