//! Tenant-first logical key encoders for V2 runtime objects.
//!
//! See `docs/v2_design/minimal-single-machine-implementation.md` §7 for the
//! canonical key namespace. Every key starts with the namespace, then the
//! tenant_id, then the object identity. This guarantees that a tenant-scoped
//! prefix scan returns only that tenant's data and that range delete is a
//! single prefix operation.

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
