use serde::{Deserialize, Serialize};

use crate::v2::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub tenant_id: Id,
    pub external_key: String,
    pub policy: serde_json::Value,
    pub created_at_ms: u64,
    /// Soft-delete marker. None = live; Some(ts) = logically deleted at ts.
    /// Reads filter `deleted_at_ms IS NULL` by default. GC (V2.4) physically
    /// removes the record after the retention window from policy elapses.
    pub deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewTenant {
    pub external_key: String,
    pub policy: serde_json::Value,
}

impl Tenant {
    pub fn create(req: NewTenant) -> Self {
        Self {
            tenant_id: Id::new(IdKind::Tenant),
            external_key: req.external_key,
            policy: req.policy,
            created_at_ms: super::now_ms(),
            deleted_at_ms: None,
        }
    }
}
