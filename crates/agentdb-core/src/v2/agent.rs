use serde::{Deserialize, Serialize};

use crate::v2::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub agent_id: Id,
    pub tenant_id: Id,
    pub alias: Option<String>,
    pub config: serde_json::Value,
    pub created_at_ms: u64,
    /// See `Tenant::deleted_at_ms` for the soft-delete contract that applies
    /// uniformly to every V2 runtime object.
    pub deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewAgent {
    pub tenant_id: Id,
    pub alias: Option<String>,
    pub config: serde_json::Value,
}

impl Agent {
    pub fn create(req: NewAgent) -> Self {
        Self {
            agent_id: Id::new(IdKind::Agent),
            tenant_id: req.tenant_id,
            alias: req.alias,
            config: req.config,
            created_at_ms: super::now_ms(),
            deleted_at_ms: None,
        }
    }
}
