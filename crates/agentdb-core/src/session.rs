use serde::{Deserialize, Serialize};

use crate::id::{Id, IdKind};
use crate::segment::{SessionArchiveManifest, SessionSegment};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Active,
    Paused,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle {
    Warm,
    Cold,
    Archive,
    /// Raw artifacts purged; metadata + derivatives retained. See V2 §12.8 +
    /// V2.4 milestone for the GC contract that drives this transition.
    DerivativeOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkRef {
    pub session_id: Id,
    pub at_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: Id,
    pub tenant_id: Id,
    pub agent_id: Id,
    pub alias: Option<String>,
    pub status: Status,
    pub lifecycle: Lifecycle,
    pub next_seq: u64,
    pub forked_from: Option<ForkRef>,
    pub created_at_ms: u64,
    pub last_active_at_ms: u64,
    pub policy_version: u64,
    #[serde(default)]
    pub warm_head_from_seq: u64,
    #[serde(default)]
    pub sealed_segments: Vec<SessionSegment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_manifest: Option<SessionArchiveManifest>,
    /// See `Tenant::deleted_at_ms` for the soft-delete contract that applies
    /// uniformly to every V2 runtime object.
    pub deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewSession {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub alias: Option<String>,
}

impl Session {
    pub fn create(req: NewSession) -> Self {
        Self::create_with_id(req, Id::new(IdKind::Session))
    }

    pub fn create_with_id(req: NewSession, session_id: Id) -> Self {
        let now = crate::now_ms();
        Self {
            session_id,
            tenant_id: req.tenant_id,
            agent_id: req.agent_id,
            alias: req.alias,
            status: Status::Active,
            lifecycle: Lifecycle::Warm,
            next_seq: 0,
            forked_from: None,
            created_at_ms: now,
            last_active_at_ms: now,
            policy_version: 1,
            warm_head_from_seq: 0,
            sealed_segments: vec![],
            archive_manifest: None,
            deleted_at_ms: None,
        }
    }
}
