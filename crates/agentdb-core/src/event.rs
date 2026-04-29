use serde::{Deserialize, Serialize};

use crate::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<Id>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub event_id: Id,
    pub seq: u64,
    pub created_at_ms: u64,
    pub kind: String,
    pub schema_version: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub policy_version: u64,
    /// See `Tenant::deleted_at_ms` for the soft-delete contract that applies
    /// uniformly to every V2 runtime object.
    pub deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewEvent {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub seq: u64,
    pub kind: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    pub idempotency_key: Option<String>,
    pub policy_version: u64,
}

impl EventEnvelope {
    pub fn create(req: NewEvent) -> Self {
        Self {
            tenant_id: req.tenant_id,
            agent_id: req.agent_id,
            session_id: req.session_id,
            event_id: Id::new(IdKind::Event),
            seq: req.seq,
            created_at_ms: crate::now_ms(),
            kind: req.kind,
            schema_version: crate::VERSION.to_string(),
            payload: req.payload,
            artifact_refs: req.artifact_refs,
            source_refs: req.source_refs,
            idempotency_key: req.idempotency_key,
            policy_version: req.policy_version,
            deleted_at_ms: None,
        }
    }
}
