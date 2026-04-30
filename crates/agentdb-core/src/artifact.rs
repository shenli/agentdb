//! Artifact runtime type — large payloads referenced by events.
//!
//! Artifacts are tenant-scoped (NOT agent-scoped, per V2 §13.6). The metadata
//! lives in the KV store alongside tenants/agents/sessions; the bytes live in
//! a separate `agentdb_storage::ContentBackend`. Events reference artifacts
//! through `event.artifact_refs[].artifact_id`.

use serde::{Deserialize, Serialize};

use agentdb_storage::content::PutResult;

use crate::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub artifact_id: Id,
    pub tenant_id: Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_key_version: Option<String>,
    pub created_at_ms: u64,
    /// See `Tenant::deleted_at_ms` for the soft-delete contract that applies
    /// uniformly to every V2 runtime object.
    pub deleted_at_ms: Option<u64>,
    /// Set when the raw content has been physically purged via the
    /// derivative-only lifecycle path (V2.4 GC). Metadata + derivatives
    /// remain; raw content is gone. `None` if raw is still readable.
    pub raw_purged_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewArtifact {
    pub tenant_id: Id,
    pub content_type: Option<String>,
    /// Caller-provided sha256 hex. If `Some`, the runtime verifies the bytes
    /// hash to this value and rejects with `HashMismatch` on mismatch. If
    /// `None`, the runtime trusts the digest computed by the content backend.
    pub expected_sha256: Option<String>,
}

impl Artifact {
    /// Build an `Artifact` from a `NewArtifact` plus the content backend's
    /// `PutResult`. The artifact_id is freshly generated here, mirroring the
    /// other `*::create` factories on `Tenant`/`Agent`/`Session`.
    ///
    /// Public for test harness reuse; production callers go through
    /// `Runtime::create_artifact` which also writes the bytes to the content
    /// backend and the metadata to the KV store atomically.
    pub fn from_put(req: NewArtifact, put: PutResult) -> Self {
        Self {
            artifact_id: Id::new(IdKind::Artifact),
            tenant_id: req.tenant_id,
            content_type: req.content_type,
            bytes: put.bytes,
            sha256: put.sha256_hex,
            content_key_id: None,
            content_key_version: None,
            metadata_key_id: None,
            metadata_key_version: None,
            created_at_ms: crate::now_ms(),
            deleted_at_ms: None,
            raw_purged_at_ms: None,
        }
    }
}
