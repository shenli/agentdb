use serde::{Deserialize, Serialize};

use crate::id::{Id, IdKind};
use crate::segment::{ReplayPrefixManifest, SessionArchiveManifest, SessionSegment};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRef {
    pub session_id: Id,
    pub at_seq: u64,
}

/// One layer of a sandbox snapshot. A snapshot is layered, not monolithic
/// (base image / deps / repo / artifacts), so AgentDB stores a *set* of layer
/// references rather than a single opaque image. See
/// `docs/architecture/agent-runtime-architecture.md` (snapshot-ref protocol).
///
/// AgentDB holds only the *reference*, never the bytes; `ref_key` resolves in an
/// external content/object store or the sandbox filesystem layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLayer {
    pub name: String,
    pub ref_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// A durable reference to a sandbox snapshot taken at a given session `seq`.
/// Resume restores the referenced snapshot, then replays the session tail after
/// `at_seq`. Snapshot refs carry their own (shorter, tiered) retention via
/// `expires_at_ms` and may dangle once expired; resume falls back to an earlier
/// snapshot plus replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub snapshot_id: Id,
    pub at_seq: u64,
    pub layers: Vec<SnapshotLayer>,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

impl SnapshotRef {
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms.is_some_and(|expiry| now_ms >= expiry)
    }
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
    /// Durable materialized logical replay prefix for forked sessions. This
    /// contains ancestor lineage events up to the leaf fork point; leaf-session
    /// events remain in the normal hot/sealed/archive path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_prefix: Option<ReplayPrefixManifest>,
    /// Sandbox snapshot references recorded over the session's life, ordered by
    /// `at_seq`. AgentDB holds refs only; bytes live in the sandbox FS / object
    /// store. See `record_session_snapshot` / `resolve_resume_snapshot`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<SnapshotRef>,
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
            replay_prefix: None,
            snapshots: vec![],
            deleted_at_ms: None,
        }
    }
}
