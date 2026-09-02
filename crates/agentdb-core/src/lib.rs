//! AgentDB runtime database.
//!
//! See `docs/design/design.md` for the design and
//! `docs/design/architecture.md` for the implementation map.
//!
//! Top-level types:
//! - [`Tenant`], [`Agent`], [`Session`], [`EventEnvelope`] — runtime
//!   objects.
//! - [`Runtime`] — the in-process orchestrator that owns tenants, agents,
//!   sessions, idempotency state, and the optional KV-backed persistence
//!   layer.
//! - [`Id`] — typed-prefix UUIDv7 identifier shared across all
//!   resources.
//! - [`Error`] / [`ErrorCode`] — structured runtime errors.

/// Runtime release identifier, ISO date — opaque, do not parse.
pub const RUNTIME_VERSION: &str = "2026-04-28";
/// Backward-compatible alias for callers that still use the old name.
pub const VERSION: &str = RUNTIME_VERSION;

pub mod agent;
pub mod artifact;
pub mod errors;
pub mod event;
pub mod global_index;
pub mod id;
pub mod idempotency;
pub(crate) mod persist;
pub mod policy;
pub mod record;
pub mod runtime;
pub mod segment;
pub mod session;
pub mod skill;
pub mod tablet_state;
pub mod tenant;

pub use agent::{Agent, NewAgent};
pub use artifact::{Artifact, NewArtifact};
pub use errors::{Error, ErrorCode};
pub use event::{ArtifactRef, CredentialRef, EventEnvelope, NewEvent, SourceRef};
pub use id::{Id, IdKind};
pub use idempotency::{IdempotencyRecord, IdempotencyStore};
pub use policy::{
    ArtifactSizePolicy, LifecyclePolicy, Priority, QuotaPolicy, RetentionPolicy, RuntimePolicy,
};
pub use record::{IndexingMode, IndexingPolicy, NewRecord, RecordStatus, SearchableRecord};
pub use runtime::{
    is_system_tenant, BackpressureReport, BackpressureSignal, ChangefeedItem, ContextInclude,
    ContextRequest, ContextResponse, FleetStorageSummary, ForkEdge, ForkSessionRequest,
    GcSweepReport, LifecycleCounts, LifecycleSweepReport, MetadataFilter, MetadataFilterCondition,
    MetadataFilterOp, NewRecordEdge, NewSnapshotRef, PolicyWorkerReport, RecordEdge,
    RecordEdgeType, RecordIndexStats, RecordSearchMatch, RecordSearchScoreComponents, RecordUpdate,
    ReplayResponse, Runtime, SessionStorageSummary, SnapshotResolution, TenantKeyProvisioner,
    TenantKeyRotator, TenantMetaEntry,
    TenantStorageSummary, TenantWorkerReport, WatchRequest, WatchScope, RUNTIME_PRIMARY_TABLET_ID,
    SYSTEM_TENANT_EXTERNAL_KEY, SYSTEM_TENANT_ID,
};
pub use segment::{
    ReplayPrefixFile, ReplayPrefixManifest, SessionArchiveFile, SessionArchiveManifest,
    SessionSegment, SessionSegmentFile,
};
pub use session::{Lifecycle, NewSession, Session, SnapshotLayer, SnapshotRef};
pub use skill::{AgentSkillBinding, BindSkillRequest, NewSkill, Skill};
pub use tablet_state::{
    decode_sst_snapshot_payload, encode_sst_snapshot_payload, is_sst_snapshot_payload,
    DecodedSstSnapshot, LoggedTabletStateMachine, SstSnapshotMeta, TabletApplyResult,
    TabletCommand, TabletStateMachine, TabletWriteOp,
};
pub use tenant::{NewTenant, Tenant};

pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
