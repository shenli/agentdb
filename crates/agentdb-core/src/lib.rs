//! AgentDB runtime database.
//!
//! See `docs/DESIGN.md` for the design and
//! `docs/ARCHITECTURE.md` for the implementation map.
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
pub mod tenant;

pub use agent::{Agent, NewAgent};
pub use artifact::{Artifact, NewArtifact};
pub use errors::{Error, ErrorCode};
pub use event::{ArtifactRef, EventEnvelope, NewEvent, SourceRef};
pub use id::{Id, IdKind};
pub use idempotency::{IdempotencyRecord, IdempotencyStore};
pub use policy::{
    ArtifactSizePolicy, LifecyclePolicy, Priority, QuotaPolicy, RetentionPolicy, RuntimePolicy,
};
pub use record::{IndexingMode, IndexingPolicy, NewRecord, SearchableRecord};
pub use runtime::{
    BackpressureReport, BackpressureSignal, ChangefeedItem, ContextInclude, ContextRequest,
    ContextResponse, FleetStorageSummary, ForkSessionRequest, GcSweepReport, LifecycleCounts,
    LifecycleSweepReport, PolicyWorkerReport, RecordIndexStats, ReplayResponse, Runtime,
    SessionStorageSummary, TenantStorageSummary, TenantWorkerReport, WatchRequest, WatchScope,
};
pub use segment::{
    SessionArchiveFile, SessionArchiveManifest, SessionSegment, SessionSegmentFile,
};
pub use session::{Lifecycle, NewSession, Session};
pub use skill::{AgentSkillBinding, BindSkillRequest, NewSkill, Skill};
pub use tenant::{NewTenant, Tenant};

pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
