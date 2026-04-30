//! AgentDB runtime database.
//!
//! See `docs/v2_design/agent-runtime-database.md` for the design and
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

/// Schema/API version, ISO date — opaque, do not parse.
pub const VERSION: &str = "2026-04-28";

pub mod agent;
pub mod artifact;
pub mod errors;
pub mod event;
pub mod id;
pub mod idempotency;
pub(crate) mod persist;
pub mod record;
pub mod runtime;
pub mod session;
pub mod skill;
pub mod tenant;

pub use agent::{Agent, NewAgent};
pub use artifact::{Artifact, NewArtifact};
pub use errors::{Error, ErrorCode};
pub use event::{ArtifactRef, EventEnvelope, NewEvent, SourceRef};
pub use id::{Id, IdKind};
pub use idempotency::{IdempotencyRecord, IdempotencyStore};
pub use record::{IndexingMode, IndexingPolicy, NewRecord, SearchableRecord};
pub use runtime::Runtime;
pub use session::{NewSession, Session};
pub use skill::{AgentSkillBinding, BindSkillRequest, NewSkill, Skill};
pub use tenant::{NewTenant, Tenant};

pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
