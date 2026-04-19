//! AgentDB core types, metadata registries, and box runtime.

pub mod box_runtime;
pub mod error;
pub mod indexes;
pub mod lifecycle;
pub(crate) mod meta;
pub mod planner;
pub mod primitives;
pub mod registry;
pub mod segment;
pub mod tiering;
pub mod types;

pub use box_runtime::{BoxRegistry, Box_};
pub use error::{Error, Result};
pub use indexes::PerBoxIndexes;
pub use lifecycle::{LifecycleConfig, LifecycleEngine};
pub use planner::{Hit, HybridPlanner, PlanOutput, Source, Strategy};
pub use primitives::{
    append_and_fetch, append_event, fetch_last_n, scan_session_events, AppendFetchOutput,
    DEFAULT_BRANCH_ID,
};
pub use registry::{SessionRegistry, TenantRegistry};
pub use segment::{read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_V1};
pub use tiering::{cold_transition, reactivate};
pub use types::{Document, Event, ForkRef, Session, Tenant, Tier};
