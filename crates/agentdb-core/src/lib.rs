//! AgentDB core types, metadata registries, and box runtime.

pub mod admission;
pub mod box_runtime;
pub mod error;
pub mod fork;
pub mod indexes;
pub mod lifecycle;
pub(crate) mod meta;
pub mod metrics;
pub mod planner;
pub mod primitives;
pub mod read_ctx;
pub mod registry;
pub mod segment;
pub mod segment_gc;
pub mod segment_pin;
pub mod tiering;
pub mod types;

pub use admission::{AdmissionDenied, Scope, TokenBucket};
pub use box_runtime::{BoxRegistry, Box_, FaultKind, InjectedFault, Quota};
pub use error::{Error, Result};
pub use fork::fork_session;
pub use indexes::PerBoxIndexes;
pub use lifecycle::{LifecycleConfig, LifecycleEngine};
pub use metrics::{BoxMetrics, GaugeSnapshot, MetricsSnapshot};
pub use planner::{Hit, HybridPlanner, PlanOutput, Source, Strategy};
pub use primitives::{
    append_and_fetch, append_event, fetch_last_n, fetch_last_n_merged, scan_session_events,
    AppendFetchOutput, DEFAULT_BRANCH_ID,
};
pub use read_ctx::ReadCtx;
pub use registry::{SessionRegistry, TenantRegistry};
pub use segment::{read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_V1};
pub use segment_gc::{GcRunReporter, SegmentGc, SegmentGcConfig};
pub use segment_pin::SegmentPinRegistry;
pub use tiering::{cold_transition, reactivate};
pub use types::{Document, Event, ForkRef, Session, Tenant, Tier};
