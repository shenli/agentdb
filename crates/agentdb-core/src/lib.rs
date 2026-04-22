//! AgentDB core types, metadata registries, and box runtime.

pub mod admission;
pub mod box_runtime;
pub mod error;
pub mod fork;
pub mod hedge;
pub mod hlc;
pub mod index_snapshot;
pub mod index_snapshot_job;
pub mod indexes;
pub mod lease;
pub mod lifecycle;
pub(crate) mod meta;
pub mod metrics;
pub mod planner;
pub mod primitives;
pub mod read_ctx;
pub mod registry;
pub mod scheduler;
pub mod segment;
pub mod segment_gc;
pub mod segment_pin;
pub mod tiering;
pub mod types;

pub use admission::{AdmissionDenied, Scope, TokenBucket};
pub use box_runtime::{
    BoxRegistry, Box_, FaultKind, InjectedFault, Quota, DEFAULT_ADMISSION_CAPACITY,
};
pub use error::{Error, Resource, Result};
pub use fork::fork_session;
pub use hedge::{hedged, HedgedResult, Winner as HedgeWinner};
pub use hlc::{HlcClock, HlcError, HlcTimestamp};
pub use index_snapshot::{
    latest_snapshot_path, list_snapshots, prune_old_snapshots, read_snapshot, restore_indexes,
    snapshot_indexes, snapshot_path, snapshot_prefix, write_snapshot, IndexSnapshot,
    INDEX_SNAPSHOT_SCHEMA_VERSION,
};
pub use index_snapshot_job::{snapshot_once, IndexSnapshotJob, IndexSnapshotJobConfig};
pub use indexes::PerBoxIndexes;
pub use lease::{LeaseConfig, LeaseManager, LeaseRenewalJob};
pub use lifecycle::{LifecycleConfig, LifecycleEngine};
pub use metrics::{BoxMetrics, GaugeSnapshot, MetricsSnapshot};
pub use planner::{Hit, HybridPlanner, PlanOutput, Source, Strategy};
pub use primitives::{
    append_and_fetch, append_and_fetch_with_actor, append_event, fetch_last_n, fetch_last_n_merged,
    scan_session_all_tiers, scan_session_events, AppendFetchOutput, DEFAULT_BRANCH_ID,
};
pub use read_ctx::ReadCtx;
pub use registry::{SessionRegistry, TenantRegistry};
pub use scheduler::{LeaderElection, LeaderElectionConfig};
pub use segment::{read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_VERSION};
pub use segment_gc::{GcRunReporter, SegmentGc, SegmentGcConfig};
pub use segment_pin::SegmentPinRegistry;
pub use tiering::{cold_transition, reactivate};
pub use types::{Document, Event, ForkRef, Session, Tenant, Tier};
