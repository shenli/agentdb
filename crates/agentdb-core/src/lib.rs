//! AgentDB core types, metadata registries, and box runtime.

pub mod admission;
pub mod backup;
pub mod backup_scheduler;
pub mod box_runtime;
pub mod error;
pub mod fold;
pub mod forwarding;
pub mod fork;
pub mod handoff;
pub mod hedge;
pub mod histogram;
pub mod hlc;
pub mod hot_tablet;
pub mod index_snapshot;
pub mod index_snapshot_job;
pub mod indexes;
pub mod lease;
pub mod lifecycle;
pub(crate) mod meta;
pub mod metrics;
pub mod node_reaper;
pub mod planner;
pub mod primitives;
pub mod rate_window;
pub mod read_ctx;
pub mod reaper;
pub mod registry;
pub mod scheduler;
pub mod segment;
pub mod segment_gc;
pub mod segment_pin;
pub mod shard;
pub mod slow_query;
pub mod tenant_quota;
pub mod tiering;
pub mod types;

pub use admission::{AdmissionDenied, Scope, TokenBucket};
pub use backup::{
    list_backup_dirs, prune_backup_dirs, run_backup, BackupManifest, BACKUP_MANIFEST_VERSION,
};
pub use backup_scheduler::{
    BackupScheduler, BackupSchedulerConfig, BackupSchedulerHandle, BackupSchedulerStatus,
};
pub use box_runtime::{
    BoxRegistry, Box_, FaultKind, InjectedFault, Quota, RouteDecision,
    DEFAULT_ADMISSION_CAPACITY,
};
pub use error::{Error, Resource, Result};
pub use forwarding::{NodeAddressProxy, NodeAddressRefreshJob, NodeTarget};
pub use fold::{fold_events, TOMBSTONE_OF_MARKER, UPDATE_OF_MARKER};
pub use fork::fork_session;
pub use handoff::{migrate_warm_session, MigratedSession};
pub use hedge::{hedged, HedgedResult, Winner as HedgeWinner};
pub use histogram::{HistogramSnapshot, LatencyHistogram, BUCKET_NS};
pub use hlc::{HlcClock, HlcError, HlcTimestamp};
pub use hot_tablet::{HotTablet, HotTabletConfig, HotTabletDetector, TabletLoad};
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
pub use node_reaper::{NodeReaper, NodeReaperConfig};
pub use planner::{Hit, HybridPlanner, PlanOutput, Source, Strategy};
pub use primitives::{
    append_and_fetch, append_and_fetch_with_actor, append_event, fetch_last_n, fetch_last_n_merged,
    scan_session_all_tiers, scan_session_events, AppendFetchOutput, DEFAULT_BRANCH_ID,
};
pub use rate_window::{RateSnapshot, RateWindow};
pub use read_ctx::ReadCtx;
pub use reaper::{ReaperConfig, StaleTabletReaper};
pub use registry::{SessionRegistry, TenantRegistry};
pub use slow_query::{
    SlowQueryBuffer, SlowQueryFilter, SlowQueryKind, SlowQueryRecord,
    DEFAULT_BUFFER_CAPACITY as SLOW_QUERY_DEFAULT_BUFFER_CAPACITY,
    DEFAULT_THRESHOLD_MS as SLOW_QUERY_DEFAULT_THRESHOLD_MS,
};
pub use tenant_quota::{
    ReloadDiff as TenantQuotaReloadDiff, TenantQuotaConfig, TenantQuotaRegistry, TenantQuotaView,
};
pub use scheduler::{LeaderElection, LeaderElectionConfig};
pub use segment::{read_segment, segment_path, write_segment, SegmentFile, SEGMENT_SCHEMA_VERSION};
pub use segment_gc::{GcRunReporter, SegmentGc, SegmentGcConfig};
pub use segment_pin::SegmentPinRegistry;
pub use shard::{shard_id, LocationProxy, LocationProxyRefreshJob, SHARD_COUNT};
pub use tiering::{cold_transition, reactivate};
pub use types::{Document, Event, ForkRef, Session, Tenant, Tier};
