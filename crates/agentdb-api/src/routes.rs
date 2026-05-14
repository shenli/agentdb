//! HTTP routes for the V2 runtime.
//!
//! Routes are mounted under `/api/...`. Each handler:
//! 1. Parses path params (typed Ids) — bad input becomes a structured 404.
//! 2. Calls the runtime.
//! 3. Returns either a serde-serialized runtime type or an `ApiError`.
//!
//! Wire DTOs live in `crate::dto`. Status codes for `agentdb_core::Error` live
//! in `crate::errors`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, Path, Query, RawQuery, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use agentdb_core::{
    record::{NewRecord, SearchableRecord},
    runtime::{
        AppendRequest, ContextInclude, ContextRequest, FetchRequest, ForkSessionRequest,
        SearchRequest, WatchRequest, WatchScope,
    },
    Agent, AgentSkillBinding, Artifact, BindSkillRequest, ChangefeedItem, ErrorCode, Id, IdKind,
    NewAgent, NewArtifact, NewSession, NewSkill, NewTenant, Skill, SourceRef, Tenant,
};
use agentdb_meta::{
    cells, nodes, placement_scopes, session_locators, tablet_movements, tablet_ranges,
    tablet_rebalance, tablet_replicas, tablet_snapshots, tablet_stats, tablets, tenant_policies,
    tenants,
};

use crate::dto::{
    AppendAndFetchBody, AppendBody, BindSkillBody, ContextBody, CreateAgentBody, CreateRecordBody,
    CreateSessionBody, CreateSkillBody, CreateTenantBody, FetchQuery, ForkSessionBody,
    IncludeDeletedQuery, ListAgentSkillsResponse, PolicyWorkerRunBody, ReplayQuery,
    RestoreSessionStorageBody, SearchRecordsBody, SearchRecordsResponse, SweepBody,
    TransitionSessionLifecycleBody, WatchQuery,
};
use crate::errors::{parse_id_or, ApiError};
use crate::internal_raft_http::{
    RaftRsHttpOutbox, TabletMovementSnapshotExportRequest, TabletMovementSnapshotExportResponse,
};
use crate::routing_http::{
    route_rejection_response, ForwardRequest, RouteGuardOutcome, HDR_FORWARDED_BY,
    HDR_PREALLOCATED_SESSION_ID, HDR_ROUTE_REASON, HDR_SHARD_ID, HDR_TABLET_ID,
};
use crate::state::AppState;

const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";
const SHA256_HEADER: &str = "X-Sha256";
const BACKPRESSURE_HEADER: &str = "X-AgentDB-Backpressure";
const RECORD_COMMIT_SEQ_HEADER: &str = "X-AgentDB-Record-Commit-Seq";
const INTERNAL_AUTH_HEADER: &str = "X-AgentDB-Internal-Token";
const DEFAULT_RAFT_SNAPSHOT_RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;

/// Per-request body cap for the artifact POST route. The runtime enforces a
/// 16 MB ceiling on the bytes; we allow a single MB of slack at the HTTP
/// layer so a borderline-oversized body still reaches the handler and
/// surfaces a structured `ARTIFACT_TOO_LARGE` 422 instead of axum's default
/// "payload too large" / connection reset.
const ARTIFACT_BODY_LIMIT_BYTES: usize = 17 * 1024 * 1024;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health/ready", get(health_ready))
        .route(
            "/api/internal/raft/messages",
            post(post_internal_raft_message),
        )
        .route(
            "/api/internal/raft/snapshots",
            post(post_internal_raft_snapshot),
        )
        .route(
            "/api/internal/tablet-movements/snapshot/export",
            post(post_internal_tablet_movement_snapshot_export),
        )
        .route("/api/admin/cluster/health", get(get_cluster_health))
        .route("/api/admin/cluster/readiness", get(get_cluster_readiness))
        .route("/api/admin/scheduler", get(get_admin_scheduler))
        .route(
            "/api/admin/drain",
            get(get_admin_drain).post(post_admin_drain),
        )
        .route(
            "/api/admin/nodes",
            get(get_admin_nodes).post(register_admin_node),
        )
        .route(
            "/api/admin/cells",
            get(get_admin_cells).post(upsert_admin_cell),
        )
        .route("/api/admin/nodes/:node_id", delete(delete_admin_node))
        .route(
            "/api/admin/nodes/:node_id/maintenance",
            post(set_admin_node_maintenance),
        )
        .route("/api/admin/tablets", get(get_admin_tablets))
        .route("/api/admin/tablets/create", post(create_admin_tablet))
        .route("/api/admin/tablet-ranges", get(get_admin_tablet_ranges))
        .route(
            "/api/admin/tablet-ranges/replace",
            post(replace_admin_tablet_ranges),
        )
        .route(
            "/api/admin/tablet-movements",
            get(get_admin_tablet_movements),
        )
        .route("/api/admin/tablet-stats", get(get_admin_tablet_stats))
        .route(
            "/api/admin/tablets/:tablet_id/split/prepare",
            post(prepare_admin_tablet_split),
        )
        .route(
            "/api/admin/tablets/:tablet_id/split/complete",
            post(complete_admin_tablet_split),
        )
        .route(
            "/api/admin/tablets/merge/prepare",
            post(prepare_admin_tablet_merge),
        )
        .route(
            "/api/admin/tablets/merge/complete",
            post(complete_admin_tablet_merge),
        )
        .route(
            "/api/admin/tenant-policies",
            get(get_admin_tenant_policies).post(upsert_admin_tenant_policy),
        )
        .route(
            "/api/admin/tenant-policies/:tenant_id",
            delete(delete_admin_tenant_policy),
        )
        .route(
            "/api/admin/tablets/rebalance",
            post(rebalance_admin_tablets),
        )
        .route(
            "/api/admin/tablets/:tablet_id/assign",
            post(assign_admin_tablet),
        )
        .route(
            "/api/admin/tablets/:tablet_id/relinquish",
            post(relinquish_admin_tablet),
        )
        .route(
            "/api/admin/tablets/:tablet_id/handoff",
            post(mark_admin_tablet_handoff),
        )
        .route(
            "/api/admin/tablets/:tablet_id/handoff/complete",
            post(complete_admin_tablet_handoff),
        )
        .route(
            "/api/admin/tablets/:tablet_id/handoff/cancel",
            post(cancel_admin_tablet_handoff),
        )
        .route(
            "/api/admin/tablets/:tablet_id/replacement/prepare",
            post(prepare_admin_tablet_replacement),
        )
        .route(
            "/api/admin/tablets/:tablet_id/replacement/complete",
            post(complete_admin_tablet_replacement),
        )
        .route("/api/admin/routing", get(get_routing_status))
        .route("/api/admin/routing/decision", get(get_routing_decision))
        .route("/api/admin/raft", get(get_raft_status))
        .route("/api/admin/raft/catch-up", post(post_raft_catch_up))
        .route("/api/admin/raft/snapshots", get(get_raft_snapshots))
        .route(
            "/api/admin/raft/snapshots/prune",
            post(post_raft_snapshot_prune),
        )
        .route("/api/admin/indexes/records", get(get_record_index_status))
        .route("/api/admin/content-mirror", get(get_content_mirror_status))
        .route("/api/admin/backups", post(create_admin_backup))
        .route("/api/admin/restores", post(validate_admin_restore))
        .route("/api/storage/summary", get(get_fleet_storage_summary))
        .route("/api/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/api/tenants/:tenant_id",
            get(get_tenant).delete(delete_tenant),
        )
        .route(
            "/api/tenants/:tenant_id/agents",
            get(list_agents).post(create_agent),
        )
        .route(
            "/api/tenants/:tenant_id/agents/:agent_id",
            get(get_agent).delete(delete_agent),
        )
        .route(
            "/api/tenants/:tenant_id/agents/:agent_id/sessions",
            post(create_session),
        )
        .route(
            "/api/tenants/:tenant_id/agents/:agent_id/skills",
            get(list_agent_skills),
        )
        .route(
            "/api/tenants/:tenant_id/agents/:agent_id/skills/bind",
            post(bind_skill),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id",
            get(get_session).delete(delete_session),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/lifecycle",
            post(transition_session_lifecycle),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/storage",
            get(get_session_storage),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/restore",
            post(restore_session_storage),
        )
        .route(
            "/api/tenants/:tenant_id/storage-summary",
            get(get_tenant_storage_summary),
        )
        .route("/api/tenants/:tenant_id/sessions", get(list_sessions))
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/events",
            post(append_event).get(fetch_events),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/append-and-fetch",
            post(append_and_fetch),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/fork",
            post(fork_session),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/replay",
            get(replay_session),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/subscribe",
            get(subscribe_session),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/context",
            post(build_context),
        )
        .route("/api/tenants/:tenant_id/watch", get(watch))
        .route(
            "/api/tenants/:tenant_id/artifacts",
            post(create_artifact)
                // Raise the per-route body limit so the runtime's 16 MB
                // ARTIFACT_TOO_LARGE check actually fires; axum's default
                // (2 MB) would reject these as PAYLOAD_TOO_LARGE before
                // the handler ever runs.
                .layer(DefaultBodyLimit::max(ARTIFACT_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/tenants/:tenant_id/artifacts/:artifact_id",
            get(get_artifact),
        )
        .route(
            "/api/tenants/:tenant_id/artifacts/:artifact_id/blob",
            get(get_artifact_blob),
        )
        // V2.2 records: register `/records/search` BEFORE
        // `/records/:record_id` so the literal `search` segment beats the
        // dynamic `:record_id` matcher. axum 0.7's matchit-based router
        // does prefer static over dynamic segments at the same depth, but
        // we encode the order here so a future router refactor can't
        // silently swap precedence.
        .route(
            "/api/tenants/:tenant_id/records/search",
            get(search_records).post(search_records_json),
        )
        .route("/api/tenants/:tenant_id/records", post(create_record))
        .route(
            "/api/tenants/:tenant_id/records/:record_id",
            get(get_record).delete(delete_record),
        )
        .route("/api/tenants/:tenant_id/skills", post(create_skill))
        .route(
            "/api/tenants/:tenant_id/skills/:skill_id",
            get(get_skill).delete(delete_skill),
        )
        .route(
            "/api/tenants/:tenant_id/policy/lifecycle-sweep",
            post(run_lifecycle_sweep),
        )
        .route(
            "/api/tenants/:tenant_id/policy/gc-sweep",
            post(run_gc_sweep),
        )
        .route("/api/policy/worker/run", post(run_policy_worker))
}

async fn health_ready() -> StatusCode {
    StatusCode::OK
}

fn now_ms_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn raft_index_to_i64(index: u64) -> Result<i64, ApiError> {
    i64::try_from(index).map_err(|_| internal_error(format!("raft index {index} exceeds i64::MAX")))
}

fn raft_snapshot_id(tablet_id: i64, from: u64, to: u64, index: u64) -> String {
    format!("tablet-{tablet_id}-from-{from}-to-{to}-idx-{index}")
}

fn snapshot_expires_at(now_ms: i64, retention_ms: Option<i64>) -> Result<Option<i64>, ApiError> {
    let retention_ms = retention_ms.unwrap_or(DEFAULT_RAFT_SNAPSHOT_RETENTION_MS);
    if retention_ms < 0 {
        return Err(invalid_admin_request("retention_ms must be non-negative"));
    }
    Ok(now_ms.checked_add(retention_ms))
}

fn drain_status_response(s: &AppState) -> DrainStatusResponse {
    DrainStatusResponse {
        node_id: s.drain.node_id().to_string(),
        is_draining: s.drain.is_draining(),
        drained_at_ms: s.drain.drained_at_ms(),
    }
}

fn reject_if_draining(s: &AppState) -> Result<(), ApiError> {
    if s.drain.is_draining() {
        return Err(ApiError(
            agentdb_core::Error::new(
                ErrorCode::NodeDraining,
                format!(
                    "node {} is draining; retry at a different node",
                    s.drain.node_id()
                ),
            )
            .with_suggestion("See /api/admin/drain and docs/runbooks/node_draining.md"),
        ));
    }
    Ok(())
}

async fn get_routing_status(
    State(s): State<AppState>,
) -> Json<crate::routing_http::RouteRuntimeStatus> {
    Json(s.routing.describe())
}

#[derive(Debug, Deserialize)]
struct ClusterAdminQuery {
    stale_threshold_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct AdminNodeView {
    node_id: String,
    address: String,
    mongo_address: Option<String>,
    state: String,
    cell_id: String,
    last_heartbeat_ms: i64,
    joined_at_ms: i64,
    availability_zone: Option<String>,
    region: Option<String>,
    capacity_class: String,
    is_stale: bool,
}

#[derive(Debug, Deserialize)]
struct RegisterNodeBody {
    node_id: String,
    address: String,
    mongo_address: Option<String>,
    state: Option<String>,
    availability_zone: Option<String>,
    region: Option<String>,
    cell_id: Option<String>,
    capacity_class: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpsertCellBody {
    cell_id: String,
    region: Option<String>,
    domain_class: Option<String>,
    deployment_profile: Option<String>,
    capacity_class: Option<String>,
    metadata_isolation_level: Option<String>,
    state: Option<String>,
    metadata_namespace: Option<String>,
    object_store_namespace: Option<String>,
    #[serde(default)]
    rollout_version: i64,
}

#[derive(Debug, Deserialize)]
struct NodeMaintenanceBody {
    state: String,
}

#[derive(Debug, Serialize)]
struct DeleteAdminNodeResponse {
    node_id: String,
    removed: bool,
}

#[derive(Debug, Serialize)]
struct AdminTabletView {
    tablet_id: i64,
    owner_node_id: Option<String>,
    epoch: i64,
    lease_expires_at_ms: i64,
    created_at_ms: i64,
    placement_scope_id: String,
    state: String,
    raft_group_id: Option<String>,
    encryption_policy_version: i64,
    handoff_target_node_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct AdminTabletReplicaView {
    tablet_id: i64,
    node_id: String,
    role: String,
    health: String,
    applied_index: i64,
    match_index: i64,
    updated_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct CreateTabletBody {
    tablet_id: i64,
    placement_scope_id: Option<String>,
    state: Option<String>,
    raft_group_id: Option<String>,
    encryption_policy_version: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct UpsertTenantPolicyBody {
    tenant_id: String,
    placement_class: String,
    placement_scope_id: String,
    cell_id: Option<String>,
    tier: Option<String>,
    priority: Option<i64>,
    encryption_class: String,
    key_policy_id: Option<String>,
    #[serde(default)]
    encryption_policy_version: i64,
    #[serde(default)]
    policy_version: i64,
}

#[derive(Debug, Serialize)]
struct DeleteTenantPolicyResponse {
    tenant_id: String,
    removed: bool,
}

#[derive(Debug, Deserialize)]
struct ReplaceTabletRangesBody {
    placement_scope_id: String,
    old_ranges: Vec<tablet_ranges::RangeBounds>,
    new_ranges: Vec<tablet_ranges::ReplacementRange>,
    map_epoch: i64,
}

#[derive(Debug, Serialize)]
struct ReplaceTabletRangesResponse {
    placement_scope_id: String,
    changed: bool,
    ranges: Vec<tablet_ranges::TabletRange>,
}

#[derive(Debug, Deserialize)]
struct TabletMovementsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TabletStatsQuery {
    tablet_id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct AdminTabletStatsResponse {
    stats: Vec<tablet_stats::TabletStats>,
    aggregates: Vec<tablet_stats::TabletStatsAggregate>,
}

#[derive(Debug, Deserialize)]
struct SplitTabletBody {
    placement_scope_id: Option<String>,
    child_ranges: Vec<tablet_ranges::ReplacementRange>,
    map_epoch: Option<i64>,
    min_applied_index: Option<i64>,
    require_ready: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct MergeTabletsBody {
    placement_scope_id: Option<String>,
    source_tablet_ids: Vec<i64>,
    destination_tablet_id: i64,
    map_epoch: Option<i64>,
    min_applied_index: Option<i64>,
    require_ready: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AdminTabletRangeLifecycleResponse {
    action: &'static str,
    changed: bool,
    placement_scope_id: String,
    source_tablet_ids: Vec<i64>,
    destination_tablet_ids: Vec<i64>,
    required_applied_index: i64,
    destinations_ready: bool,
    old_ranges: Vec<tablet_ranges::RangeBounds>,
    new_ranges: Vec<tablet_ranges::ReplacementRange>,
    tablets: Vec<AdminTabletView>,
    replicas: Vec<AdminTabletReplicaView>,
    movements: Vec<tablet_movements::TabletMovement>,
    ranges: Vec<tablet_ranges::TabletRange>,
}

#[derive(Debug, Deserialize)]
struct AssignTabletBody {
    node_id: String,
}

#[derive(Debug, Deserialize)]
struct HandoffTabletBody {
    target_node_id: String,
}

#[derive(Debug, Deserialize)]
struct CompleteHandoffBody {
    expected_source_node_id: String,
}

#[derive(Debug, Deserialize)]
struct PrepareTabletReplacementBody {
    target_node_id: String,
    source_node_id: Option<String>,
    stale_threshold_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CompleteTabletReplacementBody {
    target_node_id: String,
    source_node_id: Option<String>,
    min_applied_index: Option<i64>,
    require_healthy: Option<bool>,
    retire_source: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AdminTabletMutationResponse {
    action: &'static str,
    changed: bool,
    tablet: AdminTabletView,
}

#[derive(Debug, Serialize)]
struct AdminTabletHandoffCompleteResponse {
    action: &'static str,
    changed: bool,
    new_owner_node_id: Option<String>,
    tablet: AdminTabletView,
}

#[derive(Debug, Serialize)]
struct AdminTabletReplacementPrepareResponse {
    action: &'static str,
    changed: bool,
    source_node_id: Option<String>,
    target_node_id: String,
    tablet: AdminTabletView,
    source_replica: Option<AdminTabletReplicaView>,
    target_replica: AdminTabletReplicaView,
}

#[derive(Debug, Serialize)]
struct AdminTabletReplacementCompleteResponse {
    action: &'static str,
    changed: bool,
    new_owner_node_id: Option<String>,
    source_node_id: Option<String>,
    target_node_id: String,
    required_applied_index: i64,
    raft_membership_changed: bool,
    raft_membership_operations: Vec<RaftMembershipOperationStatus>,
    retired_source: bool,
    tablet: AdminTabletView,
    source_replica: Option<AdminTabletReplicaView>,
    target_replica: AdminTabletReplicaView,
}

#[derive(Debug, Serialize)]
struct DrainStatusResponse {
    node_id: String,
    is_draining: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    drained_at_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SchedulerStatusResponse {
    leader_node_id: Option<String>,
    leader_epoch: i64,
    leader_expires_at_ms: i64,
    is_leader: bool,
    dead_nodes_marked_total: u64,
    tablet_reassignments_total: u64,
    local_tablet_log_compactions_total: u64,
    local_tablet_log_compaction_failures_total: u64,
    local_tablet_log_compaction_last_tick_at_ms: i64,
    last_tick_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tablet_store: Option<crate::state::TabletStoreStatus>,
}

#[derive(Debug, Deserialize)]
struct CreateBackupBody {
    backup_id: Option<String>,
    directory: Option<String>,
    include_meta: Option<bool>,
    include_runtime_summary: Option<bool>,
    include_tablet_checkpoints: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AdminBackupResponse {
    job_id: String,
    status: &'static str,
    manifest_path: String,
    backup_dir: String,
    meta_backup_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RestoreBody {
    manifest_path: String,
    #[serde(default = "default_true")]
    dry_run: bool,
}

#[derive(Debug, Serialize)]
struct AdminRestoreResponse {
    job_id: String,
    status: &'static str,
    dry_run: bool,
    manifest_path: String,
    backup_id: Option<String>,
    checks: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TabletCheckpointBackup {
    tablet_id: i64,
    path: String,
    manifest: agentdb_storage::TabletStoreManifest,
}

#[derive(Debug, Serialize)]
struct ClusterHealthSummary {
    node_count: usize,
    alive_node_count: usize,
    stale_node_count: usize,
    draining_node_count: usize,
    dead_node_count: usize,
    tablet_count: usize,
    unowned_tablet_count: usize,
    tablets_with_stale_owner_count: usize,
}

#[derive(Debug, Serialize)]
struct ClusterHealthResponse {
    mode: &'static str,
    local_node_id: String,
    status: &'static str,
    stale_threshold_ms: i64,
    single_node: bool,
    local_node_found: bool,
    local_node_stale: bool,
    local_node_state: Option<String>,
    summary: ClusterHealthSummary,
    issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ClusterReadinessResponse {
    mode: &'static str,
    local_node_id: String,
    ready: bool,
    stale_threshold_ms: i64,
    reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ContentMirrorStatusResponse {
    enabled: bool,
    queued: u64,
    completed: u64,
    failed: u64,
    backlog: u64,
}

#[derive(Debug, Serialize)]
struct RaftStatusResponse {
    enabled: bool,
    control_plane_enabled: bool,
    tablet_count: usize,
    configured_replica_count: usize,
    unhealthy_replica_count: usize,
    pending_conf_change_count: usize,
    outbound_message_count: usize,
    summary: RaftOpsSummary,
    tablets: Vec<RaftTabletStatus>,
}

#[derive(Debug, Serialize)]
struct RaftOpsSummary {
    leaderless_tablet_count: usize,
    multi_leader_tablet_count: usize,
    commit_index_drift_tablet_count: usize,
    applied_index_drift_tablet_count: usize,
    max_commit_index_gap: u64,
    max_applied_index_gap: u64,
    catch_up_required_replica_count: usize,
    lagging_replica_count: usize,
    unreachable_replica_count: usize,
    snapshot_absent_for_catch_up_count: usize,
    expired_snapshot_count: usize,
}

#[derive(Debug, Serialize)]
struct RaftTabletStatus {
    tablet_id: i64,
    local_group_open: bool,
    replicas: Vec<RaftReplicaStatus>,
    configured_replicas: Vec<RaftConfiguredReplicaStatus>,
}

#[derive(Debug, Serialize)]
struct RaftReplicaStatus {
    node_id: u64,
    role: String,
    leader_id: u64,
    term: u64,
    commit_index: u64,
    applied_index: u64,
    pending_conf_change: bool,
    outbound_message_count: usize,
}

#[derive(Debug, Serialize)]
struct RaftConfiguredReplicaStatus {
    node_id: String,
    raft_node_id: Option<u64>,
    role: String,
    health: String,
    applied_index: i64,
    match_index: i64,
    replication_lag: u64,
    catch_up_required: bool,
    last_snapshot_index: Option<i64>,
    last_snapshot_age_ms: Option<i64>,
    last_snapshot_status: Option<String>,
    last_snapshot_expires_at_ms: Option<i64>,
    updated_at_ms: i64,
    local_runtime: bool,
    runtime_role: Option<String>,
    runtime_leader_id: Option<u64>,
    runtime_term: Option<u64>,
    runtime_commit_index: Option<u64>,
    runtime_applied_index: Option<u64>,
    membership_state: String,
    pending_conf_change: bool,
    outbound_message_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RaftMembershipOperationStatus {
    tablet_id: i64,
    operation: &'static str,
    node_id: String,
    raft_node_id: Option<u64>,
    status: &'static str,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RaftCatchUpBody {
    tablet_id: Option<i64>,
    target_node_id: Option<String>,
    min_lag: Option<u64>,
    retention_ms: Option<i64>,
    dry_run: Option<bool>,
}

#[derive(Debug, Serialize)]
struct RaftCatchUpResponse {
    dry_run: bool,
    tablet_count: usize,
    attempted: usize,
    sent: usize,
    skipped: usize,
    failures: Vec<RaftCatchUpFailure>,
    actions: Vec<RaftCatchUpAction>,
}

#[derive(Debug, Serialize)]
struct RaftCatchUpAction {
    tablet_id: i64,
    node_id: String,
    from_raft_node_id: u64,
    to_raft_node_id: u64,
    lag: u64,
    snapshot_index: Option<u64>,
    status: String,
}

#[derive(Debug, Serialize)]
struct RaftCatchUpFailure {
    tablet_id: i64,
    node_id: String,
    to_raft_node_id: Option<u64>,
    error: String,
}

#[derive(Debug, Deserialize)]
struct RaftSnapshotsQuery {
    tablet_id: Option<i64>,
    include_deleted: Option<bool>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct RaftSnapshotsResponse {
    snapshots: Vec<RaftSnapshotView>,
}

#[derive(Debug, Serialize)]
struct RaftSnapshotView {
    snapshot_id: String,
    tablet_id: i64,
    source_node_id: String,
    source_raft_node_id: i64,
    target_node_id: String,
    target_raft_node_id: i64,
    snapshot_index: i64,
    snapshot_term: i64,
    size_bytes: i64,
    sha256_hex: String,
    storage_uri: Option<String>,
    status: String,
    created_at_ms: i64,
    updated_at_ms: i64,
    expires_at_ms: Option<i64>,
    age_ms: i64,
    expired: bool,
}

#[derive(Debug, Deserialize)]
struct RaftSnapshotPruneBody {
    dry_run: Option<bool>,
}

#[derive(Debug, Serialize)]
struct RaftSnapshotPruneResponse {
    dry_run: bool,
    expired_snapshot_count: i64,
    deleted_snapshot_count: i64,
}

#[derive(Debug, Deserialize)]
struct InternalRaftMessageBody {
    tablet_id: i64,
    from: u64,
    to: u64,
    payload_base64: String,
}

#[derive(Debug, Deserialize)]
struct InternalRaftSnapshotBody {
    tablet_id: i64,
    from: u64,
    to: u64,
    index: u64,
    term: u64,
    payload_base64: String,
}

#[derive(Debug, Serialize)]
struct InternalRaftAccepted {
    status: &'static str,
    tablet_id: i64,
    from: u64,
    to: u64,
}

async fn get_cluster_health(
    State(s): State<AppState>,
    Query(query): Query<ClusterAdminQuery>,
) -> Json<ClusterHealthResponse> {
    Json(build_cluster_health(
        &s.routing.describe(),
        s.drain.is_draining(),
        query.stale_threshold_ms.unwrap_or(15_000),
    ))
}

async fn get_cluster_readiness(
    State(s): State<AppState>,
    Query(query): Query<ClusterAdminQuery>,
) -> Response {
    let readiness = build_cluster_readiness(
        &s.routing.describe(),
        s.drain.is_draining(),
        query.stale_threshold_ms.unwrap_or(15_000),
    );
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn post_internal_raft_message(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<InternalRaftMessageBody>,
) -> Response {
    if let Err(response) = validate_internal_raft_request(&s, &headers) {
        return response;
    }
    let Some(ingress) = s.raft_ingress.as_ref() else {
        return internal_raft_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "RAFT_INGRESS_UNAVAILABLE",
            "raft ingress is not configured on this node",
        );
    };
    let payload = match decode_base64(&body.payload_base64) {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    if let Err(response) =
        validate_internal_raft_peer_ids(&s, body.tablet_id, body.from, body.to).await
    {
        return response;
    }
    match ingress.step_wire_message(agentdb_storage::RaftRsWireMessage {
        tablet_id: body.tablet_id,
        from: body.from,
        to: body.to,
        payload,
    }) {
        Ok(()) => {
            if let Some(registry) = s.raft_registry.as_ref() {
                if let Ok(statuses) = registry.status_for(body.tablet_id) {
                    if let Some(status) = statuses
                        .into_iter()
                        .find(|status| status.node_id == body.to)
                    {
                        record_local_raft_replica_progress(
                            &s,
                            body.tablet_id,
                            body.to,
                            status.applied_index,
                            status.commit_index,
                        )
                        .await;
                    }
                }
            }
            (
                StatusCode::ACCEPTED,
                Json(InternalRaftAccepted {
                    status: "accepted",
                    tablet_id: body.tablet_id,
                    from: body.from,
                    to: body.to,
                }),
            )
                .into_response()
        }
        Err(err) => internal_raft_error(
            StatusCode::BAD_REQUEST,
            "RAFT_MESSAGE_REJECTED",
            err.to_string(),
        ),
    }
}

async fn post_internal_raft_snapshot(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<InternalRaftSnapshotBody>,
) -> Response {
    if let Err(response) = validate_internal_raft_request(&s, &headers) {
        return response;
    }
    let Some(ingress) = s.raft_ingress.as_ref() else {
        return internal_raft_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "RAFT_INGRESS_UNAVAILABLE",
            "raft ingress is not configured on this node",
        );
    };
    let payload = match decode_base64(&body.payload_base64) {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    if let Err(response) =
        validate_internal_raft_peer_ids(&s, body.tablet_id, body.from, body.to).await
    {
        return response;
    }
    match ingress.install_snapshot_transfer(
        body.to,
        agentdb_storage::RaftRsSnapshotTransfer {
            tablet_id: body.tablet_id,
            from: body.from,
            to: body.to,
            index: body.index,
            term: body.term,
            payload,
        },
    ) {
        Ok(application_snapshot_payload) => {
            if !application_snapshot_payload.is_empty() {
                match s
                    .runtime
                    .install_tablet_snapshot_payload_and_rehydrate(
                        body.tablet_id,
                        &application_snapshot_payload,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        return internal_raft_error(
                            StatusCode::BAD_REQUEST,
                            "RAFT_SNAPSHOT_STATE_REJECTED",
                            err.message,
                        )
                    }
                }
            }
            record_local_raft_replica_progress(&s, body.tablet_id, body.to, body.index, body.index)
                .await;
            (
                StatusCode::ACCEPTED,
                Json(InternalRaftAccepted {
                    status: "accepted",
                    tablet_id: body.tablet_id,
                    from: body.from,
                    to: body.to,
                }),
            )
                .into_response()
        }
        Err(err) => internal_raft_error(
            StatusCode::BAD_REQUEST,
            "RAFT_SNAPSHOT_REJECTED",
            err.to_string(),
        ),
    }
}

async fn post_internal_tablet_movement_snapshot_export(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<TabletMovementSnapshotExportRequest>,
) -> Response {
    if let Err(response) = validate_internal_raft_request(&s, &headers) {
        return response;
    }
    match s.runtime.export_tablet_snapshot_payload_for_shard_range(
        body.source_tablet_id,
        body.target_tablet_id,
        body.start_shard,
        body.end_shard,
        body.shard_count,
    ) {
        Ok(payload) => {
            let applied_index = match serde_json::from_slice::<
                agentdb_core::tablet_state::TabletStateSnapshot,
            >(&payload)
            {
                Ok(snapshot) => snapshot.applied_index,
                Err(err) => {
                    return internal_raft_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "TABLET_MOVEMENT_SNAPSHOT_INVALID",
                        format!("exported tablet movement snapshot is invalid: {err}"),
                    )
                }
            };
            (
                StatusCode::OK,
                Json(TabletMovementSnapshotExportResponse {
                    source_tablet_id: body.source_tablet_id,
                    target_tablet_id: body.target_tablet_id,
                    applied_index,
                    payload_base64: base64::engine::general_purpose::STANDARD.encode(payload),
                }),
            )
                .into_response()
        }
        Err(err) => internal_raft_error(
            StatusCode::BAD_REQUEST,
            "TABLET_MOVEMENT_SNAPSHOT_EXPORT_REJECTED",
            err.message,
        ),
    }
}

#[allow(clippy::result_large_err)]
fn validate_internal_raft_request(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let Some(expected) = state.internal_auth_token.as_ref() else {
        return Err(internal_raft_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "RAFT_INTERNAL_AUTH_UNCONFIGURED",
            "internal raft RPC auth token is not configured on this node",
        ));
    };
    let presented = headers
        .get(INTERNAL_AUTH_HEADER)
        .and_then(|value| value.to_str().ok());
    let previous = state
        .internal_auth_previous_token
        .as_ref()
        .map(|token| token.as_ref());
    let matches_current = presented == Some(expected.as_ref());
    let matches_previous = previous.is_some_and(|previous| presented == Some(previous));
    if !matches_current && !matches_previous {
        return Err(internal_raft_error(
            StatusCode::UNAUTHORIZED,
            "RAFT_INTERNAL_AUTH_FAILED",
            "invalid internal raft RPC token",
        ));
    }
    Ok(())
}

async fn validate_internal_raft_peer_ids(
    state: &AppState,
    tablet_id: i64,
    from: u64,
    to: u64,
) -> Result<(), Response> {
    if tablet_id <= 0 || from == 0 || to == 0 {
        return Err(internal_raft_error(
            StatusCode::BAD_REQUEST,
            "RAFT_PEER_INVALID",
            "raft tablet_id, from, and to must be positive",
        ));
    }
    let Some(meta) = state.meta.as_ref() else {
        return Ok(());
    };
    let replicas = tablet_replicas::list_by_tablet(meta.pool(), tablet_id)
        .await
        .map_err(|err| {
            internal_raft_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "RAFT_PEER_LOOKUP_FAILED",
                err.to_string(),
            )
        })?;
    let has_from = replicas.iter().any(|replica| {
        agentdb_storage::stable_raft_node_id(&replica.node_id)
            .is_ok_and(|candidate| candidate == from)
    });
    let has_to = replicas.iter().any(|replica| {
        agentdb_storage::stable_raft_node_id(&replica.node_id)
            .is_ok_and(|candidate| candidate == to)
    });
    if !has_from || !has_to {
        return Err(internal_raft_error(
            StatusCode::FORBIDDEN,
            "RAFT_PEER_NOT_CONFIGURED",
            format!(
                "raft message peers are not configured for tablet {tablet_id}: from_configured={has_from}, to_configured={has_to}"
            ),
        ));
    }
    Ok(())
}

fn decode_base64(value: &str) -> Result<Vec<u8>, Box<Response>> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|err| {
            Box::new(internal_raft_error(
                StatusCode::BAD_REQUEST,
                "RAFT_PAYLOAD_INVALID",
                format!("invalid base64 raft payload: {err}"),
            ))
        })
}

async fn record_local_raft_replica_progress(
    state: &AppState,
    tablet_id: i64,
    raft_node_id: u64,
    applied_index: u64,
    match_index: u64,
) {
    let Some(meta) = state.meta.as_ref() else {
        return;
    };
    let local_node_id = match resolve_control_plane_node_id(meta, tablet_id, raft_node_id).await {
        Ok(Some(node_id)) => node_id,
        Ok(None) => {
            tracing::debug!(
                tablet_id,
                raft_node_id,
                "skipping raft replica progress update; control-plane node not found"
            );
            return;
        }
        Err(error) => {
            tracing::warn!(
                tablet_id,
                raft_node_id,
                ?error,
                "failed to resolve raft replica control-plane node"
            );
            return;
        }
    };
    let now_ms = now_ms_i64();
    let applied = applied_index.min(i64::MAX as u64) as i64;
    let matched = match_index.min(i64::MAX as u64) as i64;
    if let Err(error) = tablet_replicas::set_progress(
        meta.pool(),
        tablet_id,
        &local_node_id,
        applied,
        matched,
        now_ms,
    )
    .await
    {
        tracing::warn!(
            tablet_id,
            node_id = %local_node_id,
            ?error,
            "failed to update raft replica progress"
        );
        return;
    }
    if let Err(error) =
        tablet_replicas::set_health(meta.pool(), tablet_id, &local_node_id, "healthy", now_ms).await
    {
        tracing::warn!(
            tablet_id,
            node_id = %local_node_id,
            ?error,
            "failed to update raft replica health"
        );
    }
}

async fn resolve_control_plane_node_id(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    raft_node_id: u64,
) -> agentdb_meta::MetaResult<Option<String>> {
    for replica in tablet_replicas::list_by_tablet(meta.pool(), tablet_id).await? {
        let Ok(candidate) = agentdb_storage::stable_raft_node_id(&replica.node_id) else {
            continue;
        };
        if candidate == raft_node_id {
            return Ok(Some(replica.node_id));
        }
    }
    Ok(None)
}

fn internal_raft_error(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
) -> Response {
    let body = serde_json::json!({
        "error": {
            "code": code,
            "message": message.into(),
            "retryable": status.is_server_error(),
        }
    });
    (status, Json(body)).into_response()
}

async fn get_admin_drain(State(s): State<AppState>) -> Json<DrainStatusResponse> {
    Json(drain_status_response(&s))
}

async fn post_admin_drain(
    State(s): State<AppState>,
) -> Result<Json<DrainStatusResponse>, ApiError> {
    let drained_at_ms = s.drain.activate(now_ms_i64());
    if let Some(meta) = s.meta.as_ref() {
        nodes::set_state(meta.pool(), s.drain.node_id(), "draining", drained_at_ms)
            .await
            .map_err(|err| {
                ApiError(agentdb_core::Error::new(
                    ErrorCode::Internal,
                    err.to_string(),
                ))
            })?;
    }
    Ok(Json(drain_status_response(&s)))
}

async fn get_admin_scheduler(State(s): State<AppState>) -> Json<SchedulerStatusResponse> {
    let snapshot = s
        .scheduler
        .as_ref()
        .map(|status| status.snapshot())
        .unwrap_or(crate::state::SchedulerStatusSnapshot {
            leader_node_id: None,
            leader_epoch: 0,
            leader_expires_at_ms: 0,
            is_leader: false,
            dead_nodes_marked_total: 0,
            tablet_reassignments_total: 0,
            local_tablet_log_compactions_total: 0,
            local_tablet_log_compaction_failures_total: 0,
            local_tablet_log_compaction_last_tick_at_ms: 0,
            last_tick_at_ms: 0,
        });
    Json(SchedulerStatusResponse {
        leader_node_id: snapshot.leader_node_id,
        leader_epoch: snapshot.leader_epoch,
        leader_expires_at_ms: snapshot.leader_expires_at_ms,
        is_leader: snapshot.is_leader,
        dead_nodes_marked_total: snapshot.dead_nodes_marked_total,
        tablet_reassignments_total: snapshot.tablet_reassignments_total,
        local_tablet_log_compactions_total: snapshot.local_tablet_log_compactions_total,
        local_tablet_log_compaction_failures_total: snapshot
            .local_tablet_log_compaction_failures_total,
        local_tablet_log_compaction_last_tick_at_ms: snapshot
            .local_tablet_log_compaction_last_tick_at_ms,
        last_tick_at_ms: snapshot.last_tick_at_ms,
        tablet_store: s.tablet_store.clone(),
    })
}

async fn get_record_index_status(
    State(s): State<AppState>,
) -> Json<agentdb_core::RecordIndexStats> {
    Json(s.runtime.record_index_stats().await)
}

async fn create_admin_backup(
    State(s): State<AppState>,
    Json(body): Json<CreateBackupBody>,
) -> Result<Json<AdminBackupResponse>, ApiError> {
    let now_ms = now_ms_i64();
    let backup_id = body.backup_id.unwrap_or_else(|| format!("bak_{now_ms}"));
    if !is_safe_backup_id(&backup_id) {
        return Err(invalid_admin_request(
            "backup_id may only contain ASCII letters, digits, '_', '-', and '.'",
        ));
    }
    let root = body
        .directory
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("agentdb-backups"));
    let backup_dir = root.join(&backup_id);
    if backup_dir.exists() {
        return Err(invalid_admin_request(format!(
            "backup directory already exists: {}",
            backup_dir.display()
        )));
    }
    fs::create_dir_all(&backup_dir).map_err(internal_error)?;

    let include_meta = body.include_meta.unwrap_or(true);
    let include_runtime_summary = body.include_runtime_summary.unwrap_or(true);
    let include_tablet_checkpoints = body.include_tablet_checkpoints.unwrap_or(false);
    let mut meta_backup_path = None;
    if include_meta {
        if let Some(meta) = s.meta.as_ref() {
            let extension = match meta.dialect() {
                agentdb_meta::MetaDialect::Sqlite => "sqlite",
                agentdb_meta::MetaDialect::Postgres => "sql",
            };
            let path = backup_dir.join(format!("meta.{extension}"));
            meta.backup(&path).await.map_err(internal_error)?;
            meta_backup_path = Some(path.display().to_string());
        }
    }

    let runtime_summary = if include_runtime_summary {
        Some(
            serde_json::to_value(s.runtime.fleet_storage_summary().await)
                .map_err(internal_error)?,
        )
    } else {
        None
    };
    let nodes = if let Some(meta) = s.meta.as_ref() {
        let rows = nodes::list_all(meta.pool()).await.map_err(internal_error)?;
        Some(
            serde_json::to_value(
                rows.into_iter()
                    .map(|node| admin_node_view(node, now_ms, 15_000))
                    .collect::<Vec<_>>(),
            )
            .map_err(internal_error)?,
        )
    } else {
        None
    };
    let tablets = if let Some(meta) = s.meta.as_ref() {
        Some(serde_json::to_value(list_tablets_for_backup(meta).await?).map_err(internal_error)?)
    } else {
        None
    };
    let tablet_store_checkpoints = if include_tablet_checkpoints {
        let checkpoint_root = backup_dir.join("tablet-stores");
        fs::create_dir_all(&checkpoint_root).map_err(internal_error)?;
        Some(
            serde_json::to_value(checkpoint_local_tablet_stores(&s, &checkpoint_root).await?)
                .map_err(internal_error)?,
        )
    } else {
        None
    };
    let manifest = serde_json::json!({
        "schema_version": 1,
        "backup_id": backup_id,
        "created_at_ms": now_ms,
        "status": "completed",
        "meta_backup_path": meta_backup_path,
        "runtime_summary": runtime_summary,
        "nodes": nodes,
        "tablets": tablets,
        "tablet_store_checkpoints": tablet_store_checkpoints,
        "notes": [
            "This is an online backup manifest.",
            "Runtime summary is observability metadata, not a full runtime data copy.",
            "Tablet-store checkpoints are included only when requested and require a physical single-tablet domain.",
            "Restore API currently validates manifests only; destructive apply must be done offline."
        ]
    });
    let manifest_path = backup_dir.join("manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(internal_error)?;
    fs::write(&manifest_path, manifest_bytes).map_err(internal_error)?;

    Ok(Json(AdminBackupResponse {
        job_id: format!("job_backup_{now_ms}"),
        status: "completed",
        manifest_path: manifest_path.display().to_string(),
        backup_dir: backup_dir.display().to_string(),
        meta_backup_path,
    }))
}

async fn validate_admin_restore(
    Json(body): Json<RestoreBody>,
) -> Result<Json<AdminRestoreResponse>, ApiError> {
    if !body.dry_run {
        return Err(invalid_admin_request(
            "online restore apply is not supported; rerun with dry_run=true to validate the backup manifest",
        ));
    }
    let manifest_path = PathBuf::from(&body.manifest_path);
    let raw = fs::read_to_string(&manifest_path).map_err(internal_error)?;
    let manifest: serde_json::Value = serde_json::from_str(&raw).map_err(internal_error)?;
    if manifest["schema_version"].as_i64() != Some(1) {
        return Err(invalid_admin_request(
            "unsupported backup manifest schema_version; expected 1",
        ));
    }
    let mut checks = vec!["schema_version=1".to_string()];
    let backup_id = manifest["backup_id"].as_str().map(str::to_string);
    if backup_id.is_some() {
        checks.push("backup_id present".to_string());
    }
    if let Some(meta_path) = manifest["meta_backup_path"].as_str() {
        let meta_path = FsPath::new(meta_path);
        if !meta_path.exists() {
            return Err(invalid_admin_request(format!(
                "meta backup path is missing: {}",
                meta_path.display()
            )));
        }
        checks.push("meta backup exists".to_string());
    }
    if manifest.get("runtime_summary").is_some() {
        checks.push("runtime summary present".to_string());
    }
    if let Some(checkpoints) = manifest["tablet_store_checkpoints"].as_array() {
        for checkpoint in checkpoints {
            let Some(path) = checkpoint["path"].as_str() else {
                return Err(invalid_admin_request(
                    "tablet_store_checkpoints entry is missing path",
                ));
            };
            if !FsPath::new(path).exists() {
                return Err(invalid_admin_request(format!(
                    "tablet checkpoint path is missing: {path}"
                )));
            }
            validate_tablet_checkpoint_manifest(checkpoint, FsPath::new(path))?;
        }
        checks.push(format!(
            "tablet store checkpoints present ({})",
            checkpoints.len()
        ));
    }
    Ok(Json(AdminRestoreResponse {
        job_id: format!("job_restore_validate_{}", now_ms_i64()),
        status: "validated",
        dry_run: true,
        manifest_path: body.manifest_path,
        backup_id,
        checks,
    }))
}

async fn checkpoint_local_tablet_stores(
    state: &AppState,
    checkpoint_root: &FsPath,
) -> Result<Vec<TabletCheckpointBackup>, ApiError> {
    let mut backups = Vec::new();
    for tablet_id in state.runtime.registered_tablet_ids() {
        let tablet_dir = checkpoint_root.join(format!("tablet-{tablet_id:020}"));
        let mut manifest = state
            .runtime
            .checkpoint_tablet_store(tablet_id, &tablet_dir)
            .map_err(|err| {
                invalid_admin_request(format!(
                    "tablet-store checkpoint for tablet {tablet_id} failed: {}",
                    err.message
                ))
            })?;
        enrich_tablet_checkpoint_manifest(state, &mut manifest).await?;
        backups.push(TabletCheckpointBackup {
            tablet_id,
            path: tablet_dir.display().to_string(),
            manifest,
        });
    }
    Ok(backups)
}

async fn enrich_tablet_checkpoint_manifest(
    state: &AppState,
    manifest: &mut agentdb_storage::TabletStoreManifest,
) -> Result<(), ApiError> {
    let Some(meta) = state.meta.as_ref() else {
        return Ok(());
    };
    let Some(tablet) = tablets::get(meta.pool(), manifest.tablet_id)
        .await
        .map_err(internal_error)?
    else {
        return Ok(());
    };
    manifest.tablet_epoch = Some(tablet.epoch);
    manifest.placement_scope_id = Some(tablet.placement_scope_id.clone());
    if let Some(scope) = placement_scopes::get(meta.pool(), &tablet.placement_scope_id)
        .await
        .map_err(internal_error)?
    {
        manifest.cell_id = Some(scope.cell_id);
    }
    if let Some(snapshot) =
        tablet_snapshots::list_recent(meta.pool(), Some(manifest.tablet_id), false, 1)
            .await
            .map_err(internal_error)?
            .into_iter()
            .next()
    {
        manifest.latest_snapshot_index = u64::try_from(snapshot.snapshot_index).ok();
    }
    Ok(())
}

fn validate_tablet_checkpoint_manifest(
    checkpoint: &serde_json::Value,
    path: &FsPath,
) -> Result<(), ApiError> {
    let summary = agentdb_storage::summarize_checkpoint_dir(path).map_err(internal_error)?;
    if let Some(expected) = checkpoint["manifest"]["checksum_sha256"].as_str() {
        if summary.checksum_sha256() != expected {
            return Err(invalid_admin_request(format!(
                "tablet checkpoint checksum mismatch for {}",
                path.display()
            )));
        }
    }
    if let Some(expected) = checkpoint["manifest"]["size_bytes"].as_u64() {
        if summary.size_bytes() != expected {
            return Err(invalid_admin_request(format!(
                "tablet checkpoint size mismatch for {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn default_true() -> bool {
    true
}

fn is_safe_backup_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

async fn get_admin_nodes(
    State(s): State<AppState>,
    Query(query): Query<ClusterAdminQuery>,
) -> Result<Json<Vec<AdminNodeView>>, ApiError> {
    let meta = s.meta.as_ref().ok_or_else(|| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::KeyUnavailable,
            "metadata-backed cluster admin is not configured on this node",
        ))
    })?;
    let stale_threshold_ms = query.stale_threshold_ms.unwrap_or(15_000);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let rows = nodes::list_all(meta.pool()).await.map_err(|err| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::Internal,
            err.to_string(),
        ))
    })?;
    Ok(Json(
        rows.into_iter()
            .map(|node| AdminNodeView {
                is_stale: now_ms - node.last_heartbeat_ms > stale_threshold_ms,
                node_id: node.node_id,
                address: node.address,
                mongo_address: node.mongo_address,
                state: node.state,
                cell_id: node.cell_id,
                last_heartbeat_ms: node.last_heartbeat_ms,
                joined_at_ms: node.joined_at_ms,
                availability_zone: node.availability_zone,
                region: node.region,
                capacity_class: node.capacity_class,
            })
            .collect(),
    ))
}

async fn get_admin_cells(State(s): State<AppState>) -> Result<Json<Vec<cells::Cell>>, ApiError> {
    let meta = meta_required(&s)?;
    let rows = cells::list_all(meta.pool()).await.map_err(internal_error)?;
    Ok(Json(rows))
}

async fn upsert_admin_cell(
    State(s): State<AppState>,
    Json(body): Json<UpsertCellBody>,
) -> Result<Json<cells::Cell>, ApiError> {
    let meta = meta_required(&s)?;
    cells::upsert(
        meta.pool(),
        &body.cell_id,
        body.region.as_deref(),
        body.domain_class
            .as_deref()
            .unwrap_or(cells::DEFAULT_DOMAIN_CLASS),
        body.deployment_profile
            .as_deref()
            .unwrap_or(cells::DEFAULT_DEPLOYMENT_PROFILE),
        body.capacity_class
            .as_deref()
            .unwrap_or(cells::DEFAULT_CAPACITY_CLASS),
        body.metadata_isolation_level
            .as_deref()
            .unwrap_or(cells::DEFAULT_METADATA_ISOLATION_LEVEL),
        body.state.as_deref().unwrap_or("serving"),
        body.metadata_namespace.as_deref(),
        body.object_store_namespace.as_deref(),
        body.rollout_version,
        now_ms_i64(),
    )
    .await
    .map_err(internal_error)?;
    let cell = cells::get(meta.pool(), &body.cell_id)
        .await
        .map_err(internal_error)?
        .expect("cell upserted above");
    Ok(Json(cell))
}

async fn register_admin_node(
    State(s): State<AppState>,
    Json(body): Json<RegisterNodeBody>,
) -> Result<Json<AdminNodeView>, ApiError> {
    let meta = s.meta.as_ref().ok_or_else(|| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::KeyUnavailable,
            "metadata-backed cluster admin is not configured on this node",
        ))
    })?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let state = body.state.unwrap_or_else(|| "alive".into());
    nodes::upsert_with_cell(
        meta.pool(),
        &body.node_id,
        &body.address,
        body.mongo_address.as_deref(),
        &state,
        now_ms,
        body.availability_zone.as_deref(),
        body.region.as_deref(),
        body.cell_id
            .as_deref()
            .unwrap_or(agentdb_meta::cells::DEFAULT_CELL_ID),
        body.capacity_class
            .as_deref()
            .unwrap_or(agentdb_meta::cells::DEFAULT_CAPACITY_CLASS),
    )
    .await
    .map_err(|err| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::Internal,
            err.to_string(),
        ))
    })?;
    let node = nodes::get(meta.pool(), &body.node_id)
        .await
        .map_err(|err| {
            ApiError(agentdb_core::Error::new(
                ErrorCode::Internal,
                err.to_string(),
            ))
        })?
        .ok_or_else(|| {
            ApiError(agentdb_core::Error::new(
                ErrorCode::Internal,
                format!(
                    "registered node {} disappeared from metadata store",
                    body.node_id
                ),
            ))
        })?;
    Ok(Json(admin_node_view(node, now_ms, 15_000)))
}

async fn delete_admin_node(
    State(s): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<DeleteAdminNodeResponse>, ApiError> {
    let meta = s.meta.as_ref().ok_or_else(|| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::KeyUnavailable,
            "metadata-backed cluster admin is not configured on this node",
        ))
    })?;
    let removed = nodes::remove(meta.pool(), &node_id).await.map_err(|err| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::Internal,
            err.to_string(),
        ))
    })?;
    Ok(Json(DeleteAdminNodeResponse { node_id, removed }))
}

async fn set_admin_node_maintenance(
    State(s): State<AppState>,
    Path(node_id): Path<String>,
    Json(body): Json<NodeMaintenanceBody>,
) -> Result<Json<AdminNodeView>, ApiError> {
    let meta = meta_required(&s)?;
    let state = body.state.as_str();
    if !matches!(state, "alive" | "draining" | "maintenance") {
        return Err(invalid_admin_request(
            "maintenance state must be one of: alive, draining, maintenance",
        ));
    }
    let now_ms = now_ms_i64();
    nodes::set_state(meta.pool(), &node_id, state, now_ms)
        .await
        .map_err(internal_error)?;
    if node_id == s.drain.node_id() {
        if state == "alive" {
            s.drain.deactivate();
        } else {
            s.drain.activate(now_ms);
        }
    }
    let node = nodes::get(meta.pool(), &node_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| invalid_admin_request(format!("node {node_id} does not exist")))?;
    Ok(Json(admin_node_view(node, now_ms, 15_000)))
}

fn meta_required(s: &AppState) -> Result<&agentdb_meta::MetaStore, ApiError> {
    s.meta.as_ref().ok_or_else(|| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::KeyUnavailable,
            "metadata-backed cluster admin is not configured on this node",
        ))
    })
}

fn internal_error(err: impl ToString) -> ApiError {
    ApiError(agentdb_core::Error::new(
        ErrorCode::Internal,
        err.to_string(),
    ))
}

fn invalid_admin_request(message: impl Into<String>) -> ApiError {
    ApiError(agentdb_core::Error::new(
        ErrorCode::PolicyDenied,
        message.into(),
    ))
}

fn admin_node_view(node: nodes::Node, now_ms: i64, stale_threshold_ms: i64) -> AdminNodeView {
    AdminNodeView {
        is_stale: now_ms - node.last_heartbeat_ms > stale_threshold_ms,
        node_id: node.node_id,
        address: node.address,
        mongo_address: node.mongo_address,
        state: node.state,
        cell_id: node.cell_id,
        last_heartbeat_ms: node.last_heartbeat_ms,
        joined_at_ms: node.joined_at_ms,
        availability_zone: node.availability_zone,
        region: node.region,
        capacity_class: node.capacity_class,
    }
}

fn tablet_view(tablet: tablets::Tablet) -> AdminTabletView {
    AdminTabletView {
        tablet_id: tablet.tablet_id,
        owner_node_id: tablet.owner_node_id,
        epoch: tablet.epoch,
        lease_expires_at_ms: tablet.lease_expires_at_ms,
        created_at_ms: tablet.created_at_ms,
        placement_scope_id: tablet.placement_scope_id,
        state: tablet.state,
        raft_group_id: tablet.raft_group_id,
        encryption_policy_version: tablet.encryption_policy_version,
        handoff_target_node_id: tablet.handoff_target_node_id,
    }
}

fn tablet_replica_view(replica: tablet_replicas::TabletReplica) -> AdminTabletReplicaView {
    AdminTabletReplicaView {
        tablet_id: replica.tablet_id,
        node_id: replica.node_id,
        role: replica.role,
        health: replica.health,
        applied_index: replica.applied_index,
        match_index: replica.match_index,
        updated_at_ms: replica.updated_at_ms,
    }
}

async fn get_tablet_or_404(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
) -> Result<tablets::Tablet, ApiError> {
    tablets::get(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| invalid_admin_request(format!("tablet {tablet_id} does not exist")))
}

async fn require_assignable_node(
    meta: &agentdb_meta::MetaStore,
    node_id: &str,
    now_ms: i64,
    stale_threshold_ms: i64,
) -> Result<nodes::Node, ApiError> {
    let node = nodes::get(meta.pool(), node_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| invalid_admin_request(format!("node {node_id} does not exist")))?;
    if node.state != "alive" {
        return Err(invalid_admin_request(format!(
            "node {node_id} is {}; target node must be alive",
            node.state
        )));
    }
    if now_ms - node.last_heartbeat_ms > stale_threshold_ms {
        return Err(invalid_admin_request(format!(
            "node {node_id} is stale; target node must be fresh"
        )));
    }
    Ok(node)
}

async fn get_tablet_replica(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    node_id: &str,
) -> Result<Option<tablet_replicas::TabletReplica>, ApiError> {
    Ok(tablet_replicas::list_by_tablet(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .find(|replica| replica.node_id == node_id))
}

async fn scoped_ranges_for_tablet(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    placement_scope_id: &str,
) -> Result<Vec<tablet_ranges::TabletRange>, ApiError> {
    Ok(tablet_ranges::list_by_tablet(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|range| range.placement_scope_id == placement_scope_id)
        .collect())
}

fn range_bounds(ranges: &[tablet_ranges::TabletRange]) -> Vec<tablet_ranges::RangeBounds> {
    ranges
        .iter()
        .map(|range| tablet_ranges::RangeBounds {
            start_shard: range.start_shard,
            end_shard: range.end_shard,
        })
        .collect()
}

fn union_replacement_range(
    ranges: &[tablet_ranges::TabletRange],
    tablet_id: i64,
) -> Result<tablet_ranges::ReplacementRange, ApiError> {
    let start_shard = ranges
        .iter()
        .map(|range| range.start_shard)
        .min()
        .ok_or_else(|| {
            invalid_admin_request("range lifecycle requires at least one source range")
        })?;
    let end_shard = ranges
        .iter()
        .map(|range| range.end_shard)
        .max()
        .expect("source range start existed above");
    Ok(tablet_ranges::ReplacementRange {
        start_shard,
        end_shard,
        tablet_id,
    })
}

async fn create_lifecycle_tablet_if_missing(
    meta: &agentdb_meta::MetaStore,
    source: &tablets::Tablet,
    tablet_id: i64,
    state: &str,
    now_ms: i64,
) -> Result<bool, ApiError> {
    if tablets::get(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?
        .is_some()
    {
        return Ok(false);
    }
    let raft_group_id = format!("raft/{tablet_id}");
    tablets::create_with_placement(
        meta.pool(),
        tablet_id,
        &source.placement_scope_id,
        state,
        Some(&raft_group_id),
        source.encryption_policy_version,
        now_ms,
    )
    .await
    .map_err(internal_error)?;
    Ok(true)
}

fn ensure_prepare_source_serving(tablet: &tablets::Tablet, action: &str) -> Result<(), ApiError> {
    if tablet.state != "serving" {
        return Err(invalid_admin_request(format!(
            "{action} prepare requires source tablet {} to be `serving`, got `{}`",
            tablet.tablet_id, tablet.state
        )));
    }
    Ok(())
}

async fn ensure_prepare_destination_available(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    placement_scope_id: &str,
    action: &str,
) -> Result<(), ApiError> {
    if let Some(tablet) = tablets::get(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?
    {
        if tablet.state != "creating" {
            return Err(invalid_admin_request(format!(
                "{action} prepare destination tablet {tablet_id} must be missing or `creating`, got `{}`",
                tablet.state
            )));
        }
    }
    let existing_ranges = scoped_ranges_for_tablet(meta, tablet_id, placement_scope_id).await?;
    if !existing_ranges.is_empty() {
        return Err(invalid_admin_request(format!(
            "{action} prepare destination tablet {tablet_id} already owns ranges in placement scope {placement_scope_id}"
        )));
    }
    Ok(())
}

async fn source_applied_index(
    meta: &agentdb_meta::MetaStore,
    source_tablet_ids: &[i64],
) -> Result<i64, ApiError> {
    let mut max_index = 0;
    for tablet_id in source_tablet_ids {
        for replica in tablet_replicas::list_by_tablet(meta.pool(), *tablet_id)
            .await
            .map_err(internal_error)?
        {
            if replica.role != "log_only" {
                max_index = max_index.max(replica.applied_index);
            }
        }
    }
    Ok(max_index)
}

async fn seed_destination_replicas_from_sources(
    meta: &agentdb_meta::MetaStore,
    source_tablet_ids: &[i64],
    destination_tablet_ids: &[i64],
    now_ms: i64,
) -> Result<bool, ApiError> {
    let mut by_node = BTreeMap::<String, (String, i64, i64)>::new();
    for source_id in source_tablet_ids {
        for replica in tablet_replicas::list_by_tablet(meta.pool(), *source_id)
            .await
            .map_err(internal_error)?
        {
            let priority = replica_role_priority(&replica.role);
            by_node
                .entry(replica.node_id)
                .and_modify(|existing| {
                    if priority > existing.1 {
                        *existing = (replica.role.clone(), priority, replica.applied_index);
                    } else {
                        existing.2 = existing.2.max(replica.applied_index);
                    }
                })
                .or_insert((replica.role, priority, replica.applied_index));
        }
    }
    let mut changed = false;
    for destination_id in destination_tablet_ids {
        let existing = tablet_replicas::list_by_tablet(meta.pool(), *destination_id)
            .await
            .map_err(internal_error)?;
        for (node_id, (role, _, _)) in &by_node {
            if existing.iter().any(|replica| replica.node_id == *node_id) {
                continue;
            }
            tablet_replicas::upsert(
                meta.pool(),
                *destination_id,
                node_id,
                role,
                "lagging",
                0,
                0,
                now_ms,
            )
            .await
            .map_err(internal_error)?;
            changed = true;
        }
    }
    Ok(changed)
}

fn replica_role_priority(role: &str) -> i64 {
    match role {
        "voter" => 3,
        "learner" => 2,
        "log_only" => 1,
        _ => 0,
    }
}

async fn ensure_destinations_ready(
    meta: &agentdb_meta::MetaStore,
    destination_tablet_ids: &[i64],
    required_applied_index: i64,
    require_ready: bool,
) -> Result<bool, ApiError> {
    let mut all_ready = true;
    for destination_id in destination_tablet_ids {
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), *destination_id)
            .await
            .map_err(internal_error)?;
        let serving_replicas: Vec<_> = replicas
            .iter()
            .filter(|replica| replica.role != "log_only")
            .collect();
        if serving_replicas.is_empty() {
            all_ready = false;
        }
        for replica in serving_replicas {
            if replica.health != "healthy" || replica.applied_index < required_applied_index {
                all_ready = false;
            }
        }
    }
    if require_ready && !all_ready {
        return Err(invalid_admin_request(format!(
            "destination tablets are not ready for cutover at applied_index {required_applied_index}"
        )));
    }
    Ok(all_ready)
}

async fn lifecycle_replicas(
    meta: &agentdb_meta::MetaStore,
    tablet_ids: &[i64],
) -> Result<Vec<AdminTabletReplicaView>, ApiError> {
    let mut replicas = Vec::new();
    for tablet_id in tablet_ids {
        replicas.extend(
            tablet_replicas::list_by_tablet(meta.pool(), *tablet_id)
                .await
                .map_err(internal_error)?
                .into_iter()
                .map(tablet_replica_view),
        );
    }
    Ok(replicas)
}

fn split_movement_id(source_tablet_id: i64, destination_tablet_id: i64) -> String {
    format!("split:{source_tablet_id}:{destination_tablet_id}")
}

fn merge_movement_id(source_tablet_id: i64, destination_tablet_id: i64) -> String {
    format!("merge:{source_tablet_id}:{destination_tablet_id}")
}

async fn movement_rows(
    meta: &agentdb_meta::MetaStore,
    movement_ids: &[String],
) -> Result<Vec<tablet_movements::TabletMovement>, ApiError> {
    let mut rows = Vec::new();
    for movement_id in movement_ids {
        if let Some(row) = tablet_movements::get(meta.pool(), movement_id)
            .await
            .map_err(internal_error)?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

async fn get_admin_tablets(
    State(s): State<AppState>,
) -> Result<Json<Vec<AdminTabletView>>, ApiError> {
    let meta = meta_required(&s)?;
    let rows = tablets::list_all(meta.pool()).await.map_err(|err| {
        ApiError(agentdb_core::Error::new(
            ErrorCode::Internal,
            err.to_string(),
        ))
    })?;
    Ok(Json(rows.into_iter().map(tablet_view).collect()))
}

async fn create_admin_tablet(
    State(s): State<AppState>,
    Json(body): Json<CreateTabletBody>,
) -> Result<Json<AdminTabletMutationResponse>, ApiError> {
    let meta = meta_required(&s)?;
    if body.tablet_id < 0 {
        return Err(invalid_admin_request("tablet_id must be non-negative"));
    }
    let existed = tablets::get(meta.pool(), body.tablet_id)
        .await
        .map_err(internal_error)?
        .is_some();
    let now_ms = now_ms_i64();
    let tablet = if existed {
        tablets::get(meta.pool(), body.tablet_id)
            .await
            .map_err(internal_error)?
            .expect("tablet existed above")
    } else if body.placement_scope_id.is_some()
        || body.state.is_some()
        || body.raft_group_id.is_some()
        || body.encryption_policy_version.is_some()
    {
        tablets::create_with_placement(
            meta.pool(),
            body.tablet_id,
            body.placement_scope_id.as_deref().unwrap_or("default"),
            body.state.as_deref().unwrap_or("serving"),
            body.raft_group_id.as_deref(),
            body.encryption_policy_version.unwrap_or(0),
            now_ms,
        )
        .await
        .map_err(internal_error)?
    } else {
        tablets::get_or_create(meta.pool(), body.tablet_id, now_ms)
            .await
            .map_err(internal_error)?
    };
    Ok(Json(AdminTabletMutationResponse {
        action: "create",
        changed: !existed,
        tablet: tablet_view(tablet),
    }))
}

async fn get_admin_tablet_ranges(
    State(s): State<AppState>,
) -> Result<Json<Vec<tablet_ranges::TabletRange>>, ApiError> {
    let meta = meta_required(&s)?;
    let rows = tablet_ranges::list_all(meta.pool())
        .await
        .map_err(internal_error)?;
    Ok(Json(rows))
}

async fn get_admin_tablet_movements(
    State(s): State<AppState>,
    Query(query): Query<TabletMovementsQuery>,
) -> Result<Json<Vec<tablet_movements::TabletMovement>>, ApiError> {
    let meta = meta_required(&s)?;
    let rows = tablet_movements::list_recent(
        meta.pool(),
        query.status.as_deref(),
        query.limit.unwrap_or(100),
    )
    .await
    .map_err(|err| invalid_admin_request(err.to_string()))?;
    Ok(Json(rows))
}

async fn get_admin_tablet_stats(
    State(s): State<AppState>,
    Query(query): Query<TabletStatsQuery>,
) -> Result<Json<AdminTabletStatsResponse>, ApiError> {
    let meta = meta_required(&s)?;
    if let Some(tablet_id) = query.tablet_id {
        if tablet_id < 0 {
            return Err(invalid_admin_request("tablet_id must be non-negative"));
        }
    }
    let stats = if let Some(tablet_id) = query.tablet_id {
        tablet_stats::list_by_tablet(meta.pool(), tablet_id)
            .await
            .map_err(internal_error)?
    } else {
        tablet_stats::list_all(meta.pool())
            .await
            .map_err(internal_error)?
    };
    let aggregates = tablet_stats::list_aggregates(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|aggregate| {
            query
                .tablet_id
                .map_or(true, |tablet_id| aggregate.tablet_id == tablet_id)
        })
        .collect();
    Ok(Json(AdminTabletStatsResponse { stats, aggregates }))
}

async fn replace_admin_tablet_ranges(
    State(s): State<AppState>,
    Json(body): Json<ReplaceTabletRangesBody>,
) -> Result<Json<ReplaceTabletRangesResponse>, ApiError> {
    let meta = meta_required(&s)?;
    tablet_ranges::replace_ranges(
        meta.pool(),
        &body.placement_scope_id,
        &body.old_ranges,
        &body.new_ranges,
        body.map_epoch,
        now_ms_i64(),
    )
    .await
    .map_err(internal_error)?;
    let ranges = tablet_ranges::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|range| range.placement_scope_id == body.placement_scope_id)
        .collect();
    Ok(Json(ReplaceTabletRangesResponse {
        placement_scope_id: body.placement_scope_id,
        changed: true,
        ranges,
    }))
}

async fn prepare_admin_tablet_split(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<SplitTabletBody>,
) -> Result<Json<AdminTabletRangeLifecycleResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let source = get_tablet_or_404(meta, tablet_id).await?;
    ensure_prepare_source_serving(&source, "split")?;
    if body.child_ranges.len() < 2 {
        return Err(invalid_admin_request(
            "split prepare requires at least two child ranges",
        ));
    }
    if body
        .child_ranges
        .iter()
        .any(|range| range.tablet_id == tablet_id)
    {
        return Err(invalid_admin_request(
            "split child tablet ids must be different from the parent",
        ));
    }
    let placement_scope_id = body
        .placement_scope_id
        .unwrap_or_else(|| source.placement_scope_id.clone());
    let old_ranges = scoped_ranges_for_tablet(meta, tablet_id, &placement_scope_id).await?;
    if old_ranges.is_empty() {
        return Err(invalid_admin_request(format!(
            "tablet {tablet_id} has no ranges in placement scope {placement_scope_id}"
        )));
    }
    let old_bounds = range_bounds(&old_ranges);
    tablet_ranges::validate_replacement_plan(&old_bounds, &body.child_ranges)
        .map_err(|err| invalid_admin_request(err.to_string()))?;
    let now_ms = now_ms_i64();
    let mut changed = tablets::set_state(meta.pool(), tablet_id, "splitting")
        .await
        .map_err(internal_error)?;
    let destination_tablet_ids: Vec<_> = body
        .child_ranges
        .iter()
        .map(|range| range.tablet_id)
        .collect();
    for child in &body.child_ranges {
        ensure_prepare_destination_available(meta, child.tablet_id, &placement_scope_id, "split")
            .await?;
        changed |=
            create_lifecycle_tablet_if_missing(meta, &source, child.tablet_id, "creating", now_ms)
                .await?;
    }
    changed |=
        seed_destination_replicas_from_sources(meta, &[tablet_id], &destination_tablet_ids, now_ms)
            .await?;
    let required_applied_index = source_applied_index(meta, &[tablet_id]).await?;
    let destinations_ready =
        ensure_destinations_ready(meta, &destination_tablet_ids, required_applied_index, false)
            .await?;
    let mut movement_ids = Vec::new();
    for child in &body.child_ranges {
        let movement_id = split_movement_id(tablet_id, child.tablet_id);
        tablet_movements::upsert_pending(
            meta.pool(),
            &movement_id,
            "split",
            tablet_id,
            child.tablet_id,
            &placement_scope_id,
            child.start_shard,
            child.end_shard,
            required_applied_index,
            now_ms,
        )
        .await
        .map_err(internal_error)?;
        movement_ids.push(movement_id);
    }
    let tablets = tablets::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|tablet| {
            tablet.tablet_id == tablet_id
                || body
                    .child_ranges
                    .iter()
                    .any(|child| child.tablet_id == tablet.tablet_id)
        })
        .map(tablet_view)
        .collect();
    let ranges = tablet_ranges::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|range| range.placement_scope_id == placement_scope_id)
        .collect();
    Ok(Json(AdminTabletRangeLifecycleResponse {
        action: "prepare_split",
        changed,
        placement_scope_id,
        source_tablet_ids: vec![tablet_id],
        destination_tablet_ids: destination_tablet_ids.clone(),
        required_applied_index,
        destinations_ready,
        old_ranges: old_bounds,
        new_ranges: body.child_ranges,
        tablets,
        replicas: lifecycle_replicas(meta, &destination_tablet_ids).await?,
        movements: movement_rows(meta, &movement_ids).await?,
        ranges,
    }))
}

async fn complete_admin_tablet_split(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<SplitTabletBody>,
) -> Result<Json<AdminTabletRangeLifecycleResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let source = get_tablet_or_404(meta, tablet_id).await?;
    if body.child_ranges.len() < 2 {
        return Err(invalid_admin_request(
            "split completion requires at least two child ranges",
        ));
    }
    if source.state != "splitting" {
        return Err(invalid_admin_request(
            "split completion requires parent tablet state `splitting`",
        ));
    }
    let map_epoch = body
        .map_epoch
        .ok_or_else(|| invalid_admin_request("split completion requires map_epoch"))?;
    let destination_tablet_ids: Vec<_> = body
        .child_ranges
        .iter()
        .map(|range| range.tablet_id)
        .collect();
    let placement_scope_id = body
        .placement_scope_id
        .unwrap_or_else(|| source.placement_scope_id.clone());
    let old_ranges = scoped_ranges_for_tablet(meta, tablet_id, &placement_scope_id).await?;
    if old_ranges.is_empty() {
        return Err(invalid_admin_request(format!(
            "tablet {tablet_id} has no ranges in placement scope {placement_scope_id}"
        )));
    }
    for child in &body.child_ranges {
        let child_tablet = get_tablet_or_404(meta, child.tablet_id).await?;
        if child_tablet.state != "creating" && child_tablet.state != "serving" {
            return Err(invalid_admin_request(
                "split child tablets must be in `creating` or `serving` state",
            ));
        }
    }
    let old_bounds = range_bounds(&old_ranges);
    let required_applied_index = body
        .min_applied_index
        .unwrap_or(source_applied_index(meta, &[tablet_id]).await?);
    if required_applied_index < 0 {
        return Err(invalid_admin_request(
            "min_applied_index must be non-negative",
        ));
    }
    let destinations_ready = ensure_destinations_ready(
        meta,
        &destination_tablet_ids,
        required_applied_index,
        body.require_ready.unwrap_or(true),
    )
    .await?;
    let now_ms = now_ms_i64();
    tablet_ranges::replace_ranges(
        meta.pool(),
        &placement_scope_id,
        &old_bounds,
        &body.child_ranges,
        map_epoch,
        now_ms,
    )
    .await
    .map_err(internal_error)?;
    let mut movement_ids = Vec::new();
    for child in &body.child_ranges {
        let movement_id = split_movement_id(tablet_id, child.tablet_id);
        let _ = tablet_movements::mark_completed(meta.pool(), &movement_id, now_ms)
            .await
            .map_err(internal_error)?;
        movement_ids.push(movement_id);
    }
    for child in &body.child_ranges {
        tablets::set_state(meta.pool(), child.tablet_id, "serving")
            .await
            .map_err(internal_error)?;
    }
    tablets::set_state(meta.pool(), tablet_id, "tombstoned")
        .await
        .map_err(internal_error)?;
    let tablets = tablets::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|tablet| {
            tablet.tablet_id == tablet_id
                || body
                    .child_ranges
                    .iter()
                    .any(|child| child.tablet_id == tablet.tablet_id)
        })
        .map(tablet_view)
        .collect();
    let ranges = tablet_ranges::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|range| range.placement_scope_id == placement_scope_id)
        .collect();
    Ok(Json(AdminTabletRangeLifecycleResponse {
        action: "complete_split",
        changed: true,
        placement_scope_id,
        source_tablet_ids: vec![tablet_id],
        destination_tablet_ids: destination_tablet_ids.clone(),
        required_applied_index,
        destinations_ready,
        old_ranges: old_bounds,
        new_ranges: body.child_ranges,
        tablets,
        replicas: lifecycle_replicas(meta, &destination_tablet_ids).await?,
        movements: movement_rows(meta, &movement_ids).await?,
        ranges,
    }))
}

async fn prepare_admin_tablet_merge(
    State(s): State<AppState>,
    Json(body): Json<MergeTabletsBody>,
) -> Result<Json<AdminTabletRangeLifecycleResponse>, ApiError> {
    let meta = meta_required(&s)?;
    if body.source_tablet_ids.len() < 2 {
        return Err(invalid_admin_request(
            "merge prepare requires at least two source tablets",
        ));
    }
    if body.source_tablet_ids.contains(&body.destination_tablet_id) {
        return Err(invalid_admin_request(
            "merge destination tablet id must be different from sources",
        ));
    }
    let first_source = get_tablet_or_404(meta, body.source_tablet_ids[0]).await?;
    let placement_scope_id = body
        .placement_scope_id
        .unwrap_or_else(|| first_source.placement_scope_id.clone());
    let mut old_ranges = Vec::new();
    for source_id in &body.source_tablet_ids {
        let source = get_tablet_or_404(meta, *source_id).await?;
        ensure_prepare_source_serving(&source, "merge")?;
        if source.placement_scope_id != first_source.placement_scope_id {
            return Err(invalid_admin_request(
                "merge sources must have the same placement scope",
            ));
        }
        old_ranges.extend(scoped_ranges_for_tablet(meta, *source_id, &placement_scope_id).await?);
    }
    if old_ranges.is_empty() {
        return Err(invalid_admin_request(
            "merge sources have no ranges in the requested placement scope",
        ));
    }
    let old_bounds = range_bounds(&old_ranges);
    let new_range = union_replacement_range(&old_ranges, body.destination_tablet_id)?;
    tablet_ranges::validate_replacement_plan(&old_bounds, &[new_range])
        .map_err(|err| invalid_admin_request(err.to_string()))?;
    ensure_prepare_destination_available(
        meta,
        body.destination_tablet_id,
        &placement_scope_id,
        "merge",
    )
    .await?;
    let now_ms = now_ms_i64();
    let mut changed = create_lifecycle_tablet_if_missing(
        meta,
        &first_source,
        body.destination_tablet_id,
        "creating",
        now_ms,
    )
    .await?;
    for source_id in &body.source_tablet_ids {
        changed |= tablets::set_state(meta.pool(), *source_id, "merging")
            .await
            .map_err(internal_error)?;
    }
    changed |= seed_destination_replicas_from_sources(
        meta,
        &body.source_tablet_ids,
        &[body.destination_tablet_id],
        now_ms,
    )
    .await?;
    let required_applied_index = source_applied_index(meta, &body.source_tablet_ids).await?;
    let destinations_ready = ensure_destinations_ready(
        meta,
        &[body.destination_tablet_id],
        required_applied_index,
        false,
    )
    .await?;
    let mut movement_ids = Vec::new();
    for source_id in &body.source_tablet_ids {
        let source_ranges = scoped_ranges_for_tablet(meta, *source_id, &placement_scope_id).await?;
        let source_range = union_replacement_range(&source_ranges, body.destination_tablet_id)?;
        let movement_id = merge_movement_id(*source_id, body.destination_tablet_id);
        tablet_movements::upsert_pending(
            meta.pool(),
            &movement_id,
            "merge",
            *source_id,
            body.destination_tablet_id,
            &placement_scope_id,
            source_range.start_shard,
            source_range.end_shard,
            required_applied_index,
            now_ms,
        )
        .await
        .map_err(internal_error)?;
        movement_ids.push(movement_id);
    }
    let tablets = tablets::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|tablet| {
            body.source_tablet_ids.contains(&tablet.tablet_id)
                || tablet.tablet_id == body.destination_tablet_id
        })
        .map(tablet_view)
        .collect();
    let ranges = tablet_ranges::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|range| range.placement_scope_id == placement_scope_id)
        .collect();
    Ok(Json(AdminTabletRangeLifecycleResponse {
        action: "prepare_merge",
        changed,
        placement_scope_id,
        source_tablet_ids: body.source_tablet_ids,
        destination_tablet_ids: vec![body.destination_tablet_id],
        required_applied_index,
        destinations_ready,
        old_ranges: old_bounds,
        new_ranges: vec![new_range],
        tablets,
        replicas: lifecycle_replicas(meta, &[body.destination_tablet_id]).await?,
        movements: movement_rows(meta, &movement_ids).await?,
        ranges,
    }))
}

async fn complete_admin_tablet_merge(
    State(s): State<AppState>,
    Json(body): Json<MergeTabletsBody>,
) -> Result<Json<AdminTabletRangeLifecycleResponse>, ApiError> {
    let meta = meta_required(&s)?;
    if body.source_tablet_ids.len() < 2 {
        return Err(invalid_admin_request(
            "merge completion requires at least two source tablets",
        ));
    }
    if body.source_tablet_ids.contains(&body.destination_tablet_id) {
        return Err(invalid_admin_request(
            "merge destination tablet id must be different from sources",
        ));
    }
    let map_epoch = body
        .map_epoch
        .ok_or_else(|| invalid_admin_request("merge completion requires map_epoch"))?;
    let first_source = get_tablet_or_404(meta, body.source_tablet_ids[0]).await?;
    let destination = get_tablet_or_404(meta, body.destination_tablet_id).await?;
    if destination.state != "creating" && destination.state != "serving" {
        return Err(invalid_admin_request(
            "merge destination tablet must be in `creating` or `serving` state",
        ));
    }
    let placement_scope_id = body
        .placement_scope_id
        .unwrap_or_else(|| first_source.placement_scope_id.clone());
    let mut old_ranges = Vec::new();
    for source_id in &body.source_tablet_ids {
        let source = get_tablet_or_404(meta, *source_id).await?;
        if source.state != "merging" {
            return Err(invalid_admin_request(
                "merge completion requires every source tablet state `merging`",
            ));
        }
        old_ranges.extend(scoped_ranges_for_tablet(meta, *source_id, &placement_scope_id).await?);
    }
    if old_ranges.is_empty() {
        return Err(invalid_admin_request(
            "merge sources have no ranges in the requested placement scope",
        ));
    }
    let old_bounds = range_bounds(&old_ranges);
    let new_range = union_replacement_range(&old_ranges, body.destination_tablet_id)?;
    tablet_ranges::validate_replacement_plan(&old_bounds, &[new_range])
        .map_err(|err| invalid_admin_request(err.to_string()))?;
    let required_applied_index = body
        .min_applied_index
        .unwrap_or(source_applied_index(meta, &body.source_tablet_ids).await?);
    if required_applied_index < 0 {
        return Err(invalid_admin_request(
            "min_applied_index must be non-negative",
        ));
    }
    let destinations_ready = ensure_destinations_ready(
        meta,
        &[body.destination_tablet_id],
        required_applied_index,
        body.require_ready.unwrap_or(true),
    )
    .await?;
    let now_ms = now_ms_i64();
    tablet_ranges::replace_ranges(
        meta.pool(),
        &placement_scope_id,
        &old_bounds,
        &[new_range],
        map_epoch,
        now_ms,
    )
    .await
    .map_err(internal_error)?;
    let mut movement_ids = Vec::new();
    for source_id in &body.source_tablet_ids {
        let movement_id = merge_movement_id(*source_id, body.destination_tablet_id);
        let _ = tablet_movements::mark_completed(meta.pool(), &movement_id, now_ms)
            .await
            .map_err(internal_error)?;
        movement_ids.push(movement_id);
    }
    tablets::set_state(meta.pool(), body.destination_tablet_id, "serving")
        .await
        .map_err(internal_error)?;
    for source_id in &body.source_tablet_ids {
        tablets::set_state(meta.pool(), *source_id, "tombstoned")
            .await
            .map_err(internal_error)?;
    }
    let tablets = tablets::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|tablet| {
            body.source_tablet_ids.contains(&tablet.tablet_id)
                || tablet.tablet_id == body.destination_tablet_id
        })
        .map(tablet_view)
        .collect();
    let ranges = tablet_ranges::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|range| range.placement_scope_id == placement_scope_id)
        .collect();
    Ok(Json(AdminTabletRangeLifecycleResponse {
        action: "complete_merge",
        changed: true,
        placement_scope_id,
        source_tablet_ids: body.source_tablet_ids,
        destination_tablet_ids: vec![body.destination_tablet_id],
        required_applied_index,
        destinations_ready,
        old_ranges: old_bounds,
        new_ranges: vec![new_range],
        tablets,
        replicas: lifecycle_replicas(meta, &[body.destination_tablet_id]).await?,
        movements: movement_rows(meta, &movement_ids).await?,
        ranges,
    }))
}

async fn get_admin_tenant_policies(
    State(s): State<AppState>,
) -> Result<Json<Vec<tenant_policies::TenantPolicy>>, ApiError> {
    let meta = meta_required(&s)?;
    let rows = tenant_policies::list_all(meta.pool())
        .await
        .map_err(internal_error)?;
    Ok(Json(rows))
}

async fn upsert_admin_tenant_policy(
    State(s): State<AppState>,
    Json(body): Json<UpsertTenantPolicyBody>,
) -> Result<Json<tenant_policies::TenantPolicy>, ApiError> {
    let meta = meta_required(&s)?;
    tenant_policies::upsert_with_cell(
        meta.pool(),
        &body.tenant_id,
        &body.placement_class,
        &body.placement_scope_id,
        &body.encryption_class,
        body.key_policy_id.as_deref(),
        body.encryption_policy_version,
        body.policy_version,
        body.cell_id
            .as_deref()
            .unwrap_or(agentdb_meta::cells::DEFAULT_CELL_ID),
        body.tier
            .as_deref()
            .unwrap_or(tenant_policies::DEFAULT_TIER),
        body.priority.unwrap_or(tenant_policies::DEFAULT_PRIORITY),
        now_ms_i64(),
    )
    .await
    .map_err(internal_error)?;
    let policy = tenant_policies::get(meta.pool(), &body.tenant_id)
        .await
        .map_err(internal_error)?
        .expect("tenant policy upserted above");
    Ok(Json(policy))
}

async fn delete_admin_tenant_policy(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<DeleteTenantPolicyResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let removed = tenant_policies::delete(meta.pool(), &tenant_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(DeleteTenantPolicyResponse { tenant_id, removed }))
}

async fn assign_admin_tablet(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<AssignTabletBody>,
) -> Result<Json<AdminTabletMutationResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let _ = get_tablet_or_404(meta, tablet_id).await?;
    let _ = require_assignable_node(meta, &body.node_id, now_ms_i64(), 15_000).await?;
    let changed = tablets::assign_to(meta.pool(), tablet_id, &body.node_id)
        .await
        .map_err(internal_error)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    Ok(Json(AdminTabletMutationResponse {
        action: "assign",
        changed,
        tablet: tablet_view(tablet),
    }))
}

async fn relinquish_admin_tablet(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
) -> Result<Json<AdminTabletMutationResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let _ = get_tablet_or_404(meta, tablet_id).await?;
    let changed = tablets::relinquish(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    Ok(Json(AdminTabletMutationResponse {
        action: "relinquish",
        changed,
        tablet: tablet_view(tablet),
    }))
}

async fn mark_admin_tablet_handoff(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<HandoffTabletBody>,
) -> Result<Json<AdminTabletMutationResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    if tablet.owner_node_id.as_deref() == Some(body.target_node_id.as_str()) {
        return Err(invalid_admin_request(
            "handoff target must be different from the current owner",
        ));
    }
    let _ = require_assignable_node(meta, &body.target_node_id, now_ms_i64(), 15_000).await?;
    let changed = tablets::mark_for_handoff(meta.pool(), tablet_id, &body.target_node_id)
        .await
        .map_err(internal_error)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    Ok(Json(AdminTabletMutationResponse {
        action: "handoff",
        changed,
        tablet: tablet_view(tablet),
    }))
}

async fn complete_admin_tablet_handoff(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<CompleteHandoffBody>,
) -> Result<Json<AdminTabletHandoffCompleteResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let _ = get_tablet_or_404(meta, tablet_id).await?;
    let new_owner =
        tablets::complete_handoff(meta.pool(), tablet_id, &body.expected_source_node_id)
            .await
            .map_err(internal_error)?;
    let changed = new_owner.is_some();
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    Ok(Json(AdminTabletHandoffCompleteResponse {
        action: "complete_handoff",
        changed,
        new_owner_node_id: new_owner,
        tablet: tablet_view(tablet),
    }))
}

async fn cancel_admin_tablet_handoff(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
) -> Result<Json<AdminTabletMutationResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let _ = get_tablet_or_404(meta, tablet_id).await?;
    let changed = tablets::cancel_handoff(meta.pool(), tablet_id)
        .await
        .map_err(internal_error)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    Ok(Json(AdminTabletMutationResponse {
        action: "cancel_handoff",
        changed,
        tablet: tablet_view(tablet),
    }))
}

async fn prepare_admin_tablet_replacement(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<PrepareTabletReplacementBody>,
) -> Result<Json<AdminTabletReplacementPrepareResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    if tablet.owner_node_id.as_deref() == Some(body.target_node_id.as_str()) {
        return Err(invalid_admin_request(
            "replacement target must be different from the current owner",
        ));
    }
    let source_node_id = body.source_node_id.or(tablet.owner_node_id.clone());
    if source_node_id.is_none() {
        return Err(invalid_admin_request(
            "tablet replacement requires a current owner or explicit source_node_id",
        ));
    }
    if let Some(source_node_id) = source_node_id.as_ref() {
        if tablet.owner_node_id.as_ref() != Some(source_node_id) {
            return Err(invalid_admin_request(format!(
                "source node {source_node_id} must match the current tablet owner"
            )));
        }
    }
    let now_ms = now_ms_i64();
    let stale_threshold_ms = body.stale_threshold_ms.unwrap_or(15_000);
    let _ = require_assignable_node(meta, &body.target_node_id, now_ms, stale_threshold_ms).await?;
    let existing_target = get_tablet_replica(meta, tablet_id, &body.target_node_id).await?;
    let target_applied = existing_target
        .as_ref()
        .map(|replica| replica.applied_index)
        .unwrap_or(0);
    let target_match = existing_target
        .as_ref()
        .map(|replica| replica.match_index)
        .unwrap_or(0);
    let target_health = existing_target
        .as_ref()
        .map(|replica| replica.health.as_str())
        .unwrap_or("lagging");
    tablet_replicas::upsert(
        meta.pool(),
        tablet_id,
        &body.target_node_id,
        "learner",
        target_health,
        target_applied,
        target_match,
        now_ms,
    )
    .await
    .map_err(internal_error)?;
    let handoff_changed = tablets::mark_for_handoff(meta.pool(), tablet_id, &body.target_node_id)
        .await
        .map_err(internal_error)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    let source_replica = if let Some(source_node_id) = source_node_id.as_ref() {
        get_tablet_replica(meta, tablet_id, source_node_id).await?
    } else {
        None
    };
    let target_replica = get_tablet_replica(meta, tablet_id, &body.target_node_id)
        .await?
        .expect("target replica upserted above");
    Ok(Json(AdminTabletReplacementPrepareResponse {
        action: "prepare_replacement",
        changed: handoff_changed
            || existing_target
                .as_ref()
                .map(|replica| replica.role != "learner")
                .unwrap_or(true),
        source_node_id,
        target_node_id: body.target_node_id,
        tablet: tablet_view(tablet),
        source_replica: source_replica.map(tablet_replica_view),
        target_replica: tablet_replica_view(target_replica),
    }))
}

async fn complete_admin_tablet_replacement(
    State(s): State<AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<CompleteTabletReplacementBody>,
) -> Result<Json<AdminTabletReplacementCompleteResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    if tablet.owner_node_id.as_deref() == Some(body.target_node_id.as_str()) {
        return Err(invalid_admin_request(
            "replacement target already owns the tablet",
        ));
    }
    let source_node_id = body.source_node_id.or(tablet.owner_node_id.clone());
    if source_node_id.is_none() {
        return Err(invalid_admin_request(
            "tablet replacement requires a current owner or explicit source_node_id",
        ));
    }
    if let Some(source_node_id) = source_node_id.as_ref() {
        if tablet.owner_node_id.as_ref() != Some(source_node_id) {
            return Err(invalid_admin_request(format!(
                "source node {source_node_id} must match the current tablet owner"
            )));
        }
    }
    if tablet.handoff_target_node_id.as_deref() != Some(body.target_node_id.as_str()) {
        return Err(invalid_admin_request(
            "replacement must be prepared before it can be completed",
        ));
    }
    let target_replica = get_tablet_replica(meta, tablet_id, &body.target_node_id)
        .await?
        .ok_or_else(|| {
            invalid_admin_request(format!(
                "target replica {} does not exist for tablet {tablet_id}",
                body.target_node_id
            ))
        })?;
    if body.require_healthy.unwrap_or(true) && target_replica.health != "healthy" {
        return Err(invalid_admin_request(format!(
            "target replica {} is {}; wait for catch-up or set require_healthy=false",
            body.target_node_id, target_replica.health
        )));
    }
    let source_replica = if let Some(source_node_id) = source_node_id.as_ref() {
        get_tablet_replica(meta, tablet_id, source_node_id).await?
    } else {
        None
    };
    let required_applied_index = if let Some(min_applied_index) = body.min_applied_index {
        min_applied_index
    } else if let Some(source_replica) = source_replica.as_ref() {
        source_replica.applied_index
    } else {
        return Err(invalid_admin_request(
            "min_applied_index is required when source replica progress is unavailable",
        ));
    };
    if required_applied_index < 0 {
        return Err(invalid_admin_request(
            "min_applied_index must be non-negative",
        ));
    }
    if target_replica.applied_index < required_applied_index {
        return Err(invalid_admin_request(format!(
            "target replica {} applied_index {} is below required_applied_index {}",
            body.target_node_id, target_replica.applied_index, required_applied_index
        )));
    }

    let retire_source = body.retire_source.unwrap_or(false);
    let mut raft_membership_changed = false;
    let mut raft_membership_operations = Vec::new();
    if let Some(registry) = s.raft_registry.as_ref() {
        if let Ok(statuses) = registry.status_for(tablet_id) {
            let leader_id = statuses
                .iter()
                .find(|status| status.role == "Leader")
                .map(|status| status.node_id)
                .or_else(|| {
                    statuses
                        .iter()
                        .find(|status| status.leader_id != 0)
                        .map(|status| status.leader_id)
                })
                .ok_or_else(|| invalid_admin_request("tablet raft group has no known leader"))?;
            let target_raft_node_id = agentdb_storage::stable_raft_node_id(&body.target_node_id)
                .map_err(internal_error)?;
            match registry.promote_to_voter(tablet_id, leader_id, target_raft_node_id) {
                Ok(()) => {
                    raft_membership_changed = true;
                    raft_membership_operations.push(RaftMembershipOperationStatus {
                        tablet_id,
                        operation: "promote_to_voter",
                        node_id: body.target_node_id.clone(),
                        raft_node_id: Some(target_raft_node_id),
                        status: "applied",
                        error: None,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    raft_membership_operations.push(RaftMembershipOperationStatus {
                        tablet_id,
                        operation: "promote_to_voter",
                        node_id: body.target_node_id.clone(),
                        raft_node_id: Some(target_raft_node_id),
                        status: "failed",
                        error: Some(message.clone()),
                    });
                    return Err(internal_error(message));
                }
            }
            if retire_source {
                if let Some(source_node_id) = source_node_id.as_ref() {
                    let source_raft_node_id = agentdb_storage::stable_raft_node_id(source_node_id)
                        .map_err(internal_error)?;
                    match registry.remove_node(tablet_id, leader_id, source_raft_node_id) {
                        Ok(()) => {
                            raft_membership_operations.push(RaftMembershipOperationStatus {
                                tablet_id,
                                operation: "remove_node",
                                node_id: source_node_id.clone(),
                                raft_node_id: Some(source_raft_node_id),
                                status: "applied",
                                error: None,
                            });
                        }
                        Err(error) => {
                            let message = error.to_string();
                            raft_membership_operations.push(RaftMembershipOperationStatus {
                                tablet_id,
                                operation: "remove_node",
                                node_id: source_node_id.clone(),
                                raft_node_id: Some(source_raft_node_id),
                                status: "failed",
                                error: Some(message.clone()),
                            });
                            return Err(internal_error(message));
                        }
                    }
                }
            }
        } else if retire_source {
            if let Some(source_node_id) = source_node_id.as_ref() {
                registry
                    .status_for(tablet_id)
                    .map_err(|err| {
                        invalid_admin_request(format!(
                            "cannot retire source {source_node_id}; tablet raft group is not registered on this API node: {err}"
                        ))
                    })?;
            }
        }
    }

    let now_ms = now_ms_i64();
    tablet_replicas::upsert(
        meta.pool(),
        tablet_id,
        &body.target_node_id,
        "voter",
        &target_replica.health,
        target_replica.applied_index,
        target_replica.match_index,
        now_ms,
    )
    .await
    .map_err(internal_error)?;
    if retire_source {
        if let Some(source_node_id) = source_node_id.as_ref() {
            let _ = tablet_replicas::delete(meta.pool(), tablet_id, source_node_id)
                .await
                .map_err(internal_error)?;
        }
    }
    let expected_source = source_node_id.clone().ok_or_else(|| {
        invalid_admin_request("source_node_id is required to complete tablet replacement")
    })?;
    let new_owner = tablets::complete_handoff(meta.pool(), tablet_id, &expected_source)
        .await
        .map_err(internal_error)?;
    let changed = new_owner.is_some();
    let tablet = get_tablet_or_404(meta, tablet_id).await?;
    let source_replica = if let Some(source_node_id) = source_node_id.as_ref() {
        get_tablet_replica(meta, tablet_id, source_node_id).await?
    } else {
        None
    };
    let target_replica = get_tablet_replica(meta, tablet_id, &body.target_node_id)
        .await?
        .expect("target replica upserted above");
    Ok(Json(AdminTabletReplacementCompleteResponse {
        action: "complete_replacement",
        changed,
        new_owner_node_id: new_owner,
        source_node_id,
        target_node_id: body.target_node_id,
        required_applied_index,
        raft_membership_changed,
        raft_membership_operations,
        retired_source: retire_source,
        tablet: tablet_view(tablet),
        source_replica: source_replica.map(tablet_replica_view),
        target_replica: tablet_replica_view(target_replica),
    }))
}

async fn rebalance_admin_tablets(
    State(s): State<AppState>,
    Json(body): Json<RebalanceTabletsBody>,
) -> Result<Json<tablet_rebalance::RebalanceReport>, ApiError> {
    let meta = meta_required(&s)?;
    let mut report = tablet_rebalance::run(
        meta.pool(),
        tablet_rebalance::RebalanceRequest {
            dry_run: body.dry_run,
            stale_threshold_ms: body.stale_threshold_ms.unwrap_or(15_000),
            max_movements: body.max_movements.unwrap_or(usize::MAX),
        },
        now_ms_i64(),
    )
    .await
    .map_err(internal_error)?;
    if !body.dry_run {
        if let Some(registry) = s.raft_registry.as_ref() {
            report.membership_operations =
                apply_rebalance_raft_membership_changes(meta, registry, &report.actions).await?;
            retire_draining_rebalance_raft_replicas(meta, registry).await?;
        }
    }
    Ok(Json(report))
}

async fn apply_rebalance_raft_membership_changes(
    meta: &agentdb_meta::MetaStore,
    registry: &crate::raft_ingress::RaftRsGroupRegistry,
    actions: &[tablet_rebalance::RebalanceAction],
) -> Result<Vec<tablet_rebalance::RebalanceMembershipOperation>, ApiError> {
    let mut operations = Vec::new();
    for action in actions {
        if action.action != "complete_replacement" || !action.changed {
            continue;
        }
        let Some(target_node_id) = action.target_node_id.as_deref() else {
            continue;
        };
        let Ok(statuses) = registry.status_for(action.tablet_id) else {
            continue;
        };
        let Some(leader_id) = raft_leader_id_from_statuses(&statuses) else {
            continue;
        };
        let target_raft_node_id = match agentdb_storage::stable_raft_node_id(target_node_id) {
            Ok(node_id) => node_id,
            Err(error) => {
                tracing::warn!(
                    tablet_id = action.tablet_id,
                    target_node_id,
                    error = %error,
                    "skipping rebalance raft membership change; target node id is invalid"
                );
                operations.push(tablet_rebalance::RebalanceMembershipOperation {
                    tablet_id: action.tablet_id,
                    operation: "promote_to_voter",
                    node_id: target_node_id.to_string(),
                    raft_node_id: None,
                    status: "failed",
                    error: Some(error.to_string()),
                });
                continue;
            }
        };
        if let Err(error) =
            registry.promote_to_voter(action.tablet_id, leader_id, target_raft_node_id)
        {
            tracing::warn!(
                tablet_id = action.tablet_id,
                target_node_id,
                error = %error,
                "failed to promote rebalance target in raft"
            );
            operations.push(tablet_rebalance::RebalanceMembershipOperation {
                tablet_id: action.tablet_id,
                operation: "promote_to_voter",
                node_id: target_node_id.to_string(),
                raft_node_id: Some(target_raft_node_id),
                status: "failed",
                error: Some(error.to_string()),
            });
            continue;
        }
        operations.push(tablet_rebalance::RebalanceMembershipOperation {
            tablet_id: action.tablet_id,
            operation: "promote_to_voter",
            node_id: target_node_id.to_string(),
            raft_node_id: Some(target_raft_node_id),
            status: "applied",
            error: None,
        });

        let Some(source_node_id) = action.source_node_id.as_deref() else {
            continue;
        };
        let source_raft_node_id = match agentdb_storage::stable_raft_node_id(source_node_id) {
            Ok(node_id) => node_id,
            Err(error) => {
                tracing::warn!(
                    tablet_id = action.tablet_id,
                    source_node_id,
                    error = %error,
                    "skipping rebalance source retirement; source node id is invalid"
                );
                operations.push(tablet_rebalance::RebalanceMembershipOperation {
                    tablet_id: action.tablet_id,
                    operation: "remove_node",
                    node_id: source_node_id.to_string(),
                    raft_node_id: None,
                    status: "failed",
                    error: Some(error.to_string()),
                });
                continue;
            }
        };
        if let Err(error) = registry.remove_node(action.tablet_id, leader_id, source_raft_node_id) {
            tracing::warn!(
                tablet_id = action.tablet_id,
                source_node_id,
                error = %error,
                "failed to retire rebalance source from raft"
            );
            operations.push(tablet_rebalance::RebalanceMembershipOperation {
                tablet_id: action.tablet_id,
                operation: "remove_node",
                node_id: source_node_id.to_string(),
                raft_node_id: Some(source_raft_node_id),
                status: "failed",
                error: Some(error.to_string()),
            });
            continue;
        }
        operations.push(tablet_rebalance::RebalanceMembershipOperation {
            tablet_id: action.tablet_id,
            operation: "remove_node",
            node_id: source_node_id.to_string(),
            raft_node_id: Some(source_raft_node_id),
            status: "applied",
            error: None,
        });
        if tablet_replicas::delete(meta.pool(), action.tablet_id, source_node_id)
            .await
            .map_err(internal_error)?
        {
            operations.push(tablet_rebalance::RebalanceMembershipOperation {
                tablet_id: action.tablet_id,
                operation: "delete_source_replica",
                node_id: source_node_id.to_string(),
                raft_node_id: Some(source_raft_node_id),
                status: "applied",
                error: None,
            });
        }
    }
    Ok(operations)
}

async fn retire_draining_rebalance_raft_replicas(
    meta: &agentdb_meta::MetaStore,
    registry: &crate::raft_ingress::RaftRsGroupRegistry,
) -> Result<usize, ApiError> {
    let tablets: BTreeMap<i64, tablets::Tablet> = tablets::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .map(|tablet| (tablet.tablet_id, tablet))
        .collect();
    let replicas = tablet_replicas::list_all(meta.pool())
        .await
        .map_err(internal_error)?;
    let mut retired = 0usize;
    for replica in replicas {
        if replica.role == "log_only" || replica.health != "draining" {
            continue;
        }
        if tablets
            .get(&replica.tablet_id)
            .and_then(|tablet| tablet.owner_node_id.as_deref())
            == Some(replica.node_id.as_str())
        {
            continue;
        }
        let Ok(statuses) = registry.status_for(replica.tablet_id) else {
            continue;
        };
        let Some(leader_id) = raft_leader_id_from_statuses(&statuses) else {
            continue;
        };
        let raft_node_id = match agentdb_storage::stable_raft_node_id(&replica.node_id) {
            Ok(node_id) => node_id,
            Err(error) => {
                tracing::warn!(
                    tablet_id = replica.tablet_id,
                    node_id = %replica.node_id,
                    error = %error,
                    "skipping draining raft replica retirement; node id is invalid"
                );
                continue;
            }
        };
        if let Err(error) = registry.remove_node(replica.tablet_id, leader_id, raft_node_id) {
            tracing::warn!(
                tablet_id = replica.tablet_id,
                node_id = %replica.node_id,
                error = %error,
                "failed to retire draining raft replica"
            );
            continue;
        }
        if tablet_replicas::delete(meta.pool(), replica.tablet_id, &replica.node_id)
            .await
            .map_err(internal_error)?
        {
            retired += 1;
        }
    }
    Ok(retired)
}

fn raft_leader_id_from_statuses(statuses: &[agentdb_storage::RaftRsReplicaStatus]) -> Option<u64> {
    statuses
        .iter()
        .find(|status| status.role == "Leader")
        .map(|status| status.node_id)
        .or_else(|| {
            statuses
                .iter()
                .find(|status| status.leader_id != 0)
                .map(|status| status.leader_id)
        })
}

#[derive(Debug, Deserialize)]
struct RebalanceTabletsBody {
    #[serde(default)]
    dry_run: bool,
    stale_threshold_ms: Option<i64>,
    max_movements: Option<usize>,
}

async fn list_tablets_for_backup(
    meta: &agentdb_meta::MetaStore,
) -> Result<Vec<AdminTabletView>, ApiError> {
    let rows = tablets::list_all(meta.pool())
        .await
        .map_err(internal_error)?;
    Ok(rows.into_iter().map(tablet_view).collect())
}

#[derive(Debug, Deserialize)]
struct RouteDecisionQuery {
    tenant_id: String,
    session_id: Option<String>,
}

async fn get_routing_decision(
    State(s): State<AppState>,
    Query(query): Query<RouteDecisionQuery>,
) -> Json<crate::routing_http::RouteDecisionDryRun> {
    Json(
        s.routing
            .dry_run_decision(&query.tenant_id, query.session_id.as_deref()),
    )
}

async fn create_tenant(
    State(s): State<AppState>,
    Json(body): Json<CreateTenantBody>,
) -> Result<Json<Tenant>, ApiError> {
    reject_if_draining(&s)?;
    let t = s
        .runtime
        .create_tenant(NewTenant {
            external_key: body.external_key,
            policy: body.policy,
        })
        .await?;
    Ok(Json(t))
}

#[derive(Debug, Serialize)]
struct ListTenantsResponse {
    count: usize,
    tenants: Vec<Tenant>,
}

async fn list_tenants(State(s): State<AppState>) -> Json<ListTenantsResponse> {
    let tenants = s.runtime.list_tenants().await;
    Json(ListTenantsResponse {
        count: tenants.len(),
        tenants,
    })
}

async fn get_fleet_storage_summary(
    State(s): State<AppState>,
) -> Json<agentdb_core::FleetStorageSummary> {
    Json(s.runtime.fleet_storage_summary().await)
}

async fn get_content_mirror_status(State(s): State<AppState>) -> Json<ContentMirrorStatusResponse> {
    let Some(mirror) = s.content_mirror.as_ref() else {
        return Json(ContentMirrorStatusResponse {
            enabled: false,
            queued: 0,
            completed: 0,
            failed: 0,
            backlog: 0,
        });
    };
    let stats = mirror.stats();
    Json(ContentMirrorStatusResponse {
        enabled: true,
        queued: stats.queued,
        completed: stats.completed,
        failed: stats.failed,
        backlog: stats.backlog,
    })
}

async fn get_raft_status(State(s): State<AppState>) -> Result<Json<RaftStatusResponse>, ApiError> {
    let configured_by_tablet = load_configured_raft_replicas(s.meta.as_ref()).await?;
    let latest_snapshots = load_latest_raft_snapshots(s.meta.as_ref()).await?;
    let now_ms = now_ms_i64();
    let Some(registry) = s.raft_registry.as_ref() else {
        let configured_replica_count = configured_by_tablet.values().map(Vec::len).sum();
        let unhealthy_replica_count = count_unhealthy_configured_replicas(&configured_by_tablet);
        let tablets = configured_only_raft_tablet_statuses(
            &configured_by_tablet,
            &latest_snapshots,
            now_ms,
            &BTreeSet::new(),
        );
        let summary = raft_ops_summary(&tablets, now_ms);
        return Ok(Json(RaftStatusResponse {
            enabled: false,
            control_plane_enabled: s.meta.is_some(),
            tablet_count: tablets.len(),
            configured_replica_count,
            unhealthy_replica_count,
            pending_conf_change_count: 0,
            outbound_message_count: 0,
            summary,
            tablets,
        }));
    };
    let mut open_tablet_ids = BTreeSet::new();
    let mut tablets: Vec<_> = registry
        .groups_snapshot()
        .into_iter()
        .map(|(tablet_id, group)| {
            open_tablet_ids.insert(tablet_id);
            let runtime_replicas: Vec<_> = group
                .status()
                .into_iter()
                .map(|status| RaftReplicaStatus {
                    node_id: status.node_id,
                    role: status.role,
                    leader_id: status.leader_id,
                    term: status.term,
                    commit_index: status.commit_index,
                    applied_index: status.applied_index,
                    pending_conf_change: status.pending_conf_change,
                    outbound_message_count: status.outbound_message_count,
                })
                .collect();
            let configured = configured_by_tablet
                .get(&tablet_id)
                .map(|replicas| {
                    configured_raft_replica_statuses(
                        replicas,
                        &runtime_replicas,
                        &latest_snapshots,
                        now_ms_i64(),
                    )
                })
                .unwrap_or_default();
            RaftTabletStatus {
                tablet_id,
                local_group_open: true,
                replicas: runtime_replicas,
                configured_replicas: configured,
            }
        })
        .collect();
    tablets.extend(configured_only_raft_tablet_statuses(
        &configured_by_tablet,
        &latest_snapshots,
        now_ms,
        &open_tablet_ids,
    ));
    let configured_replica_count = configured_by_tablet.values().map(Vec::len).sum();
    let unhealthy_replica_count = count_unhealthy_configured_replicas(&configured_by_tablet);
    let pending_conf_change_count = tablets
        .iter()
        .filter(|tablet| {
            tablet
                .replicas
                .iter()
                .any(|replica| replica.pending_conf_change)
        })
        .count();
    let outbound_message_count = tablets
        .iter()
        .flat_map(|tablet| &tablet.replicas)
        .map(|replica| replica.outbound_message_count)
        .sum();
    let summary = raft_ops_summary(&tablets, now_ms);
    Ok(Json(RaftStatusResponse {
        enabled: true,
        control_plane_enabled: s.meta.is_some(),
        tablet_count: tablets.len(),
        configured_replica_count,
        unhealthy_replica_count,
        pending_conf_change_count,
        outbound_message_count,
        summary,
        tablets,
    }))
}

fn raft_ops_summary(tablets: &[RaftTabletStatus], now_ms: i64) -> RaftOpsSummary {
    let mut summary = RaftOpsSummary {
        leaderless_tablet_count: 0,
        multi_leader_tablet_count: 0,
        commit_index_drift_tablet_count: 0,
        applied_index_drift_tablet_count: 0,
        max_commit_index_gap: 0,
        max_applied_index_gap: 0,
        catch_up_required_replica_count: 0,
        lagging_replica_count: 0,
        unreachable_replica_count: 0,
        snapshot_absent_for_catch_up_count: 0,
        expired_snapshot_count: 0,
    };

    for tablet in tablets {
        if tablet.local_group_open {
            let local_leader_count = tablet
                .replicas
                .iter()
                .filter(|replica| replica.role == "Leader")
                .count();
            let known_leader_count = tablet
                .replicas
                .iter()
                .filter_map(|replica| (replica.leader_id != 0).then_some(replica.leader_id))
                .collect::<BTreeSet<_>>()
                .len();
            if local_leader_count == 0 && known_leader_count == 0 {
                summary.leaderless_tablet_count += 1;
            } else if local_leader_count > 1 || known_leader_count > 1 {
                summary.multi_leader_tablet_count += 1;
            }

            let commit_gap = index_gap(tablet.replicas.iter().map(|replica| replica.commit_index));
            if commit_gap > 0 {
                summary.commit_index_drift_tablet_count += 1;
                summary.max_commit_index_gap = summary.max_commit_index_gap.max(commit_gap);
            }
            let applied_gap =
                index_gap(tablet.replicas.iter().map(|replica| replica.applied_index));
            if applied_gap > 0 {
                summary.applied_index_drift_tablet_count += 1;
                summary.max_applied_index_gap = summary.max_applied_index_gap.max(applied_gap);
            }
        }

        for replica in &tablet.configured_replicas {
            if replica.catch_up_required {
                summary.catch_up_required_replica_count += 1;
                if replica.last_snapshot_index.is_none() {
                    summary.snapshot_absent_for_catch_up_count += 1;
                }
            }
            if replica.health == "lagging" || replica.replication_lag > 0 {
                summary.lagging_replica_count += 1;
            }
            if replica.health == "unreachable" || replica.membership_state == "unreachable" {
                summary.unreachable_replica_count += 1;
            }
            if replica
                .last_snapshot_expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
            {
                summary.expired_snapshot_count += 1;
            }
        }
    }

    summary
}

fn index_gap(indexes: impl Iterator<Item = u64>) -> u64 {
    let mut min = u64::MAX;
    let mut max = 0;
    let mut count = 0;
    for index in indexes {
        min = min.min(index);
        max = max.max(index);
        count += 1;
    }
    if count < 2 {
        0
    } else {
        max.saturating_sub(min)
    }
}

fn configured_only_raft_tablet_statuses(
    configured_by_tablet: &BTreeMap<i64, Vec<tablet_replicas::TabletReplica>>,
    latest_snapshots: &BTreeMap<(i64, String), tablet_snapshots::TabletSnapshot>,
    now_ms: i64,
    open_tablet_ids: &BTreeSet<i64>,
) -> Vec<RaftTabletStatus> {
    configured_by_tablet
        .iter()
        .filter(|(tablet_id, _)| !open_tablet_ids.contains(tablet_id))
        .map(|(tablet_id, replicas)| RaftTabletStatus {
            tablet_id: *tablet_id,
            local_group_open: false,
            replicas: Vec::new(),
            configured_replicas: configured_raft_replica_statuses(
                replicas,
                &[],
                latest_snapshots,
                now_ms,
            ),
        })
        .collect()
}

async fn post_raft_catch_up(
    State(s): State<AppState>,
    Json(body): Json<RaftCatchUpBody>,
) -> Result<Json<RaftCatchUpResponse>, ApiError> {
    if body.tablet_id.is_some_and(|tablet_id| tablet_id <= 0) {
        return Err(invalid_admin_request("tablet_id must be positive"));
    }
    let meta = meta_required(&s)?;
    let Some(registry) = s.raft_registry.as_ref() else {
        return Err(invalid_admin_request(
            "raft registry is not configured on this node",
        ));
    };
    let configured_by_tablet = load_configured_raft_replicas(Some(meta)).await?;
    let dry_run = body.dry_run.unwrap_or(false);
    let min_lag = body.min_lag.unwrap_or(1);
    let now_ms = now_ms_i64();
    let expires_at_ms = snapshot_expires_at(now_ms, body.retention_ms)?;
    let mut response = RaftCatchUpResponse {
        dry_run,
        tablet_count: 0,
        attempted: 0,
        sent: 0,
        skipped: 0,
        failures: Vec::new(),
        actions: Vec::new(),
    };

    let outbox = if dry_run {
        None
    } else {
        let token = s.internal_auth_token.clone().ok_or_else(|| {
            invalid_admin_request("internal auth token is required for raft catch-up")
        })?;
        let outbox = RaftRsHttpOutbox::new(token);
        outbox.replace_peer_urls(load_raft_peer_urls(meta, &configured_by_tablet).await?);
        Some(outbox)
    };

    for (tablet_id, group) in registry.groups_snapshot() {
        if body
            .tablet_id
            .is_some_and(|requested| requested != tablet_id)
        {
            continue;
        }
        response.tablet_count += 1;
        let statuses = group.status();
        let Some(source_status) = statuses.iter().max_by_key(|status| status.applied_index) else {
            response.skipped += 1;
            continue;
        };
        let Some(replicas) = configured_by_tablet.get(&tablet_id) else {
            response.skipped += 1;
            continue;
        };
        let source_node_id = raft_node_label(replicas, source_status.node_id)
            .unwrap_or_else(|| source_status.node_id.to_string());
        let mut application_snapshot: Option<Vec<u8>> = None;
        let mut compact_through = 0u64;
        for replica in replicas {
            if !matches!(replica.role.as_str(), "voter" | "learner") {
                response.skipped += 1;
                continue;
            }
            if body
                .target_node_id
                .as_ref()
                .is_some_and(|target| target != &replica.node_id)
            {
                continue;
            }
            let to = match agentdb_storage::stable_raft_node_id(&replica.node_id) {
                Ok(to) => to,
                Err(err) => {
                    response.failures.push(RaftCatchUpFailure {
                        tablet_id,
                        node_id: replica.node_id.clone(),
                        to_raft_node_id: None,
                        error: err.to_string(),
                    });
                    continue;
                }
            };
            if to == source_status.node_id {
                if !dry_run {
                    let index = raft_index_to_i64(source_status.applied_index)?;
                    tablet_replicas::set_progress(
                        meta.pool(),
                        tablet_id,
                        &replica.node_id,
                        index,
                        index,
                        now_ms_i64(),
                    )
                    .await
                    .map_err(internal_error)?;
                    tablet_replicas::set_health(
                        meta.pool(),
                        tablet_id,
                        &replica.node_id,
                        "healthy",
                        now_ms_i64(),
                    )
                    .await
                    .map_err(internal_error)?;
                }
                response.skipped += 1;
                continue;
            }
            let peer_applied = replica.applied_index.max(0) as u64;
            let lag = source_status.applied_index.saturating_sub(peer_applied);
            if lag < min_lag && replica.health != "lagging" {
                response.skipped += 1;
                continue;
            }
            response.attempted += 1;
            if dry_run {
                response.actions.push(RaftCatchUpAction {
                    tablet_id,
                    node_id: replica.node_id.clone(),
                    from_raft_node_id: source_status.node_id,
                    to_raft_node_id: to,
                    lag,
                    snapshot_index: Some(source_status.applied_index),
                    status: "would_send".into(),
                });
                continue;
            }
            let snapshot = match application_snapshot.as_ref() {
                Some(snapshot) => snapshot.clone(),
                None => match s.runtime.export_tablet_snapshot_payload(tablet_id) {
                    Ok(snapshot) => {
                        application_snapshot = Some(snapshot.clone());
                        snapshot
                    }
                    Err(err) => {
                        response.failures.push(RaftCatchUpFailure {
                            tablet_id,
                            node_id: replica.node_id.clone(),
                            to_raft_node_id: Some(to),
                            error: err.message,
                        });
                        continue;
                    }
                },
            };
            let transfer = match group.build_snapshot_transfer(source_status.node_id, to, snapshot)
            {
                Ok(transfer) => transfer,
                Err(err) => {
                    response.failures.push(RaftCatchUpFailure {
                        tablet_id,
                        node_id: replica.node_id.clone(),
                        to_raft_node_id: Some(to),
                        error: err.to_string(),
                    });
                    continue;
                }
            };
            tablet_replicas::set_health(
                meta.pool(),
                tablet_id,
                &replica.node_id,
                "lagging",
                now_ms_i64(),
            )
            .await
            .map_err(internal_error)?;
            match outbox
                .as_ref()
                .unwrap()
                .send_snapshot_to_peer(&transfer)
                .await
            {
                Ok(()) => {
                    let index = raft_index_to_i64(transfer.index)?;
                    tablet_snapshots::record(
                        meta.pool(),
                        &raft_snapshot_id(tablet_id, transfer.from, transfer.to, transfer.index),
                        tablet_id,
                        &source_node_id,
                        raft_index_to_i64(transfer.from)?,
                        &replica.node_id,
                        raft_index_to_i64(transfer.to)?,
                        index,
                        raft_index_to_i64(transfer.term)?,
                        i64::try_from(transfer.payload.len())
                            .map_err(|_| internal_error("snapshot payload exceeds i64::MAX"))?,
                        &agentdb_storage::sha256_hex(&transfer.payload),
                        None,
                        "available",
                        now_ms_i64(),
                        expires_at_ms,
                    )
                    .await
                    .map_err(internal_error)?;
                    tablet_replicas::set_progress(
                        meta.pool(),
                        tablet_id,
                        &replica.node_id,
                        index,
                        index,
                        now_ms_i64(),
                    )
                    .await
                    .map_err(internal_error)?;
                    tablet_replicas::set_health(
                        meta.pool(),
                        tablet_id,
                        &replica.node_id,
                        "healthy",
                        now_ms_i64(),
                    )
                    .await
                    .map_err(internal_error)?;
                    compact_through = compact_through.max(transfer.index);
                    response.sent += 1;
                    response.actions.push(RaftCatchUpAction {
                        tablet_id,
                        node_id: replica.node_id.clone(),
                        from_raft_node_id: source_status.node_id,
                        to_raft_node_id: to,
                        lag,
                        snapshot_index: Some(transfer.index),
                        status: "sent".into(),
                    });
                }
                Err(err) => {
                    tablet_replicas::set_health(
                        meta.pool(),
                        tablet_id,
                        &replica.node_id,
                        "unreachable",
                        now_ms_i64(),
                    )
                    .await
                    .map_err(internal_error)?;
                    response.failures.push(RaftCatchUpFailure {
                        tablet_id,
                        node_id: replica.node_id.clone(),
                        to_raft_node_id: Some(to),
                        error: err.to_string(),
                    });
                }
            }
        }
        if compact_through > 0 {
            let _ = s
                .runtime
                .compact_tablet_log_through(tablet_id, compact_through);
        }
    }

    if body.tablet_id.is_some() && response.tablet_count == 0 {
        return Err(invalid_admin_request(
            "requested tablet has no local raft group",
        ));
    }

    Ok(Json(response))
}

async fn get_raft_snapshots(
    State(s): State<AppState>,
    Query(query): Query<RaftSnapshotsQuery>,
) -> Result<Json<RaftSnapshotsResponse>, ApiError> {
    if query.tablet_id.is_some_and(|tablet_id| tablet_id <= 0) {
        return Err(invalid_admin_request("tablet_id must be positive"));
    }
    let meta = meta_required(&s)?;
    let now_ms = now_ms_i64();
    let snapshots = tablet_snapshots::list_recent(
        meta.pool(),
        query.tablet_id,
        query.include_deleted.unwrap_or(false),
        query.limit.unwrap_or(100),
    )
    .await
    .map_err(internal_error)?
    .into_iter()
    .map(|snapshot| raft_snapshot_view(snapshot, now_ms))
    .collect();
    Ok(Json(RaftSnapshotsResponse { snapshots }))
}

async fn post_raft_snapshot_prune(
    State(s): State<AppState>,
    Json(body): Json<RaftSnapshotPruneBody>,
) -> Result<Json<RaftSnapshotPruneResponse>, ApiError> {
    let meta = meta_required(&s)?;
    let dry_run = body.dry_run.unwrap_or(false);
    let now_ms = now_ms_i64();
    let expired_snapshot_count = tablet_snapshots::count_expired(meta.pool(), now_ms)
        .await
        .map_err(internal_error)?;
    let deleted_snapshot_count =
        tablet_snapshots::mark_expired_deleted(meta.pool(), now_ms, dry_run)
            .await
            .map_err(internal_error)?;
    Ok(Json(RaftSnapshotPruneResponse {
        dry_run,
        expired_snapshot_count,
        deleted_snapshot_count: if dry_run { 0 } else { deleted_snapshot_count },
    }))
}

async fn load_configured_raft_replicas(
    meta: Option<&agentdb_meta::MetaStore>,
) -> Result<BTreeMap<i64, Vec<tablet_replicas::TabletReplica>>, ApiError> {
    let Some(meta) = meta else {
        return Ok(BTreeMap::new());
    };
    let mut by_tablet: BTreeMap<i64, Vec<tablet_replicas::TabletReplica>> = BTreeMap::new();
    for replica in tablet_replicas::list_all(meta.pool())
        .await
        .map_err(internal_error)?
    {
        by_tablet
            .entry(replica.tablet_id)
            .or_default()
            .push(replica);
    }
    Ok(by_tablet)
}

async fn load_raft_peer_urls(
    meta: &agentdb_meta::MetaStore,
    configured_by_tablet: &BTreeMap<i64, Vec<tablet_replicas::TabletReplica>>,
) -> Result<BTreeMap<u64, String>, ApiError> {
    let needed_node_ids: BTreeSet<_> = configured_by_tablet
        .values()
        .flatten()
        .map(|replica| replica.node_id.as_str())
        .collect();
    let nodes_by_id: BTreeMap<_, _> = nodes::list_all(meta.pool())
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|node| needed_node_ids.contains(node.node_id.as_str()))
        .map(|node| (node.node_id.clone(), node))
        .collect();
    let mut peer_urls = BTreeMap::new();
    for replica in configured_by_tablet.values().flatten() {
        let raft_node_id =
            agentdb_storage::stable_raft_node_id(&replica.node_id).map_err(internal_error)?;
        if let Some(node) = nodes_by_id.get(&replica.node_id) {
            peer_urls.insert(raft_node_id, node.address.clone());
        }
    }
    Ok(peer_urls)
}

async fn load_latest_raft_snapshots(
    meta: Option<&agentdb_meta::MetaStore>,
) -> Result<BTreeMap<(i64, String), tablet_snapshots::TabletSnapshot>, ApiError> {
    let Some(meta) = meta else {
        return Ok(BTreeMap::new());
    };
    let mut latest = BTreeMap::new();
    for snapshot in tablet_snapshots::latest_available_by_tablet(meta.pool())
        .await
        .map_err(internal_error)?
    {
        let key = (snapshot.tablet_id, snapshot.target_node_id.clone());
        let replace = latest
            .get(&key)
            .map(|existing: &tablet_snapshots::TabletSnapshot| {
                snapshot.snapshot_index > existing.snapshot_index
            })
            .unwrap_or(true);
        if replace {
            latest.insert(key, snapshot);
        }
    }
    Ok(latest)
}

fn raft_node_label(
    replicas: &[tablet_replicas::TabletReplica],
    raft_node_id: u64,
) -> Option<String> {
    replicas.iter().find_map(|replica| {
        let candidate = agentdb_storage::stable_raft_node_id(&replica.node_id).ok()?;
        (candidate == raft_node_id).then(|| replica.node_id.clone())
    })
}

fn raft_snapshot_view(snapshot: tablet_snapshots::TabletSnapshot, now_ms: i64) -> RaftSnapshotView {
    let expired = snapshot
        .expires_at_ms
        .map(|expires_at_ms| expires_at_ms <= now_ms)
        .unwrap_or(false);
    RaftSnapshotView {
        snapshot_id: snapshot.snapshot_id,
        tablet_id: snapshot.tablet_id,
        source_node_id: snapshot.source_node_id,
        source_raft_node_id: snapshot.source_raft_node_id,
        target_node_id: snapshot.target_node_id,
        target_raft_node_id: snapshot.target_raft_node_id,
        snapshot_index: snapshot.snapshot_index,
        snapshot_term: snapshot.snapshot_term,
        size_bytes: snapshot.size_bytes,
        sha256_hex: snapshot.sha256_hex,
        storage_uri: snapshot.storage_uri,
        status: snapshot.status,
        created_at_ms: snapshot.created_at_ms,
        updated_at_ms: snapshot.updated_at_ms,
        expires_at_ms: snapshot.expires_at_ms,
        age_ms: now_ms.saturating_sub(snapshot.created_at_ms),
        expired,
    }
}

fn count_unhealthy_configured_replicas(
    configured_by_tablet: &BTreeMap<i64, Vec<tablet_replicas::TabletReplica>>,
) -> usize {
    configured_by_tablet
        .values()
        .flatten()
        .filter(|replica| replica.health != "healthy")
        .count()
}

fn configured_raft_replica_statuses(
    replicas: &[tablet_replicas::TabletReplica],
    runtime_replicas: &[RaftReplicaStatus],
    latest_snapshots: &BTreeMap<(i64, String), tablet_snapshots::TabletSnapshot>,
    now_ms: i64,
) -> Vec<RaftConfiguredReplicaStatus> {
    let source_applied_index = runtime_replicas
        .iter()
        .map(|status| status.applied_index)
        .max()
        .unwrap_or(0);
    replicas
        .iter()
        .map(|replica| {
            let raft_node_id = agentdb_storage::stable_raft_node_id(&replica.node_id).ok();
            let runtime = raft_node_id.and_then(|node_id| {
                runtime_replicas
                    .iter()
                    .find(|status| status.node_id == node_id)
            });
            let replica_applied_index = runtime
                .map(|status| status.applied_index)
                .unwrap_or_else(|| replica.applied_index.max(0) as u64);
            let replication_lag = source_applied_index.saturating_sub(replica_applied_index);
            let latest_snapshot =
                latest_snapshots.get(&(replica.tablet_id, replica.node_id.clone()));
            let pending_conf_change = runtime
                .map(|status| status.pending_conf_change)
                .unwrap_or(false);
            let outbound_message_count = runtime
                .map(|status| status.outbound_message_count)
                .unwrap_or(0);
            let membership_state = if pending_conf_change {
                "membership_change_pending"
            } else if replica.role == "learner" {
                "catching_up"
            } else if replica.health == "draining" {
                "retirement_pending"
            } else if replica.health == "unreachable" {
                "unreachable"
            } else if replication_lag > 0 || replica.health == "lagging" {
                "catch_up_required"
            } else {
                "stable"
            };
            RaftConfiguredReplicaStatus {
                node_id: replica.node_id.clone(),
                raft_node_id,
                role: replica.role.clone(),
                health: replica.health.clone(),
                applied_index: replica.applied_index,
                match_index: replica.match_index,
                replication_lag,
                catch_up_required: replication_lag > 0 || replica.health == "lagging",
                last_snapshot_index: latest_snapshot.map(|snapshot| snapshot.snapshot_index),
                last_snapshot_age_ms: latest_snapshot
                    .map(|snapshot| now_ms.saturating_sub(snapshot.created_at_ms)),
                last_snapshot_status: latest_snapshot.map(|snapshot| snapshot.status.clone()),
                last_snapshot_expires_at_ms: latest_snapshot
                    .and_then(|snapshot| snapshot.expires_at_ms),
                updated_at_ms: replica.updated_at_ms,
                local_runtime: runtime.is_some(),
                runtime_role: runtime.map(|status| status.role.clone()),
                runtime_leader_id: runtime.map(|status| status.leader_id),
                runtime_term: runtime.map(|status| status.term),
                runtime_commit_index: runtime.map(|status| status.commit_index),
                runtime_applied_index: runtime.map(|status| status.applied_index),
                membership_state: membership_state.into(),
                pending_conf_change,
                outbound_message_count,
            }
        })
        .collect()
}

fn build_cluster_health(
    status: &crate::routing_http::RouteRuntimeStatus,
    local_is_draining: bool,
    stale_threshold_ms: i64,
) -> ClusterHealthResponse {
    let now_ms = now_ms_i64();
    let mut issues = Vec::new();
    match status.snapshot.as_ref() {
        None => ClusterHealthResponse {
            mode: status.mode,
            local_node_id: status.local_node_id.clone(),
            status: "ok",
            stale_threshold_ms,
            single_node: true,
            local_node_found: true,
            local_node_stale: false,
            local_node_state: Some("single-node".into()),
            summary: ClusterHealthSummary {
                node_count: 1,
                alive_node_count: 1,
                stale_node_count: 0,
                draining_node_count: 0,
                dead_node_count: 0,
                tablet_count: 0,
                unowned_tablet_count: 0,
                tablets_with_stale_owner_count: 0,
            },
            issues,
        },
        Some(snapshot) => {
            let local_node = snapshot
                .nodes
                .iter()
                .find(|node| node.node_id == status.local_node_id);
            let local_node_found = local_node.is_some();
            let local_node_stale = local_node
                .map(|node| now_ms - node.last_heartbeat_ms > stale_threshold_ms)
                .unwrap_or(true);
            let local_node_state = if local_is_draining {
                Some("draining".into())
            } else {
                local_node.map(|node| node.state.clone())
            };
            let stale_node_count = snapshot
                .nodes
                .iter()
                .filter(|node| now_ms - node.last_heartbeat_ms > stale_threshold_ms)
                .count();
            let draining_node_count = snapshot
                .nodes
                .iter()
                .filter(|node| node.state == "draining")
                .count();
            let dead_node_count = snapshot
                .nodes
                .iter()
                .filter(|node| node.state == "dead")
                .count();
            let alive_node_count = snapshot
                .nodes
                .iter()
                .filter(|node| node.state == "alive")
                .count();
            let tablets_with_stale_owner_count = snapshot
                .tablets
                .iter()
                .filter(|tablet| {
                    tablet.owner_node_id.as_ref().is_some_and(|owner_id| {
                        snapshot
                            .nodes
                            .iter()
                            .find(|node| &node.node_id == owner_id)
                            .is_some_and(|node| {
                                now_ms - node.last_heartbeat_ms > stale_threshold_ms
                            })
                    })
                })
                .count();
            let unowned_tablet_count = snapshot
                .tablets
                .iter()
                .filter(|tablet| tablet.owner_node_id.is_none())
                .count();

            if !local_node_found {
                issues.push("local node is missing from the routing snapshot".into());
            }
            if local_node_stale {
                issues.push("local node heartbeat is stale".into());
            }
            if local_is_draining {
                issues.push("local node is draining".into());
            }
            if unowned_tablet_count > 0 {
                issues.push(format!(
                    "{unowned_tablet_count} tablet(s) are currently unowned"
                ));
            }
            if tablets_with_stale_owner_count > 0 {
                issues.push(format!(
                    "{tablets_with_stale_owner_count} tablet(s) are owned by stale nodes"
                ));
            }
            if dead_node_count > 0 {
                issues.push(format!("{dead_node_count} node(s) are marked dead"));
            }
            if draining_node_count > 0 {
                issues.push(format!("{draining_node_count} node(s) are draining"));
            }

            let overall_status = if !local_node_found || local_node_stale {
                "error"
            } else if !issues.is_empty() {
                "degraded"
            } else {
                "ok"
            };

            ClusterHealthResponse {
                mode: status.mode,
                local_node_id: status.local_node_id.clone(),
                status: overall_status,
                stale_threshold_ms,
                single_node: false,
                local_node_found,
                local_node_stale,
                local_node_state,
                summary: ClusterHealthSummary {
                    node_count: snapshot.node_count,
                    alive_node_count,
                    stale_node_count,
                    draining_node_count,
                    dead_node_count,
                    tablet_count: snapshot.tablet_count,
                    unowned_tablet_count,
                    tablets_with_stale_owner_count,
                },
                issues,
            }
        }
    }
}

fn build_cluster_readiness(
    status: &crate::routing_http::RouteRuntimeStatus,
    local_is_draining: bool,
    stale_threshold_ms: i64,
) -> ClusterReadinessResponse {
    let health = build_cluster_health(status, local_is_draining, stale_threshold_ms);
    if health.single_node {
        return ClusterReadinessResponse {
            mode: health.mode,
            local_node_id: health.local_node_id,
            ready: true,
            stale_threshold_ms,
            reasons: Vec::new(),
        };
    }

    let mut reasons = Vec::new();
    if !health.local_node_found {
        reasons.push("local node is missing from the routing snapshot".into());
    }
    if health.local_node_stale {
        reasons.push("local node heartbeat is stale".into());
    }
    if health.local_node_state.as_deref() == Some("draining") {
        reasons.push("local node is draining".into());
    }
    if health.summary.unowned_tablet_count > 0 {
        reasons.push(format!(
            "{} tablet(s) are currently unowned",
            health.summary.unowned_tablet_count
        ));
    }
    if health.summary.tablets_with_stale_owner_count > 0 {
        reasons.push(format!(
            "{} tablet(s) are owned by stale nodes",
            health.summary.tablets_with_stale_owner_count
        ));
    }

    ClusterReadinessResponse {
        mode: health.mode,
        local_node_id: health.local_node_id,
        ready: reasons.is_empty(),
        stale_threshold_ms,
        reasons,
    }
}

async fn get_tenant(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    Ok(Json(s.runtime.get_tenant(&tid).await?))
}

#[derive(Debug, Serialize)]
struct ListSessionsResponse {
    count: usize,
    sessions: Vec<agentdb_core::Session>,
}

async fn list_sessions(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<ListSessionsResponse>, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sessions = s.runtime.list_sessions(&tid).await?;
    Ok(Json(ListSessionsResponse {
        count: sessions.len(),
        sessions,
    }))
}

async fn delete_tenant(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    s.runtime.delete_tenant(&tid).await?;
    delete_tenant_session_locators(&s, &tid).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_agent(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<CreateAgentBody>,
) -> Result<Json<Agent>, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let a = s
        .runtime
        .create_agent(NewAgent {
            tenant_id: tid,
            alias: body.alias,
            config: body.config,
        })
        .await?;
    Ok(Json(a))
}

#[derive(Debug, Serialize)]
struct ListAgentsResponse {
    count: usize,
    agents: Vec<Agent>,
}

async fn list_agents(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<ListAgentsResponse>, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let agents = s.runtime.list_agents(&tid).await?;
    Ok(Json(ListAgentsResponse {
        count: agents.len(),
        agents,
    }))
}

async fn get_agent(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
) -> Result<Json<Agent>, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let aid = parse_id_or(
        &agent_id,
        IdKind::Agent,
        agentdb_core::ErrorCode::AgentNotFound,
    )?;
    Ok(Json(s.runtime.get_agent(&tid, &aid).await?))
}

async fn delete_agent(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let aid = parse_id_or(
        &agent_id,
        IdKind::Agent,
        agentdb_core::ErrorCode::AgentNotFound,
    )?;
    s.runtime.delete_agent(&tid, &aid).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_session(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<CreateSessionBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let aid = parse_id_or(
        &agent_id,
        IdKind::Agent,
        agentdb_core::ErrorCode::AgentNotFound,
    )?;
    let preallocated_session_id =
        forwarded_preallocated_session_id(&headers)?.unwrap_or_else(|| Id::new(IdKind::Session));
    let forward_body = serde_json::to_vec(&body).expect("create-session body must serialize");
    let mut extra_forward_headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(preallocated_session_id.as_str()) {
        extra_forward_headers.insert(HDR_PREALLOCATED_SESSION_ID, value);
    }
    let route_headers = match guard_routing_key_request(
        &s,
        RoutingKeyGuardRequest {
            tenant_id: &tid,
            routing_key: Some(preallocated_session_id.as_str()),
            original_uri: &original_uri,
            headers: &headers,
            method: Method::POST,
            body: Some(forward_body),
            extra_forward_headers,
        },
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let runtime = s.runtime.clone();
    let create_tid = tid.clone();
    let sess = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime
            .create_session_with_id(
                NewSession {
                    tenant_id: create_tid,
                    agent_id: aid,
                    alias: body.alias,
                },
                preallocated_session_id,
            )
            .await
    })
    .await?;
    record_session_locator(&s, &tid, &sess.session_id, &route_headers).await?;
    let backpressure = s.runtime.backpressure_for_tenant(&tid).await?;
    Ok(with_headers(
        json_with_backpressure(&s, StatusCode::CREATED, Json(sess), backpressure),
        route_headers,
    ))
}

async fn get_session(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let route_headers =
        match guard_session_request(&s, &tid, &sid, &original_uri, &headers, Method::GET, None)
            .await
        {
            Ok(headers) => headers,
            Err(response) => return Ok(response),
        };
    let runtime = s.runtime.clone();
    let session = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime.get_session(&tid, &sid).await
    })
    .await?;
    Ok(with_headers(Json(session).into_response(), route_headers))
}

async fn delete_session(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::DELETE,
        None,
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let runtime = s.runtime.clone();
    let delete_tid = tid.clone();
    let delete_sid = sid.clone();
    with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime.delete_session(&delete_tid, &delete_sid).await
    })
    .await?;
    delete_session_locator(&s, &sid).await?;
    Ok(with_headers(
        StatusCode::NO_CONTENT.into_response(),
        route_headers,
    ))
}

async fn get_session_storage(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let route_headers =
        match guard_session_request(&s, &tid, &sid, &original_uri, &headers, Method::GET, None)
            .await
        {
            Ok(headers) => headers,
            Err(response) => return Ok(response),
        };
    let runtime = s.runtime.clone();
    let storage = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime.get_session_storage(&tid, &sid).await
    })
    .await?;
    Ok(with_headers(Json(storage).into_response(), route_headers))
}

async fn get_tenant_storage_summary(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<agentdb_core::TenantStorageSummary>, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    Ok(Json(s.runtime.tenant_storage_summary(&tid).await?))
}

async fn transition_session_lifecycle(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<TransitionSessionLifecycleBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let route_headers = match guard_session_request_lazy_body(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(|| serde_json::to_vec(&body).expect("lifecycle body must serialize")),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let target = match body.target.to_ascii_lowercase().as_str() {
        "warm" => agentdb_core::Lifecycle::Warm,
        "cold" => agentdb_core::Lifecycle::Cold,
        "archive" => agentdb_core::Lifecycle::Archive,
        "derivative_only" => agentdb_core::Lifecycle::DerivativeOnly,
        other => {
            return Err(ApiError::from(agentdb_core::Error::new(
                ErrorCode::PolicyDenied,
                format!("unknown lifecycle target: {other}"),
            )))
        }
    };
    let runtime = s.runtime.clone();
    let session = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime
            .transition_session_lifecycle(&tid, &sid, target)
            .await
    })
    .await?;
    Ok(with_headers(Json(session).into_response(), route_headers))
}

async fn restore_session_storage(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<RestoreSessionStorageBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let route_headers = match guard_session_request_lazy_body(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(|| serde_json::to_vec(&body).expect("restore body must serialize")),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let target = match body.target.to_ascii_lowercase().as_str() {
        "warm" => agentdb_core::Lifecycle::Warm,
        "cold" => agentdb_core::Lifecycle::Cold,
        other => {
            return Err(ApiError::from(agentdb_core::Error::new(
                ErrorCode::PolicyDenied,
                format!("unknown restore target: {other}"),
            )))
        }
    };
    let runtime = s.runtime.clone();
    let session = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime
            .restore_session_from_archive(&tid, &sid, target)
            .await
    })
    .await?;
    Ok(with_headers(Json(session).into_response(), route_headers))
}

async fn append_event(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<AppendBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let aid = parse_id_or(
        &body.agent_id,
        IdKind::Agent,
        agentdb_core::ErrorCode::AgentNotFound,
    )?;
    let route_headers = match guard_session_request_lazy_body(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(|| serde_json::to_vec(&body).expect("append body must serialize")),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let idem_key = idempotency_key(&headers);

    let runtime = s.runtime.clone();
    let append_tid = tid.clone();
    let append_sid = sid.clone();
    let (resp, backpressure) =
        with_route_tablet_context(runtime.clone(), &route_headers, async move {
            runtime
                .append_with_backpressure(AppendRequest {
                    tenant_id: append_tid,
                    agent_id: aid,
                    session_id: append_sid,
                    kind: body.kind,
                    payload: body.payload,
                    artifact_refs: body.artifact_refs,
                    source_refs: body.source_refs,
                    idempotency_key: idem_key,
                })
                .await
        })
        .await?;
    Ok(with_headers(
        json_with_backpressure(&s, StatusCode::OK, Json(resp), backpressure),
        route_headers,
    ))
}

async fn append_and_fetch(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<AppendAndFetchBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let aid = parse_id_or(
        &body.agent_id,
        IdKind::Agent,
        agentdb_core::ErrorCode::AgentNotFound,
    )?;
    let route_headers = match guard_session_request_lazy_body(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(|| serde_json::to_vec(&body).expect("append-and-fetch body must serialize")),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let idem_key = idempotency_key(&headers);

    let runtime = s.runtime.clone();
    let append_tid = tid.clone();
    let append_sid = sid.clone();
    let (resp, backpressure) =
        with_route_tablet_context(runtime.clone(), &route_headers, async move {
            runtime
                .append_and_fetch_with_backpressure(
                    AppendRequest {
                        tenant_id: append_tid,
                        agent_id: aid,
                        session_id: append_sid,
                        kind: body.kind,
                        payload: body.payload,
                        artifact_refs: body.artifact_refs,
                        source_refs: body.source_refs,
                        idempotency_key: idem_key,
                    },
                    body.last,
                    body.max_bytes,
                )
                .await
        })
        .await?;
    Ok(with_headers(
        json_with_backpressure(&s, StatusCode::OK, Json(resp), backpressure),
        route_headers,
    ))
}

async fn fetch_events(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Query(q): Query<FetchQuery>,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let route_headers =
        match guard_session_request(&s, &tid, &sid, &original_uri, &headers, Method::GET, None)
            .await
        {
            Ok(headers) => headers,
            Err(response) => return Ok(response),
        };
    let runtime = s.runtime.clone();
    let events = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        if q.latest {
            runtime.fetch_recent(tid, sid, q.limit).await
        } else {
            runtime
                .fetch_from_seq(FetchRequest {
                    tenant_id: tid,
                    session_id: sid,
                    from_seq: q.from_seq,
                    limit: q.limit,
                })
                .await
        }
    })
    .await?;
    Ok(with_headers(Json(events).into_response(), route_headers))
}

async fn fork_session(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<ForkSessionBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(&session_id, IdKind::Session, ErrorCode::SessionNotFound)?;
    let route_headers = match guard_session_request_lazy_body(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(|| serde_json::to_vec(&body).expect("fork body must serialize")),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let runtime = s.runtime.clone();
    let fork_tid = tid.clone();
    let fork_sid = sid.clone();
    let child = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime
            .fork_session(ForkSessionRequest {
                tenant_id: fork_tid,
                session_id: fork_sid,
                at_seq: body.at_seq,
                alias: body.alias,
            })
            .await
    })
    .await?;
    record_session_locator(&s, &tid, &child.session_id, &route_headers).await?;
    Ok(with_headers(
        (StatusCode::CREATED, Json(child)).into_response(),
        route_headers,
    ))
}

async fn replay_session(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Query(q): Query<ReplayQuery>,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(&session_id, IdKind::Session, ErrorCode::SessionNotFound)?;
    let route_headers =
        match guard_session_request(&s, &tid, &sid, &original_uri, &headers, Method::GET, None)
            .await
        {
            Ok(headers) => headers,
            Err(response) => return Ok(response),
        };
    let runtime = s.runtime.clone();
    let replay_tid = tid.clone();
    let replay_sid = sid.clone();
    let mut replay = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime.replay_session(&replay_tid, &replay_sid).await
    })
    .await?;
    if let Some(from_seq) = q.from_seq {
        replay
            .events
            .retain(|event| event.session_id.as_str() != sid.as_str() || event.seq >= from_seq);
    }
    replay.events.truncate(q.limit.clamp(1, 1000));
    Ok(with_headers(Json(replay).into_response(), route_headers))
}

async fn subscribe_session(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Query(q): Query<FetchQuery>,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(&session_id, IdKind::Session, ErrorCode::SessionNotFound)?;
    let route_headers =
        match guard_session_request(&s, &tid, &sid, &original_uri, &headers, Method::GET, None)
            .await
        {
            Ok(headers) => headers,
            Err(response) => return Ok(response),
        };
    let runtime = s.runtime.clone();
    let events = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime
            .fetch_from_seq(FetchRequest {
                tenant_id: tid,
                session_id: sid,
                from_seq: q.from_seq,
                limit: q.limit.clamp(1, 1000),
            })
            .await
    })
    .await?;
    Ok(with_headers(
        sse_response(events.into_iter().map(|event| ("event", event))),
        route_headers,
    ))
}

async fn build_context(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<ContextBody>,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(&session_id, IdKind::Session, ErrorCode::SessionNotFound)?;
    let route_headers = match guard_session_request_lazy_body(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(|| serde_json::to_vec(&body).expect("context body must serialize")),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    let record_query = body
        .record_query
        .map(|q| search_body_to_request(tid.clone(), q));
    let runtime = s.runtime.clone();
    let context = with_route_tablet_context(runtime.clone(), &route_headers, async move {
        runtime
            .build_context(ContextRequest {
                tenant_id: tid,
                session_id: sid,
                max_events: body.max_events.clamp(0, 1000),
                max_bytes: body.max_bytes,
                include: parse_context_includes(&body.include),
                record_query,
            })
            .await
    })
    .await?;
    Ok(with_headers(Json(context).into_response(), route_headers))
}

async fn watch(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(tenant_id): Path<String>,
    Query(q): Query<WatchQuery>,
) -> Result<Json<Vec<ChangefeedItem>>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let scope = match q.scope.as_str() {
        "session" => WatchScope::Session,
        "agent" => WatchScope::Agent,
        _ => WatchScope::Tenant,
    };
    let session_id = q
        .session_id
        .as_deref()
        .map(|raw| parse_id_or(raw, IdKind::Session, ErrorCode::SessionNotFound))
        .transpose()?;
    let agent_id = q
        .agent_id
        .as_deref()
        .map(|raw| parse_id_or(raw, IdKind::Agent, ErrorCode::AgentNotFound))
        .transpose()?;
    let runtime = s.runtime.clone();
    let items = with_route_tablet_context(runtime.clone(), &headers, async move {
        runtime
            .watch(WatchRequest {
                tenant_id: tid,
                scope,
                session_id,
                agent_id,
                from_cursor: q.from_cursor,
                limit: q.limit.clamp(1, 1000),
            })
            .await
    })
    .await?;
    Ok(Json(items))
}

/// `POST /api/tenants/:tenant_id/artifacts`
///
/// Body: raw bytes (the artifact content). NO multipart, NO base64 — content
/// type is taken straight off the request `Content-Type` header.
///
/// Headers:
/// - `Content-Type` — recorded on the artifact metadata. Absent → None.
/// - `X-Sha256` (optional, hex) — caller-asserted digest. Mismatch with the
///   server's computed digest yields 422 HASH_MISMATCH.
/// - `Idempotency-Key` (optional) — accepted but currently a no-op. Artifact
///   idempotency is V2.1+ work; we don't fail just because the header was
///   sent so existing clients keep working.
///
/// Errors map per `crate::errors::status_for`:
/// - body > 16 MB → 422 ARTIFACT_TOO_LARGE
/// - X-Sha256 mismatch → 422 HASH_MISMATCH
/// - tenant not found → 404 TENANT_NOT_FOUND
async fn create_artifact(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    // Bad path-id collapses to a structured 404. There's no INVALID_INPUT
    // ErrorCode in V2 — a syntactically broken id is indistinguishable from
    // a non-existent one to the caller, so TenantNotFound is the right
    // outcome (matches the rest of the V2.0 routes).
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let expected_sha256 = headers
        .get(SHA256_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    // Idempotency-Key on artifact POST is intentionally a no-op for V2.1.
    // Read it just to avoid surprising clients that send it; a future
    // milestone will route it through an idempotency store like the
    // append path does.
    let _ = idempotency_key(&headers);

    let art = s
        .runtime
        .create_artifact(
            NewArtifact {
                tenant_id: tid.clone(),
                content_type,
                expected_sha256,
            },
            body.to_vec(),
        )
        .await?;
    let backpressure = s.runtime.backpressure_for_tenant(&tid).await?;
    Ok(json_with_backpressure(
        &s,
        StatusCode::CREATED,
        Json(art),
        backpressure,
    ))
}

/// `GET /api/tenants/:tenant_id/artifacts/:artifact_id`
///
/// Returns the artifact metadata as JSON. ArtifactNotFound (which collapses
/// "unknown id", "soft-deleted", "wrong tenant") → 404.
async fn get_artifact(
    State(s): State<AppState>,
    Path((tenant_id, artifact_id)): Path<(String, String)>,
) -> Result<Json<Artifact>, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let aid = parse_id_or(
        &artifact_id,
        IdKind::Artifact,
        agentdb_core::ErrorCode::ArtifactNotFound,
    )?;
    let art = s.runtime.get_artifact(&tid, &aid).await?;
    Ok(Json(art))
}

/// `GET /api/tenants/:tenant_id/artifacts/:artifact_id/blob`
///
/// Returns the raw bytes with `Content-Type` set from the artifact metadata
/// (or `application/octet-stream` if absent) and `Content-Length` from the
/// stored byte count. V2.1 reads the whole blob into memory; streaming is
/// V2.4 work.
async fn get_artifact_blob(
    State(s): State<AppState>,
    Path((tenant_id, artifact_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let tid = parse_id_or(
        &tenant_id,
        IdKind::Tenant,
        agentdb_core::ErrorCode::TenantNotFound,
    )?;
    let aid = parse_id_or(
        &artifact_id,
        IdKind::Artifact,
        agentdb_core::ErrorCode::ArtifactNotFound,
    )?;

    // Fetch metadata first so we can set Content-Type and Content-Length on
    // the response. If the artifact doesn't exist this errors out before we
    // ever ask the content backend for bytes.
    let art = s.runtime.get_artifact(&tid, &aid).await?;
    let bytes = s.runtime.get_artifact_blob(&tid, &aid).await?;

    let content_type = art
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream")
        .to_string();
    let mut resp = (StatusCode::OK, bytes).into_response();
    let h = resp.headers_mut();
    if let Ok(v) = content_type.parse() {
        h.insert(header::CONTENT_TYPE, v);
    }
    if let Ok(v) = art.bytes.to_string().parse() {
        h.insert(header::CONTENT_LENGTH, v);
    }
    Ok(resp)
}

fn idempotency_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get(IDEMPOTENCY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn json_with_backpressure<T: serde::Serialize>(
    state: &AppState,
    status: StatusCode,
    body: Json<T>,
    backpressure: agentdb_core::BackpressureReport,
) -> Response {
    let mut resp = if let Some(profile) = state.api_json_response_profile.as_deref() {
        let started = Instant::now();
        let bytes = match serde_json::to_vec(&body.0) {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::error!(error = %err, "failed to serialize json response");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                    Body::from("failed to serialize json response"),
                )
                    .into_response();
            }
        };
        profile.record(bytes.len(), started.elapsed().as_nanos());
        (
            status,
            [(header::CONTENT_TYPE, "application/json")],
            Bytes::from(bytes),
        )
            .into_response()
    } else {
        (status, body).into_response()
    };
    if let Some(value) = backpressure.header_value() {
        if let Ok(value) = HeaderValue::from_str(&value) {
            resp.headers_mut().insert(BACKPRESSURE_HEADER, value);
        }
    }
    resp
}

async fn with_route_tablet_context<F, T>(
    runtime: Arc<agentdb_core::runtime::Runtime>,
    headers: &HeaderMap,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    if let Some(tablet_id) = route_tablet_id(headers) {
        runtime.with_tablet_context(tablet_id, future).await
    } else {
        future.await
    }
}

fn route_tablet_id(headers: &HeaderMap) -> Option<i64> {
    headers
        .get(HDR_TABLET_ID)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
}

fn route_shard_id(headers: &HeaderMap) -> Option<i32> {
    headers
        .get(HDR_SHARD_ID)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i32>().ok())
}

fn forwarded_preallocated_session_id(headers: &HeaderMap) -> Result<Option<Id>, ApiError> {
    if !headers.contains_key(HDR_FORWARDED_BY) {
        return Ok(None);
    }
    let Some(raw) = headers
        .get(HDR_PREALLOCATED_SESSION_ID)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    Ok(Some(parse_id_or(
        raw,
        IdKind::Session,
        ErrorCode::SessionNotFound,
    )?))
}

async fn record_session_locator(
    state: &AppState,
    tenant_id: &Id,
    session_id: &Id,
    route_headers: &HeaderMap,
) -> Result<(), ApiError> {
    let Some(meta) = state.meta.as_ref() else {
        return Ok(());
    };
    let (Some(tablet_id), Some(shard_id)) = (
        route_tablet_id(route_headers),
        route_shard_id(route_headers),
    ) else {
        return Ok(());
    };
    let tenant = tenants::get_or_create(meta.pool(), tenant_id.as_str(), now_ms_i64())
        .await
        .map_err(internal_error)?;
    session_locators::upsert(
        meta.pool(),
        session_id.as_str(),
        tenant.internal_id,
        tenant_id.as_str(),
        Some(tablet_id),
        shard_id,
        now_ms_i64(),
    )
    .await
    .map_err(internal_error)?;
    Ok(())
}

async fn delete_session_locator(state: &AppState, session_id: &Id) -> Result<(), ApiError> {
    let Some(meta) = state.meta.as_ref() else {
        return Ok(());
    };
    session_locators::delete(meta.pool(), session_id.as_str())
        .await
        .map_err(internal_error)?;
    Ok(())
}

async fn delete_tenant_session_locators(state: &AppState, tenant_id: &Id) -> Result<(), ApiError> {
    let Some(meta) = state.meta.as_ref() else {
        return Ok(());
    };
    session_locators::delete_by_tenant_external(meta.pool(), tenant_id.as_str())
        .await
        .map_err(internal_error)?;
    Ok(())
}

struct RoutingKeyGuardRequest<'a> {
    tenant_id: &'a Id,
    routing_key: Option<&'a str>,
    original_uri: &'a axum::http::Uri,
    headers: &'a HeaderMap,
    method: Method,
    body: Option<Vec<u8>>,
    extra_forward_headers: HeaderMap,
}

async fn guard_routing_key_request(
    state: &AppState,
    request: RoutingKeyGuardRequest<'_>,
) -> Result<HeaderMap, Response> {
    let path_and_query = request
        .original_uri
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or_else(|| request.original_uri.path());
    match state.routing.guard_routing_key(
        request.tenant_id.as_str(),
        request.routing_key,
        path_and_query,
        request.headers,
    ) {
        RouteGuardOutcome::ServeLocal(headers) => Ok(headers),
        RouteGuardOutcome::Forward(mut forward) => {
            forward.headers.extend(request.extra_forward_headers);
            Err(execute_forward(state, forward, request.method, request.body).await)
        }
        RouteGuardOutcome::Reject(rejection) => Err(route_rejection_response(&rejection)),
    }
}

async fn guard_session_request(
    state: &AppState,
    tenant_id: &Id,
    session_id: &Id,
    original_uri: &axum::http::Uri,
    headers: &HeaderMap,
    method: Method,
    body: Option<Vec<u8>>,
) -> Result<HeaderMap, Response> {
    guard_session_request_lazy_body(
        state,
        tenant_id,
        session_id,
        original_uri,
        headers,
        method,
        body.map(|body| move || body),
    )
    .await
}

async fn guard_session_request_lazy_body<F>(
    state: &AppState,
    tenant_id: &Id,
    session_id: &Id,
    original_uri: &axum::http::Uri,
    headers: &HeaderMap,
    method: Method,
    body: Option<F>,
) -> Result<HeaderMap, Response>
where
    F: FnOnce() -> Vec<u8>,
{
    let path_and_query = original_uri
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or_else(|| original_uri.path());
    match state.routing.guard_session_id(
        tenant_id.as_str(),
        session_id.as_str(),
        path_and_query,
        headers,
    ) {
        RouteGuardOutcome::ServeLocal(headers) => Ok(headers),
        RouteGuardOutcome::Forward(forward) => {
            Err(execute_forward(state, forward, method, body.map(|build| build())).await)
        }
        RouteGuardOutcome::Reject(rejection) => Err(route_rejection_response(&rejection)),
    }
}

async fn execute_forward(
    state: &AppState,
    forward: ForwardRequest,
    method: Method,
    body: Option<Vec<u8>>,
) -> Response {
    let method = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .expect("HTTP method must be valid for reqwest");
    let mut request = state
        .proxy
        .request(method, &forward.uri)
        .headers(forward.headers.clone())
        .timeout(Duration::from_millis(state.proxy_policy.timeout_ms.max(1)));
    if let Some(body) = body {
        request = request.body(body);
    }
    match request.send().await {
        Ok(response) => {
            proxy_response(response, forward, state.proxy_policy.max_response_bytes).await
        }
        Err(err) => route_forward_failed_response(&forward, &err),
    }
}

async fn proxy_response(
    response: reqwest::Response,
    forward: ForwardRequest,
    max_response_bytes: u64,
) -> Response {
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let headers = response.headers().clone();
    if response
        .content_length()
        .is_some_and(|content_length| content_length > max_response_bytes)
    {
        return route_forward_response_too_large(&forward, max_response_bytes);
    }
    let body = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            return route_forward_failed_response(&forward, &err);
        }
    };
    if body.len() as u64 > max_response_bytes {
        return route_forward_response_too_large(&forward, max_response_bytes);
    }
    let mut out = (status, body).into_response();
    out.headers_mut().extend(headers);
    out
}

fn route_forward_failed_response(forward: &ForwardRequest, err: &reqwest::Error) -> Response {
    let code = if err.is_timeout() {
        "ROUTE_FORWARD_TIMEOUT"
    } else {
        "ROUTE_FORWARD_FAILED"
    };
    let body = serde_json::json!({
        "error": {
            "code": code,
            "message": format!("failed to forward request to owner node: {err}"),
            "retryable": true,
            "suggestion": "retry against the owner node advertised in the routing headers"
        }
    });
    let mut response = (StatusCode::BAD_GATEWAY, Json(body)).into_response();
    response.headers_mut().extend(forward.headers.clone());
    response
        .headers_mut()
        .insert(HDR_ROUTE_REASON, HeaderValue::from_static(code));
    response
}

fn route_forward_response_too_large(forward: &ForwardRequest, max_response_bytes: u64) -> Response {
    let body = serde_json::json!({
        "error": {
            "code": "ROUTE_FORWARD_RESPONSE_TOO_LARGE",
            "message": format!("forwarded owner response exceeded proxy limit of {max_response_bytes} bytes"),
            "retryable": false,
            "suggestion": "increase route proxy response limits or request a smaller response"
        }
    });
    let mut response = (StatusCode::BAD_GATEWAY, Json(body)).into_response();
    response.headers_mut().extend(forward.headers.clone());
    response.headers_mut().insert(
        HDR_ROUTE_REASON,
        HeaderValue::from_static("ROUTE_FORWARD_RESPONSE_TOO_LARGE"),
    );
    response
}

#[allow(clippy::result_large_err)]
fn validate_forwarded_mutation(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    state
        .routing
        .validate_forwarded_epoch(headers)
        .map_err(|rejection| route_rejection_response(&rejection))
}

fn with_headers(mut response: Response, headers: HeaderMap) -> Response {
    response.headers_mut().extend(headers);
    response
}

/// `POST /api/tenants/:tenant_id/records`
///
/// Body shape mirrors `agentdb_core::NewRecord` minus `tenant_id` (which the
/// path supplies). Optional fields default sensibly via serde; if `policy` is
/// omitted the runtime applies `IndexingPolicy::default()`.
async fn create_record(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<CreateRecordBody>,
) -> Result<Response, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let written = s
        .runtime
        .write_record_with_commit(NewRecord {
            tenant_id: tid.clone(),
            namespace: body.namespace,
            scope: body.scope,
            text: body.text,
            metadata: body.metadata,
            tags: body.tags,
            source: body.source,
            embedding: body.embedding,
            policy: body.policy.unwrap_or_default(),
            expires_at_ms: body.expires_at_ms,
        })
        .await?;
    let backpressure = s.runtime.backpressure_for_tenant(&tid).await?;
    let mut resp =
        json_with_backpressure(&s, StatusCode::CREATED, Json(written.record), backpressure);
    if let Ok(value) = HeaderValue::from_str(&written.commit_token.seq.to_string()) {
        resp.headers_mut().insert(RECORD_COMMIT_SEQ_HEADER, value);
    }
    Ok(resp)
}

/// `GET /api/tenants/:tenant_id/records/:record_id`
///
/// Returns the record. RecordNotFound (collapsing "unknown id",
/// "soft-deleted", "expired", "wrong tenant") → 404.
async fn get_record(
    State(s): State<AppState>,
    Path((tenant_id, record_id)): Path<(String, String)>,
    Query(q): Query<IncludeDeletedQuery>,
) -> Result<Json<SearchableRecord>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let rid = parse_id_or(&record_id, IdKind::Record, ErrorCode::RecordNotFound)?;
    let rec = s
        .runtime
        .get_record_with_options(&tid, &rid, q.include_deleted)
        .await?;
    Ok(Json(rec))
}

/// `GET /api/tenants/:tenant_id/records/search`
///
/// Query params (see [`crate::dto::SearchRecordsQuery`] for the full list):
/// - `text` (optional)
/// - `namespace` (repeatable; empty list = tenant root)
/// - `tag` (repeatable; AND-semantics)
/// - `source_session` + `source_seq`, `source_artifact`, `source_record`,
///   `source_skill`, `source_event` (post-filter on `SourceRef`)
/// - `limit` (default 20, capped at 100)
///
/// `embedding` is intentionally NOT exposed via GET — vector search uses
/// `POST /records/search` with a JSON body.
async fn search_records(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<Json<SearchRecordsResponse>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;

    // Parse the query string by hand because the default `Query` extractor
    // sits on top of serde_urlencoded, which collapses repeated keys (we
    // need `namespace=a&namespace=b` to round-trip into a Vec).
    let parsed = parse_search_query(raw.as_deref().unwrap_or(""));

    let limit = parsed.limit.clamp(1, 100);

    // Build the source filter from the populated `source_*` query params.
    // We only construct one if at least one of these is set; an absent
    // filter means "no source-based post-filter".
    let source_filter = build_source_filter(&parsed)?;

    let req = SearchRequest {
        tenant_id: tid,
        namespace: parsed.namespace,
        sharding_key: parsed.sharding_key,
        text: parsed.text,
        // Vector search via GET is intentionally unsupported in V2.2b.
        // See route doc comment.
        embedding: None,
        tags: parsed.tags,
        source_filter,
        limit,
        min_visible_record_commit_seq: parsed.min_visible_record_commit_seq,
    };
    let records = s.runtime.search_records(req).await?;
    Ok(Json(SearchRecordsResponse { records }))
}

/// `POST /api/tenants/:tenant_id/records/search`
///
/// JSON search form for the full V2.2 record-search surface. This is the route
/// callers use for vector search because embeddings are request-body data, not
/// URL query data.
async fn search_records_json(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<SearchRecordsBody>,
) -> Result<Json<SearchRecordsResponse>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let req = search_body_to_request(tid, body);
    let records = s.runtime.search_records(req).await?;
    Ok(Json(SearchRecordsResponse { records }))
}

/// `DELETE /api/tenants/:tenant_id/records/:record_id`
///
/// Soft-delete. RecordNotFound (already deleted, unknown, wrong tenant) → 404.
/// Successful delete returns 204 with an empty body.
async fn delete_record(
    State(s): State<AppState>,
    Path((tenant_id, record_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let rid = parse_id_or(&record_id, IdKind::Record, ErrorCode::RecordNotFound)?;
    s.runtime.delete_record(&tid, &rid).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_skill(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CreateSkillBody>,
) -> Result<(StatusCode, Json<Skill>), ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let skill = s
        .runtime
        .register_skill(
            NewSkill {
                tenant_id: tid,
                name: body.name,
                version: body.version,
                manifest: body.manifest,
                artifact_refs: body.artifact_refs,
            },
            idempotency_key(&headers),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(skill)))
}

async fn get_skill(
    State(s): State<AppState>,
    Path((tenant_id, skill_id)): Path<(String, String)>,
) -> Result<Json<Skill>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(&skill_id, IdKind::Skill, ErrorCode::SkillNotFound)?;
    Ok(Json(s.runtime.get_skill(&tid, &sid).await?))
}

async fn delete_skill(
    State(s): State<AppState>,
    Path((tenant_id, skill_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(&skill_id, IdKind::Skill, ErrorCode::SkillNotFound)?;
    s.runtime.delete_skill(&tid, &sid).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn bind_skill(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<BindSkillBody>,
) -> Result<(StatusCode, Json<AgentSkillBinding>), ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let aid = parse_id_or(&agent_id, IdKind::Agent, ErrorCode::AgentNotFound)?;
    let sid = parse_id_or(&body.skill_id, IdKind::Skill, ErrorCode::SkillNotFound)?;
    let binding = s
        .runtime
        .bind_skill(BindSkillRequest {
            tenant_id: tid,
            agent_id: aid,
            skill_id: sid,
            idempotency_key: idempotency_key(&headers),
        })
        .await?;
    Ok((StatusCode::CREATED, Json(binding)))
}

async fn list_agent_skills(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
) -> Result<Json<ListAgentSkillsResponse>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let aid = parse_id_or(&agent_id, IdKind::Agent, ErrorCode::AgentNotFound)?;
    let skills = s.runtime.list_agent_skills(&tid, &aid).await?;
    Ok(Json(ListAgentSkillsResponse { skills }))
}

async fn run_lifecycle_sweep(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<SweepBody>,
) -> Result<Json<agentdb_core::LifecycleSweepReport>, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let report = s
        .runtime
        .sweep_lifecycle_for_tenant_at(&tid, body.now_ms.unwrap_or_else(current_ms))
        .await?;
    Ok(Json(report))
}

async fn run_gc_sweep(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<SweepBody>,
) -> Result<Json<agentdb_core::GcSweepReport>, ApiError> {
    reject_if_draining(&s)?;
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, ErrorCode::TenantNotFound)?;
    let report = s
        .runtime
        .gc_sweep_for_tenant_at(&tid, body.now_ms.unwrap_or_else(current_ms))
        .await?;
    Ok(Json(report))
}

fn current_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn run_policy_worker(
    State(s): State<AppState>,
    Json(body): Json<PolicyWorkerRunBody>,
) -> Result<Json<agentdb_core::PolicyWorkerReport>, ApiError> {
    reject_if_draining(&s)?;
    let max_tenants = body.max_tenants.clamp(1, 1024);
    let report = s
        .runtime
        .run_policy_worker_once_at(body.now_ms.unwrap_or_else(current_ms), max_tenants)
        .await?;
    Ok(Json(report))
}

/// Hand-rolled query-string parser for the search route. Handles the
/// repeated-key cases (`namespace`, `tag`) that serde_urlencoded can't
/// express; everything else is a single-occurrence `key=value` pair.
struct ParsedSearchQuery {
    text: Option<String>,
    namespace: Vec<String>,
    sharding_key: Option<String>,
    tags: Vec<String>,
    source_session: Option<String>,
    source_seq: Option<u64>,
    source_artifact: Option<String>,
    source_record: Option<String>,
    source_skill: Option<String>,
    source_event: Option<String>,
    limit: usize,
    min_visible_record_commit_seq: Option<u64>,
}

fn parse_search_query(raw: &str) -> ParsedSearchQuery {
    let mut out = ParsedSearchQuery {
        text: None,
        namespace: vec![],
        sharding_key: None,
        tags: vec![],
        source_session: None,
        source_seq: None,
        source_artifact: None,
        source_record: None,
        source_skill: None,
        source_event: None,
        limit: 20,
        min_visible_record_commit_seq: None,
    };
    if raw.is_empty() {
        return out;
    }
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = url_decode(k);
        let val = url_decode(v);
        match key.as_str() {
            "text" => out.text = Some(val),
            "namespace" => out.namespace.push(val),
            "sharding_key" => out.sharding_key = Some(val),
            "tag" => out.tags.push(val),
            "source_session" => out.source_session = Some(val),
            "source_seq" => out.source_seq = val.parse().ok(),
            "source_artifact" => out.source_artifact = Some(val),
            "source_record" => out.source_record = Some(val),
            "source_skill" => out.source_skill = Some(val),
            "source_event" => out.source_event = Some(val),
            "limit" => {
                if let Ok(n) = val.parse() {
                    out.limit = n;
                }
            }
            "min_visible_record_commit_seq" => {
                out.min_visible_record_commit_seq = val.parse().ok();
            }
            _ => {} // unknown keys are silently ignored
        }
    }
    out
}

/// Lightweight URL-decoder for query string values: handles `+` → space and
/// `%XX` percent-encoding. Falls back to the raw input on malformed escapes
/// (matches what serde_urlencoded does — we don't want to 400 a search just
/// because a stray `%` slipped through).
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'+' {
            out.push(b' ');
            i += 1;
        } else if b == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
            } else {
                out.push(b);
                i += 1;
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|e| {
        // Lossily recover so invalid UTF-8 doesn't 500 the request.
        String::from_utf8_lossy(e.as_bytes()).into_owned()
    })
}

fn build_source_filter(q: &ParsedSearchQuery) -> Result<Option<SourceRef>, ApiError> {
    // Skip building entirely when nothing is set.
    let any_set = q.source_session.is_some()
        || q.source_seq.is_some()
        || q.source_artifact.is_some()
        || q.source_record.is_some()
        || q.source_skill.is_some()
        || q.source_event.is_some();
    if !any_set {
        return Ok(None);
    }
    let session_id = parse_opt_id(q.source_session.as_deref(), IdKind::Session)?;
    let artifact_id = parse_opt_id(q.source_artifact.as_deref(), IdKind::Artifact)?;
    let record_id = parse_opt_id(q.source_record.as_deref(), IdKind::Record)?;
    let skill_id = parse_opt_id(q.source_skill.as_deref(), IdKind::Skill)?;
    let event_id = parse_opt_id(q.source_event.as_deref(), IdKind::Event)?;
    Ok(Some(SourceRef {
        session_id,
        seq: q.source_seq,
        event_id,
        artifact_id,
        record_id,
        skill_id,
    }))
}

fn search_body_to_request(tenant_id: Id, body: SearchRecordsBody) -> SearchRequest {
    SearchRequest {
        tenant_id,
        namespace: body.namespace,
        sharding_key: body.sharding_key,
        text: body.text,
        embedding: body.embedding,
        tags: body.tags,
        source_filter: body.source,
        limit: body.limit.clamp(1, 100),
        min_visible_record_commit_seq: body.min_visible_record_commit_seq,
    }
}

fn parse_context_includes(raw: &[String]) -> Vec<ContextInclude> {
    let mut out = Vec::new();
    for item in raw {
        match item.as_str() {
            "recent_events" => out.push(ContextInclude::RecentEvents),
            "records" | "search_records" => out.push(ContextInclude::Records),
            "skills" | "skill_instructions" => out.push(ContextInclude::Skills),
            "policy" => out.push(ContextInclude::Policy),
            _ => {}
        }
    }
    if out.is_empty() {
        out.push(ContextInclude::RecentEvents);
    }
    out
}

fn sse_response<T: Serialize>(events: impl Iterator<Item = (&'static str, T)>) -> Response {
    let mut body = String::new();
    for (event, data) in events {
        body.push_str("event: ");
        body.push_str(event);
        body.push('\n');
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&data).expect("sse payload serializes"));
        body.push_str("\n\n");
    }
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        )],
        body,
    )
        .into_response()
}

/// Parse an optional id string into an Id of the given kind. Bad parses
/// collapse to `None` rather than erroring — a malformed source filter id
/// matches no record by definition, which is the desired behavior. (We
/// don't want a typo'd source_record to 400 a search.)
fn parse_opt_id(s: Option<&str>, kind: IdKind) -> Result<Option<Id>, ApiError> {
    match s {
        None => Ok(None),
        Some(raw) => Ok(Id::parse_kind(raw, kind).ok()),
    }
}
