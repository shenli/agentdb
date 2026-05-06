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
    fs,
    path::{Path as FsPath, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
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
use agentdb_meta::{nodes, tablet_ranges, tablet_rebalance, tablets, tenant_policies};

use crate::dto::{
    AppendAndFetchBody, AppendBody, BindSkillBody, ContextBody, CreateAgentBody, CreateRecordBody,
    CreateSessionBody, CreateSkillBody, CreateTenantBody, FetchQuery, ForkSessionBody,
    IncludeDeletedQuery, ListAgentSkillsResponse, PolicyWorkerRunBody, ReplayQuery,
    RestoreSessionStorageBody, SearchRecordsBody, SearchRecordsResponse, SweepBody,
    TransitionSessionLifecycleBody, WatchQuery,
};
use crate::errors::{parse_id_or, ApiError};
use crate::routing_http::{
    route_rejection_response, ForwardRequest, RouteGuardOutcome, HDR_ROUTE_REASON,
};
use crate::state::AppState;

const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";
const SHA256_HEADER: &str = "X-Sha256";
const BACKPRESSURE_HEADER: &str = "X-AgentDB-Backpressure";
const RECORD_COMMIT_SEQ_HEADER: &str = "X-AgentDB-Record-Commit-Seq";
const INTERNAL_AUTH_HEADER: &str = "X-AgentDB-Internal-Token";

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
        .route("/api/admin/routing", get(get_routing_status))
        .route("/api/admin/routing/decision", get(get_routing_decision))
        .route("/api/admin/indexes/records", get(get_record_index_status))
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
    last_heartbeat_ms: i64,
    joined_at_ms: i64,
    availability_zone: Option<String>,
    region: Option<String>,
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
    last_tick_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct CreateBackupBody {
    backup_id: Option<String>,
    directory: Option<String>,
    include_meta: Option<bool>,
    include_runtime_summary: Option<bool>,
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
        Err(response) => return response,
    };
    match ingress.step_wire_message(agentdb_storage::RaftRsWireMessage {
        tablet_id: body.tablet_id,
        from: body.from,
        to: body.to,
        payload,
    }) {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(InternalRaftAccepted {
                status: "accepted",
                tablet_id: body.tablet_id,
                from: body.from,
                to: body.to,
            }),
        )
            .into_response(),
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
        Err(response) => return response,
    };
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
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(InternalRaftAccepted {
                status: "accepted",
                tablet_id: body.tablet_id,
                from: body.from,
                to: body.to,
            }),
        )
            .into_response(),
        Err(err) => internal_raft_error(
            StatusCode::BAD_REQUEST,
            "RAFT_SNAPSHOT_REJECTED",
            err.to_string(),
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
    if presented != Some(expected.as_ref()) {
        return Err(internal_raft_error(
            StatusCode::UNAUTHORIZED,
            "RAFT_INTERNAL_AUTH_FAILED",
            "invalid internal raft RPC token",
        ));
    }
    Ok(())
}

fn decode_base64(value: &str) -> Result<Vec<u8>, Response> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|err| {
            internal_raft_error(
                StatusCode::BAD_REQUEST,
                "RAFT_PAYLOAD_INVALID",
                format!("invalid base64 raft payload: {err}"),
            )
        })
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
            last_tick_at_ms: 0,
        });
    Json(SchedulerStatusResponse {
        leader_node_id: snapshot.leader_node_id,
        leader_epoch: snapshot.leader_epoch,
        leader_expires_at_ms: snapshot.leader_expires_at_ms,
        is_leader: snapshot.is_leader,
        dead_nodes_marked_total: snapshot.dead_nodes_marked_total,
        tablet_reassignments_total: snapshot.tablet_reassignments_total,
        last_tick_at_ms: snapshot.last_tick_at_ms,
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
    let manifest = serde_json::json!({
        "schema_version": 1,
        "backup_id": backup_id,
        "created_at_ms": now_ms,
        "status": "completed",
        "meta_backup_path": meta_backup_path,
        "runtime_summary": runtime_summary,
        "nodes": nodes,
        "tablets": tablets,
        "notes": [
            "This is an online backup manifest.",
            "Runtime summary is observability metadata, not a full runtime data copy.",
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
    Ok(Json(AdminRestoreResponse {
        job_id: format!("job_restore_validate_{}", now_ms_i64()),
        status: "validated",
        dry_run: true,
        manifest_path: body.manifest_path,
        backup_id,
        checks,
    }))
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
                last_heartbeat_ms: node.last_heartbeat_ms,
                joined_at_ms: node.joined_at_ms,
                availability_zone: node.availability_zone,
                region: node.region,
            })
            .collect(),
    ))
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
    nodes::upsert(
        meta.pool(),
        &body.node_id,
        &body.address,
        body.mongo_address.as_deref(),
        &state,
        now_ms,
        body.availability_zone.as_deref(),
        body.region.as_deref(),
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
        last_heartbeat_ms: node.last_heartbeat_ms,
        joined_at_ms: node.joined_at_ms,
        availability_zone: node.availability_zone,
        region: node.region,
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
    tenant_policies::upsert(
        meta.pool(),
        &body.tenant_id,
        &body.placement_class,
        &body.placement_scope_id,
        &body.encryption_class,
        body.key_policy_id.as_deref(),
        body.encryption_policy_version,
        body.policy_version,
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

async fn rebalance_admin_tablets(
    State(s): State<AppState>,
    Json(body): Json<RebalanceTabletsBody>,
) -> Result<Json<tablet_rebalance::RebalanceReport>, ApiError> {
    let meta = meta_required(&s)?;
    let report = tablet_rebalance::run(
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
    Ok(Json(report))
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
    let sess = s
        .runtime
        .create_session(NewSession {
            tenant_id: tid.clone(),
            agent_id: aid,
            alias: body.alias,
        })
        .await?;
    let backpressure = s.runtime.backpressure_for_tenant(&tid).await?;
    Ok(json_with_backpressure(
        StatusCode::CREATED,
        Json(sess),
        backpressure,
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
    Ok(with_headers(
        Json(s.runtime.get_session(&tid, &sid).await?).into_response(),
        route_headers,
    ))
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
    s.runtime.delete_session(&tid, &sid).await?;
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
    Ok(with_headers(
        Json(s.runtime.get_session_storage(&tid, &sid).await?).into_response(),
        route_headers,
    ))
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
    let forward_body = serde_json::to_vec(&body).expect("lifecycle body must serialize");
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(forward_body),
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
    Ok(with_headers(
        Json(
            s.runtime
                .transition_session_lifecycle(&tid, &sid, target)
                .await?,
        )
        .into_response(),
        route_headers,
    ))
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
    let forward_body = serde_json::to_vec(&body).expect("restore body must serialize");
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(forward_body),
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
    Ok(with_headers(
        Json(
            s.runtime
                .restore_session_from_archive(&tid, &sid, target)
                .await?,
        )
        .into_response(),
        route_headers,
    ))
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
    let forward_body = serde_json::to_vec(&body).expect("append body must serialize");
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(forward_body),
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

    let resp = s
        .runtime
        .append(AppendRequest {
            tenant_id: tid.clone(),
            agent_id: aid,
            session_id: sid.clone(),
            kind: body.kind,
            payload: body.payload,
            artifact_refs: body.artifact_refs,
            source_refs: body.source_refs,
            idempotency_key: idem_key,
        })
        .await?;
    let mut backpressure = s.runtime.backpressure_for_tenant(&tid).await?;
    backpressure.merge(s.runtime.backpressure_for_session(&tid, &sid).await?);
    Ok(with_headers(
        json_with_backpressure(StatusCode::OK, Json(resp), backpressure),
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
    let forward_body = serde_json::to_vec(&body).expect("append-and-fetch body must serialize");
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(forward_body),
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

    let resp = s
        .runtime
        .append_and_fetch(
            AppendRequest {
                tenant_id: tid.clone(),
                agent_id: aid,
                session_id: sid.clone(),
                kind: body.kind,
                payload: body.payload,
                artifact_refs: body.artifact_refs,
                source_refs: body.source_refs,
                idempotency_key: idem_key,
            },
            body.last,
            body.max_bytes,
        )
        .await?;
    let mut backpressure = s.runtime.backpressure_for_tenant(&tid).await?;
    backpressure.merge(s.runtime.backpressure_for_session(&tid, &sid).await?);
    Ok(with_headers(
        json_with_backpressure(StatusCode::OK, Json(resp), backpressure),
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
    let events = s
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: tid,
            session_id: sid,
            from_seq: q.from_seq,
            limit: q.limit,
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
    let forward_body = serde_json::to_vec(&body).expect("fork body must serialize");
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(forward_body),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    if let Err(response) = validate_forwarded_mutation(&s, &headers) {
        return Ok(response);
    }
    let child = s
        .runtime
        .fork_session(ForkSessionRequest {
            tenant_id: tid,
            session_id: sid,
            at_seq: body.at_seq,
            alias: body.alias,
        })
        .await?;
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
    let mut replay = s.runtime.replay_session(&tid, &sid).await?;
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
    let events = s
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: tid,
            session_id: sid,
            from_seq: q.from_seq,
            limit: q.limit.clamp(1, 1000),
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
    let forward_body = serde_json::to_vec(&body).expect("context body must serialize");
    let route_headers = match guard_session_request(
        &s,
        &tid,
        &sid,
        &original_uri,
        &headers,
        Method::POST,
        Some(forward_body),
    )
    .await
    {
        Ok(headers) => headers,
        Err(response) => return Ok(response),
    };
    let record_query = body
        .record_query
        .map(|q| search_body_to_request(tid.clone(), q));
    let context = s
        .runtime
        .build_context(ContextRequest {
            tenant_id: tid,
            session_id: sid,
            max_events: body.max_events.clamp(0, 1000),
            max_bytes: body.max_bytes,
            include: parse_context_includes(&body.include),
            record_query,
        })
        .await?;
    Ok(with_headers(Json(context).into_response(), route_headers))
}

async fn watch(
    State(s): State<AppState>,
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
    let items = s
        .runtime
        .watch(WatchRequest {
            tenant_id: tid,
            scope,
            session_id,
            agent_id,
            from_cursor: q.from_cursor,
            limit: q.limit.clamp(1, 1000),
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
    status: StatusCode,
    body: Json<T>,
    backpressure: agentdb_core::BackpressureReport,
) -> Response {
    let mut resp = (status, body).into_response();
    if let Some(value) = backpressure.header_value() {
        if let Ok(value) = HeaderValue::from_str(&value) {
            resp.headers_mut().insert(BACKPRESSURE_HEADER, value);
        }
    }
    resp
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
            Err(execute_forward(state, forward, method, body).await)
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
    let mut resp = json_with_backpressure(StatusCode::CREATED, Json(written.record), backpressure);
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
