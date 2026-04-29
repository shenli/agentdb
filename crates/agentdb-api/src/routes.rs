//! HTTP routes for the V2.0 runtime.
//!
//! Routes are mounted under `/api/...`. Each handler:
//! 1. Parses path params (typed Ids) — bad input becomes a structured 404.
//! 2. Calls the runtime.
//! 3. Returns either a serde-serialized runtime type or an `ApiError`.
//!
//! Wire DTOs live in `crate::dto`. Status codes for `agentdb_core::Error` live
//! in `crate::errors`.

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};

use agentdb_core::{
    runtime::{AppendAndFetchResponse, AppendRequest, AppendResponse, FetchRequest},
    Agent, EventEnvelope, IdKind, NewAgent, NewSession, NewTenant, Session, Tenant,
};

use crate::dto::{
    AppendAndFetchBody, AppendBody, CreateAgentBody, CreateSessionBody, CreateTenantBody,
    FetchQuery,
};
use crate::errors::{parse_id_or, ApiError};
use crate::state::AppState;

const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/tenants", post(create_tenant))
        .route("/api/tenants/:tenant_id", get(get_tenant))
        .route("/api/tenants/:tenant_id/agents", post(create_agent))
        .route(
            "/api/tenants/:tenant_id/agents/:agent_id",
            get(get_agent),
        )
        .route(
            "/api/tenants/:tenant_id/agents/:agent_id/sessions",
            post(create_session),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id",
            get(get_session),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/events",
            post(append_event).get(fetch_events),
        )
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/append-and-fetch",
            post(append_and_fetch),
        )
}

async fn create_tenant(
    State(s): State<AppState>,
    Json(body): Json<CreateTenantBody>,
) -> Result<Json<Tenant>, ApiError> {
    let t = s
        .runtime
        .create_tenant(NewTenant {
            external_key: body.external_key,
            policy: body.policy,
        })
        .await?;
    Ok(Json(t))
}

async fn get_tenant(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    Ok(Json(s.runtime.get_tenant(&tid).await?))
}

async fn create_agent(
    State(s): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<CreateAgentBody>,
) -> Result<Json<Agent>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
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

async fn get_agent(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
) -> Result<Json<Agent>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    let aid = parse_id_or(&agent_id, IdKind::Agent, agentdb_core::ErrorCode::AgentNotFound)?;
    Ok(Json(s.runtime.get_agent(&tid, &aid).await?))
}

async fn create_session(
    State(s): State<AppState>,
    Path((tenant_id, agent_id)): Path<(String, String)>,
    Json(body): Json<CreateSessionBody>,
) -> Result<Json<Session>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    let aid = parse_id_or(&agent_id, IdKind::Agent, agentdb_core::ErrorCode::AgentNotFound)?;
    let sess = s
        .runtime
        .create_session(NewSession {
            tenant_id: tid,
            agent_id: aid,
            alias: body.alias,
        })
        .await?;
    Ok(Json(sess))
}

async fn get_session(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
) -> Result<Json<Session>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    Ok(Json(s.runtime.get_session(&tid, &sid).await?))
}

async fn append_event(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<AppendBody>,
) -> Result<Json<AppendResponse>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let aid = parse_id_or(&body.agent_id, IdKind::Agent, agentdb_core::ErrorCode::AgentNotFound)?;
    let idem_key = idempotency_key(&headers);

    let resp = s
        .runtime
        .append(AppendRequest {
            tenant_id: tid,
            agent_id: aid,
            session_id: sid,
            kind: body.kind,
            payload: body.payload,
            artifact_refs: body.artifact_refs,
            source_refs: body.source_refs,
            idempotency_key: idem_key,
        })
        .await?;
    Ok(Json(resp))
}

async fn append_and_fetch(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<AppendAndFetchBody>,
) -> Result<Json<AppendAndFetchResponse>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let aid = parse_id_or(&body.agent_id, IdKind::Agent, agentdb_core::ErrorCode::AgentNotFound)?;
    let idem_key = idempotency_key(&headers);

    let resp = s
        .runtime
        .append_and_fetch(
            AppendRequest {
                tenant_id: tid,
                agent_id: aid,
                session_id: sid,
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
    Ok(Json(resp))
}

async fn fetch_events(
    State(s): State<AppState>,
    Path((tenant_id, session_id)): Path<(String, String)>,
    Query(q): Query<FetchQuery>,
) -> Result<Json<Vec<EventEnvelope>>, ApiError> {
    let tid = parse_id_or(&tenant_id, IdKind::Tenant, agentdb_core::ErrorCode::TenantNotFound)?;
    let sid = parse_id_or(
        &session_id,
        IdKind::Session,
        agentdb_core::ErrorCode::SessionNotFound,
    )?;
    let events = s
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: tid,
            session_id: sid,
            from_seq: q.from_seq,
            limit: q.limit,
        })
        .await?;
    Ok(Json(events))
}

fn idempotency_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get(IDEMPOTENCY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
