use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::auth_middleware::AuthedTenant;
use crate::dto::{AppendAndFetchRequest, AppendAndFetchResponse, EventDto};
use crate::error::{ApiError, ErrorResponse};
use crate::forwarding::{self, PassthroughHeaders, FORWARDED_HEADER};
use crate::AppState;

/// Resolve the current `next_seq` for an external (tenant, session)
/// pair without doing any of the write-path work (admission,
/// quotas, fault injection). Used by subscribe handlers to pin a
/// "live as of T" cursor before they start tailing the bus.
///
/// Routing matches `append_and_fetch`'s local path: pick the box
/// via `route_session`, look up the tenant in that box, then look
/// up the session under that tenant.
///
/// Returns `None` when either the tenant or the session has not
/// been minted yet (a subscriber may legitimately attach before
/// the very first append). Transient KV errors are also collapsed
/// to `None` — Phase 1 callers want a "best effort" cursor and
/// emit `live_seq: 0` in that case — but errors are logged via
/// `tracing::warn!` so infra issues show up in logs instead of
/// silently turning into `live_seq=0`. Task 5's catch-up replay
/// depends on this distinction: legitimate "not yet minted" should
/// stay quiet, real KV failures should be visible.
pub async fn resolve_session_next_seq(
    state: &AppState,
    tenant_external: &str,
    session_external: &str,
) -> Option<u64> {
    let box_ = state
        .registry
        .route_session(tenant_external, session_external);
    let tenant = match state.tenants.lookup(box_.id, tenant_external) {
        Ok(Some(t)) => t,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(
                error = %e,
                tenant = tenant_external,
                "resolve_session_next_seq: tenant lookup error",
            );
            return None;
        }
    };
    let session = match state
        .sessions
        .lookup(box_.id, tenant.internal_id, session_external)
    {
        Ok(Some(s)) => s,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(
                error = %e,
                tenant = tenant_external,
                session = session_external,
                "resolve_session_next_seq: session lookup error",
            );
            return None;
        }
    };
    Some(session.next_seq)
}

/// Catch-up scan helper. Resolves the (tenant, session) pair the
/// same way [`resolve_session_next_seq`] does, opens a `ReadCtx`
/// against the box, and delegates to
/// [`agentdb_core::fetch_events_from_seq`] for the half-open range
/// `[from_seq, end_seq)`.
///
/// Returns `Ok(empty vec)` if the tenant or session does not exist
/// yet (subscriber attached before any append). Returns
/// `Err(ApiError::Internal)` on KV / snapshot errors so the caller
/// can log them — at the SSE handler boundary we degrade to "no
/// catch-up" rather than dropping the whole subscription, but the
/// error path stays distinguishable from "no events to replay".
pub async fn scan_events_from_seq(
    state: &AppState,
    tenant_external: &str,
    session_external: &str,
    from_seq: u64,
    end_seq: u64,
) -> Result<Vec<agentdb_core::types::Event>, ApiError> {
    let box_ = state
        .registry
        .route_session(tenant_external, session_external);
    let tenant = match state.tenants.lookup(box_.id, tenant_external) {
        Ok(Some(t)) => t,
        Ok(None) => return Ok(Vec::new()),
        Err(e) => {
            return Err(ApiError::Internal(format!(
                "scan_events_from_seq: tenant lookup: {e}"
            )))
        }
    };
    let session = match state
        .sessions
        .lookup(box_.id, tenant.internal_id, session_external)
    {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(Vec::new()),
        Err(e) => {
            return Err(ApiError::Internal(format!(
                "scan_events_from_seq: session lookup: {e}"
            )))
        }
    };
    let ctx = agentdb_core::ReadCtx::open(&box_, state.registry.kv())
        .map_err(|e| ApiError::Internal(format!("scan_events_from_seq: open ReadCtx: {e}")))?;
    agentdb_core::fetch_events_from_seq(&ctx, &session, from_seq, end_seq)
        .map_err(|e| ApiError::Internal(format!("scan_events_from_seq: fetch: {e}")))
}

#[utoipa::path(
    post,
    path = "/api/tenants/{tenant_id}/sessions/{session_id}/append_and_fetch",
    tag = "primitives",
    params(
        ("tenant_id" = String, Path, description = "External tenant id"),
        ("session_id" = String, Path, description = "External session id"),
    ),
    request_body = AppendAndFetchRequest,
    responses(
        (status = 200, description = "Event appended; last N events returned", body = AppendAndFetchResponse),
        (status = 400, description = "Malformed request", body = ErrorResponse),
        (status = 403, description = "Authenticated tenant disagrees with path tenant", body = ErrorResponse),
        (status = 421, description = "Routing drift on a forwarded request"),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "Fault injected, forwarding failed, or node draining", body = ErrorResponse),
    ),
)]
#[tracing::instrument(
    name = "rest.append_and_fetch",
    skip(state, headers, body, authed),
    fields(
        tenant = %tenant,
        session = %session,
        forwarded = headers.contains_key(FORWARDED_HEADER),
    ),
)]
pub async fn append_and_fetch(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    headers: HeaderMap,
    authed: Option<Extension<AuthedTenant>>,
    body: Bytes,
) -> Response {
    // Auth-vs-path cross-check (M26 closeout). The middleware
    // already verified path tenant matches credentials, but
    // belt-and-braces: if for any reason an unauthed request
    // reached us with a path tenant we can't trust, the
    // handlers' own auth-derived view stays the source of
    // truth. Match → fall through; mismatch → 403.
    if let Some(Extension(authed)) = &authed {
        if authed.external_id != tenant {
            return ApiError::Forbidden(format!(
                "user {} is bound to tenant {} but path targets {tenant}",
                authed.user_id, authed.external_id
            ))
            .into_response();
        }
    }
    do_append_and_fetch(state, tenant, session, headers, body).await
}

#[utoipa::path(
    post,
    path = "/api/sessions/{session_id}/append_and_fetch",
    tag = "primitives",
    params(
        ("session_id" = String, Path, description = "External session id"),
    ),
    request_body = AppendAndFetchRequest,
    responses(
        (status = 200, description = "Event appended; last N events returned", body = AppendAndFetchResponse),
        (status = 400, description = "Malformed request, or tenant cannot be resolved (no auth, no locator)", body = ErrorResponse),
        (status = 401, description = "Missing or invalid Authorization header (when auth is enabled)", body = ErrorResponse),
        (status = 403, description = "Authenticated tenant doesn't own this session", body = ErrorResponse),
        (status = 421, description = "Routing drift on a forwarded request"),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "Fault injected, forwarding failed, or node draining", body = ErrorResponse),
    ),
)]
#[tracing::instrument(
    name = "rest.append_and_fetch_session_keyed",
    skip(state, headers, body, authed),
    fields(
        session = %session,
        forwarded = headers.contains_key(FORWARDED_HEADER),
    ),
)]
pub async fn append_and_fetch_session_keyed(
    State(state): State<AppState>,
    Path(session): Path<String>,
    headers: HeaderMap,
    authed: Option<Extension<AuthedTenant>>,
    body: Bytes,
) -> Response {
    // M26 v2 contract: tenant is derived from auth (canonical
    // when present) and cross-checked against the session
    // locator. No tenant in the path or body — silent client
    // attempts to spoof tenant get a 403, not a confused
    // routing decision.
    let authed_tenant = authed.as_deref().map(|a| a.external_id.as_str());
    let authed_user = authed.as_deref().map(|a| a.user_id.as_str());
    let locator_tenant = match state.registry.meta_store() {
        Some(meta) => match agentdb_meta::session_locators::get(meta.pool(), &session).await {
            Ok(Some(loc)) => loc.tenant_external_id,
            Ok(None) => None,
            Err(e) => {
                return ApiError::Core(agentdb_core::Error::Invalid {
                    field: None,
                    message: format!("session locator lookup failed: {e}"),
                })
                .into_response()
            }
        },
        None => None,
    };
    let tenant = match (authed_tenant, locator_tenant.as_deref()) {
        (Some(a), Some(l)) if a != l => {
            return ApiError::Forbidden(format!(
                "user {} is bound to tenant {a} but session {session} belongs to tenant {l}",
                authed_user.unwrap_or("?")
            ))
            .into_response();
        }
        (Some(a), _) => a.to_string(),
        (None, Some(l)) => l.to_string(),
        (None, None) => {
            return ApiError::Core(agentdb_core::Error::Invalid {
                field: Some("tenant".into()),
                message: format!(
                    "cannot resolve tenant for session {session}: no Authorization header, \
                     no session locator. Use /api/tenants/<tid>/sessions/<sid>/append_and_fetch \
                     for pre-M26 sessions, or enable auth"
                ),
            })
            .into_response()
        }
    };
    do_append_and_fetch(state, tenant, session, headers, body).await
}

async fn do_append_and_fetch(
    state: AppState,
    tenant: String,
    session: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // M20 multi-node: decide whether to write locally or
    // forward before we deserialize, so we can ship the original
    // bytes to the peer without a round-trip through JSON.
    //
    // M26 final phase: route via the locator (where the
    // session canonically lives) before falling back to
    // shard-override / tenant-hash — keeps writes aligned
    // with the locator-driven reads from PR #74.
    let decision = state
        .registry
        .route_session_for_write(&tenant, &session)
        .await;
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);

    if let agentdb_core::RouteDecision::Remote { target, tablet_id } = &decision {
        if already_forwarded {
            // A peer forwarded to us, and our own routing still
            // says Remote. Routing drift between nodes — surface
            // 421 so operators see it in logs rather than a
            // silent infinite loop.
            let local_node = state.registry.node_id().unwrap_or("unknown").to_string();
            return forwarding::misdirected_reply(&local_node, *tablet_id);
        }
        // Count the forward attempt on box 0 (admin-chosen
        // attribution — the actual write lands on a remote
        // box, so there's no natural local box to attribute to).
        let box_for_metrics = state.registry.box_by_id(0);
        if let Some(ref b) = box_for_metrics {
            b.metrics
                .forwarded_writes_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let passthrough = PassthroughHeaders::from_headers(&headers);
        match forwarding::forward_append_and_fetch(
            &state.http,
            target,
            &tenant,
            &session,
            body,
            passthrough,
        )
        .await
        {
            Ok(resp) => return resp,
            Err(err) => {
                if let Some(b) = box_for_metrics {
                    b.metrics
                        .forwarded_write_failures_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                let body = err.to_body();
                let status = StatusCode::SERVICE_UNAVAILABLE;
                return (status, axum::Json(body)).into_response();
            }
        }
    }

    // Local path: deserialize the body we read as bytes and
    // fall through to the existing append_and_fetch.
    let parsed: AppendAndFetchRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return ApiError::Core(agentdb_core::Error::Invalid {
                field: None,
                message: format!("invalid JSON body: {e}"),
            })
            .into_response()
        }
    };
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let out = match agentdb_core::append_and_fetch_with_actor(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &tenant,
        &session,
        parsed.event,
        parsed.actor_id,
        parsed.fetch_last_n,
        now_millis,
    )
    .await
    {
        Ok(o) => o,
        Err(agentdb_core::Error::TooManyRequests {
            scope,
            retry_after_ms,
        }) => {
            // Attribute the QuotaHit event to the box that actually
            // rejected — matches the M20 session routing above.
            let box_id = state.registry.route_session(&tenant, &session).id;
            state.stream.emit(crate::stream::StreamEvent::QuotaHit {
                ts_ms: crate::stream::now_ms(),
                box_id,
                kind: scope.as_str().into(),
                tenant: Some(tenant.clone()),
            });
            return ApiError::Core(agentdb_core::Error::TooManyRequests {
                scope,
                retry_after_ms,
            })
            .into_response();
        }
        Err(e) => return ApiError::Core(e).into_response(),
    };
    // Pub/sub fan-out. Spec §6.7: emit AFTER the durable write
    // succeeds (we are past `append_and_fetch_with_actor` Ok) and
    // BEFORE the response is built, so subscribers never see an
    // event we wouldn't actually return on read. Fire-and-forget;
    // backpressure is absorbed by the bus's overflow policy.
    state.event_bus.emit(crate::event_bus::envelope_from_event(
        &tenant,
        &session,
        &out.appended,
    ));
    let recent = out.recent.into_iter().map(EventDto::from).collect();
    axum::Json(AppendAndFetchResponse {
        seq: out.new_seq,
        recent,
    })
    .into_response()
}
