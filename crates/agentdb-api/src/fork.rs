use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::fork::fork_session;
use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth_middleware::AuthedTenant;
use crate::error::{ApiError, ErrorResponse};
use crate::forwarding::{self, PassthroughHeaders, FORWARDED_HEADER};
use crate::stream::StreamEvent;
use crate::AppState;

/// Body shape for `POST /api/sessions/{session_id}/fork`.
///
/// `tenant_id` is **deprecated** as of M26 Phase 1b and will
/// reject mismatching values when auth is enabled. The
/// canonical resolution path is now:
///
/// 1. `AuthedTenant` extension (M26 closeout). When auth is
///    enabled, the tenant is taken from credentials and
///    cross-checked against the locator.
/// 2. `session_locators::get(parent_session_id)` returns the
///    parent's `tenant_external_id`.
/// 3. Body `tenant_id`, kept for back-compat in single-process
///    no-auth deployments and the rare pre-locator session.
///
/// When several sources supply a tenant and they disagree, the
/// handler returns 400/403 — silently overruling the client
/// would hide configuration drift.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct ForkRequest {
    /// Deprecated as of M26 Phase 1b. When omitted, the tenant
    /// is derived from credentials (auth) and/or the parent
    /// session's locator. Supply only for backward compat with
    /// pre-M26 clients in no-auth mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub new_session_id: String,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ForkResponse {
    pub new_session_id: String,
    pub parent_session_id: String,
    pub parent_seq_at_fork: u64,
}

#[utoipa::path(
    post,
    path = "/api/sessions/{session_id}/fork",
    tag = "primitives",
    params(("session_id" = String, Path, description = "Parent session's external id")),
    request_body = ForkRequest,
    responses(
        (status = 200, description = "Child session created", body = ForkResponse),
        (status = 400, description = "Malformed request, missing tenant resolution, or body/locator mismatch", body = ErrorResponse),
        (status = 404, description = "Parent tenant/session not found", body = ErrorResponse),
        (status = 421, description = "Routing drift on a forwarded request"),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
    ),
)]
pub async fn fork_handler(
    State(state): State<AppState>,
    Path(parent_session_id): Path<String>,
    headers: HeaderMap,
    authed: Option<Extension<AuthedTenant>>,
    body: Bytes,
) -> Response {
    let parsed: ForkRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return ApiError::Core(agentdb_core::Error::Invalid {
                field: None,
                message: format!("invalid JSON body: {e}"),
            })
            .into_response()
        }
    };

    // Resolve the effective tenant. Auth first (M26 closeout);
    // locator second; body third (back-compat). Mismatches
    // surface 400/403 rather than silently overruling.
    let tenant_id = match resolve_fork_tenant(
        &state,
        &parent_session_id,
        parsed.tenant_id.as_deref(),
        authed.as_deref(),
    )
    .await
    {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let decision = state
        .registry
        .route_session_for_io(&tenant_id, &parent_session_id);
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);

    if let agentdb_core::RouteDecision::Remote { target, tablet_id } = &decision {
        if already_forwarded {
            let local_node = state
                .registry
                .node_id()
                .unwrap_or("unknown")
                .to_string();
            return forwarding::misdirected_reply(&local_node, *tablet_id);
        }
        if let Some(b) = state.registry.box_by_id(0) {
            b.metrics.forwarded_reads_total.fetch_add(1, Ordering::Relaxed);
        }
        let passthrough = PassthroughHeaders::from_headers(&headers);
        match forwarding::forward_fork(
            &state.http,
            target,
            &parent_session_id,
            body,
            passthrough,
        )
        .await
        {
            Ok(resp) => return resp,
            Err(err) => {
                if let Some(b) = state.registry.box_by_id(0) {
                    b.metrics
                        .forwarded_read_failures_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                return (StatusCode::SERVICE_UNAVAILABLE, Json(err.to_body())).into_response();
            }
        }
    }

    let box_ = match decision {
        agentdb_core::RouteDecision::Local(b) => b,
        agentdb_core::RouteDecision::Remote { .. } => unreachable!(),
    };
    let guard = box_.tenant_write_lock(&tenant_id).lock_owned().await;
    let child = match fork_session(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &guard,
        &tenant_id,
        &parent_session_id,
        &parsed.new_session_id,
        now_millis(),
    ) {
        Ok(s) => s,
        Err(agentdb_core::Error::TooManyRequests {
            scope,
            retry_after_ms,
        }) => {
            let box_id = box_.id;
            state.stream.emit(StreamEvent::QuotaHit {
                ts_ms: crate::stream::now_ms(),
                box_id,
                kind: scope.as_str().to_string(),
                tenant: Some(tenant_id.clone()),
            });
            return ApiError::Core(agentdb_core::Error::TooManyRequests {
                scope,
                retry_after_ms,
            })
            .into_response();
        }
        Err(e) => return ApiError::Core(e).into_response(),
    };
    drop(guard);
    // M26 Phase 1: register the child session in the locator
    // table so subsequent session-scoped lookups can route to
    // it directly. Idempotent — `INSERT OR IGNORE` is the
    // only side effect, so no harm if the locator already
    // exists from a prior partial-failure retry.
    if let Some(meta) = state.registry.meta_store() {
        if let Ok(Some(tenant)) = state.tenants.lookup(box_.id, &tenant_id) {
            agentdb_core::primitives::populate_session_locator(
                meta.as_ref(),
                &child.external_id,
                tenant.internal_id,
                &tenant_id,
                box_.id,
                now_millis(),
            )
            .await;
        }
    }
    let fork = child
        .forked_from
        .expect("fork_session always sets forked_from");
    state.stream.emit(StreamEvent::ForkCreated {
        ts_ms: now_millis(),
        box_id: box_.id,
        parent: parent_session_id.clone(),
        child: child.external_id.clone(),
    });
    Json(ForkResponse {
        new_session_id: child.external_id,
        parent_session_id,
        parent_seq_at_fork: fork.parent_seq_at_fork,
    })
    .into_response()
}

/// Resolve the effective tenant for a fork request.
///
/// Source priority (M26 closeout):
///
/// 1. **`AuthedTenant`** (auth-bound). When present, this is
///    authoritative — it's the credential identity, not a
///    client claim. Locator and body must agree if supplied;
///    disagreement returns 403 (auth-vs-locator) or 400
///    (auth-vs-body) so misconfigured clients are visible.
/// 2. **Session locator**. The canonical record of which
///    tenant owns the parent session. Cross-checked against
///    auth above; used standalone when auth is disabled.
/// 3. **Request body** (legacy / back-compat). Used only when
///    auth is disabled AND the locator is absent (pre-M26
///    sessions, single-process no-auth tests).
async fn resolve_fork_tenant(
    state: &AppState,
    parent_session_id: &str,
    body_tenant: Option<&str>,
    authed: Option<&AuthedTenant>,
) -> Result<String, Response> {
    let locator_tenant = match state.registry.meta_store() {
        Some(meta) => match agentdb_meta::session_locators::get(meta.pool(), parent_session_id)
            .await
        {
            Ok(Some(loc)) => loc.tenant_external_id,
            Ok(None) => None,
            Err(e) => {
                return Err(ApiError::Core(agentdb_core::Error::Invalid {
                    field: None,
                    message: format!("session locator lookup failed: {e}"),
                })
                .into_response());
            }
        },
        None => None,
    };

    // Cross-checks. Auth-vs-locator drifts to 403 because the
    // credentialed user genuinely doesn't own the parent
    // session — that's an authorization failure, not a request
    // shape error. Auth-vs-body drifts to 400 because the body
    // is just a stale client claim.
    if let Some(authed) = authed {
        if let Some(locator) = locator_tenant.as_deref() {
            if locator != authed.external_id {
                return Err(ApiError::Forbidden(format!(
                    "user {} is bound to tenant {} but session {parent_session_id} \
                     belongs to tenant {locator}",
                    authed.user_id, authed.external_id
                ))
                .into_response());
            }
        }
        if let Some(body) = body_tenant {
            if body != authed.external_id {
                return Err(ApiError::Core(agentdb_core::Error::Invalid {
                    field: Some("tenant_id".into()),
                    message: format!(
                        "request body tenant_id '{body}' disagrees with the authenticated \
                         tenant '{}'; omit tenant_id or update the client",
                        authed.external_id
                    ),
                })
                .into_response());
            }
        }
        return Ok(authed.external_id.clone());
    }

    match (locator_tenant, body_tenant) {
        (Some(locator), Some(body)) if locator != body => Err(ApiError::Core(
            agentdb_core::Error::Invalid {
                field: Some("tenant_id".into()),
                message: format!(
                    "request tenant_id '{body}' disagrees with parent session's locator (tenant '{locator}'); \
                     either omit tenant_id or update the client"
                ),
            },
        )
        .into_response()),
        (Some(locator), _) => Ok(locator),
        (None, Some(body)) => Ok(body.to_string()),
        (None, None) => Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("tenant_id".into()),
            message: "tenant_id is required when the parent session has no locator (pre-M26 \
                      session); omit it once the parent has been touched on M26 or later"
                .into(),
        })
        .into_response()),
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
