//! Coarse installation-level HTTP authentication.
//!
//! `server.api_auth_token` protects every external `/api/*` route as the root
//! credential. Metadata-backed bearer tokens add finer scopes: `admin` (full
//! access), `readonly` (GET/HEAD anywhere), and `tenant` (data-plane access
//! bound to a single tenant). The instance stays open only until the first
//! metadata token is minted and only when no static token is configured.
//! Internal Raft routes retain their separate rotating token and are
//! intentionally excluded here.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::sysmeta::{self, AdminScope};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::state::AppState;

/// The authenticated identity attached to every authorized `/api/*` request.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApiPrincipal {
    /// Static installation token, or an instance still in open bootstrap.
    Root,
    Admin {
        token_id: String,
    },
    Readonly {
        token_id: String,
    },
    Tenant {
        token_id: String,
        tenant_id: String,
    },
}

#[derive(Debug, Clone)]
struct ResolvedToken {
    token_id: String,
    scope: AdminScope,
    tenant_id: Option<String>,
}

/// Short-TTL memo of metadata-token lookups so per-request auth stays off the
/// SQL hot path. Mint/revoke invalidate it; a TTL of zero disables caching.
pub struct AuthTokenCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, (Instant, Option<ResolvedToken>)>>,
    any_tokens: Mutex<Option<(Instant, bool)>>,
}

const AUTH_CACHE_MAX_ENTRIES: usize = 1024;

impl AuthTokenCache {
    pub fn from_env() -> Self {
        let ttl_ms = std::env::var("AGENTDB_AUTH_CACHE_TTL_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(60_000);
        Self::with_ttl(Duration::from_millis(ttl_ms))
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
            any_tokens: Mutex::new(None),
        }
    }

    /// Drop every memoized lookup. Called on mint and revoke so scope and
    /// revocation changes take effect immediately on this node.
    pub fn invalidate(&self) {
        self.entries.lock().unwrap().clear();
        *self.any_tokens.lock().unwrap() = None;
    }

    fn get(&self, key: &str) -> Option<Option<ResolvedToken>> {
        if self.ttl.is_zero() {
            return None;
        }
        let entries = self.entries.lock().unwrap();
        let (stored_at, resolved) = entries.get(key)?;
        (stored_at.elapsed() < self.ttl).then(|| resolved.clone())
    }

    fn put(&self, key: String, resolved: Option<ResolvedToken>) {
        if self.ttl.is_zero() {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        // Unauthenticated probes with garbage bearers must not grow this
        // without bound.
        if entries.len() >= AUTH_CACHE_MAX_ENTRIES {
            entries.clear();
        }
        entries.insert(key, (Instant::now(), resolved));
    }

    fn get_any_tokens(&self) -> Option<bool> {
        if self.ttl.is_zero() {
            return None;
        }
        let cached = self.any_tokens.lock().unwrap();
        let (stored_at, value) = (*cached)?;
        (stored_at.elapsed() < self.ttl).then_some(value)
    }

    fn put_any_tokens(&self, value: bool) {
        if self.ttl.is_zero() {
            return;
        }
        *self.any_tokens.lock().unwrap() = Some((Instant::now(), value));
    }
}

pub async fn authorize_external_api(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    if !path.starts_with("/api/") || path.starts_with("/api/internal/") {
        return next.run(request).await;
    }

    // The system tenant (self-hosted metadata) is not reachable through the
    // public API under ANY principal; only in-process code touches it.
    if path
        .strip_prefix("/api/tenants/")
        .is_some_and(|rest| rest.split('/').next() == Some(agentdb_core::SYSTEM_TENANT_ID))
    {
        return auth_error(
            StatusCode::FORBIDDEN,
            "the system tenant is reserved for internal metadata",
        );
    }

    let presented = bearer_token(request.headers()).map(str::to_string);
    if static_token_matches(&state, presented.as_deref()) {
        request.extensions_mut().insert(ApiPrincipal::Root);
        return next.run(request).await;
    }

    if let Some(presented) = presented.as_deref() {
        let resolved = match resolve_token_cached(&state, presented).await {
            Ok(resolved) => resolved,
            Err(error) => {
                tracing::error!(?error, "failed to verify API bearer token");
                return auth_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authentication metadata is unavailable",
                );
            }
        };
        if let Some(token) = resolved {
            return match authorize_principal(&token, &path, request.method()) {
                Ok(principal) => {
                    request.extensions_mut().insert(principal);
                    next.run(request).await
                }
                Err(message) => auth_error(StatusCode::FORBIDDEN, message),
            };
        }
    }

    // No valid credential presented. A configured static token always closes
    // the instance; otherwise it stays open only until the first metadata
    // token is minted (local/bootstrap compatibility). Non-loopback startup
    // separately requires a static API token.
    if state.api_auth_token.is_some() || state.api_auth_previous_token.is_some() {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "a valid API bearer token is required",
        );
    }

    let any_tokens = match any_tokens_cached(&state).await {
        Ok(any) => any,
        Err(error) => {
            tracing::error!(?error, "failed to read authentication state");
            return auth_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication metadata is unavailable",
            );
        }
    };
    if any_tokens {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "a valid API bearer token is required",
        );
    }
    request.extensions_mut().insert(ApiPrincipal::Root);
    next.run(request).await
}

fn authorize_principal(
    token: &ResolvedToken,
    path: &str,
    method: &Method,
) -> Result<ApiPrincipal, &'static str> {
    match token.scope {
        AdminScope::Admin => Ok(ApiPrincipal::Admin {
            token_id: token.token_id.clone(),
        }),
        AdminScope::Readonly => {
            if matches!(*method, Method::GET | Method::HEAD) {
                Ok(ApiPrincipal::Readonly {
                    token_id: token.token_id.clone(),
                })
            } else {
                Err("the bearer token is read-only")
            }
        }
        AdminScope::Tenant => {
            // verify() fails closed on tenant rows without a binding, so this
            // is unreachable in practice; deny anyway.
            let Some(tenant_id) = token.tenant_id.as_deref() else {
                return Err("the bearer token has no tenant binding");
            };
            if tenant_path_matches(path, tenant_id) || path == "/api/me" {
                Ok(ApiPrincipal::Tenant {
                    token_id: token.token_id.clone(),
                    tenant_id: tenant_id.to_string(),
                })
            } else {
                Err("the bearer token is scoped to a different tenant")
            }
        }
    }
}

/// True when `path` addresses exactly the tenant this token is bound to:
/// `/api/tenants/{tenant_id}` or anything below it.
fn tenant_path_matches(path: &str, tenant_id: &str) -> bool {
    let Some(rest) = path.strip_prefix("/api/tenants/") else {
        return false;
    };
    match rest.split_once('/') {
        Some((segment, _)) => segment == tenant_id,
        None => rest == tenant_id,
    }
}

async fn resolve_token_cached(
    state: &AppState,
    presented: &str,
) -> Result<Option<ResolvedToken>, agentdb_core::Error> {
    let key = sysmeta::fingerprint(presented);
    if let Some(cached) = state.auth_cache.get(&key) {
        return Ok(cached);
    }
    let resolved = sysmeta::verify_token(&state.runtime, presented)
        .await?
        .map(|verified| ResolvedToken {
            token_id: verified.token_id,
            scope: verified.scope,
            tenant_id: verified.tenant_id,
        });
    state.auth_cache.put(key, resolved.clone());
    Ok(resolved)
}

async fn any_tokens_cached(state: &AppState) -> Result<bool, agentdb_core::Error> {
    if let Some(cached) = state.auth_cache.get_any_tokens() {
        return Ok(cached);
    }
    let any = sysmeta::count_all_tokens(&state.runtime).await? > 0;
    state.auth_cache.put_any_tokens(any);
    Ok(any)
}

fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

fn static_token_matches(state: &AppState, presented: Option<&str>) -> bool {
    let Some(presented) = presented else {
        return false;
    };
    let current = state
        .api_auth_token
        .as_deref()
        .is_some_and(|expected| constant_time_eq(expected, presented));
    let previous = state
        .api_auth_previous_token
        .as_deref()
        .is_some_and(|expected| constant_time_eq(expected, presented));
    current | previous
}

fn constant_time_eq(expected: &str, presented: &str) -> bool {
    expected.as_bytes().ct_eq(presented.as_bytes()).unwrap_u8() == 1
}

fn auth_error(status: StatusCode, message: &'static str) -> Response {
    let mut response = (
        status,
        Json(serde_json::json!({
            "code": if status == StatusCode::FORBIDDEN { "forbidden" } else { "unauthorized" },
            "message": message,
            "retryable": false
        })),
    )
        .into_response();
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"agentdb\""),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::tenant_path_matches;

    #[test]
    fn tenant_path_matching_is_exact_per_segment() {
        assert!(tenant_path_matches("/api/tenants/t_1", "t_1"));
        assert!(tenant_path_matches("/api/tenants/t_1/sessions", "t_1"));
        assert!(!tenant_path_matches("/api/tenants/t_10", "t_1"));
        assert!(!tenant_path_matches("/api/tenants/t_10/sessions", "t_1"));
        assert!(!tenant_path_matches("/api/tenants", "t_1"));
        assert!(!tenant_path_matches("/api/admin/tokens", "t_1"));
        assert!(!tenant_path_matches("/api/storage/summary", "t_1"));
    }
}
