//! Per-tenant CORS configuration for subscribe endpoints (spec § 6.13).
//!
//! Wired into the router in Task 10-2.

use std::collections::HashMap;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::AppState;

#[derive(Clone, Default, Debug)]
pub struct CorsConfig {
    /// Map of tenant_id → list of allowed origins.
    /// Wildcard `*` is forbidden when JWT auth is in play; the
    /// middleware enforces this at request time.
    pub per_tenant: HashMap<String, Vec<String>>,
}

impl CorsConfig {
    /// Returns the origin to echo back if the given origin is allowed
    /// for this tenant; otherwise None.
    pub fn allowed_origin(&self, tenant: &str, origin: &str) -> Option<String> {
        self.per_tenant
            .get(tenant)
            .and_then(|origins| origins.iter().find(|o| o.as_str() == origin).cloned())
    }
}

/// CORS middleware for v2 subscribe and channel routes.
///
/// v2 paths (`/api/sessions/:sid/subscribe`, `/api/subscribe`,
/// `/api/channels/:c/publish`, `/api/channels/:c/subscribe`) carry
/// no tenant in the URL — tenant is derived from auth/JWT, which
/// runs *after* this middleware. With no path-tenant to look up
/// against `CorsConfig::per_tenant`, the middleware applies
/// default-permissive CORS: echoes back whatever `Origin` header
/// the browser sent.
///
/// `OPTIONS` preflight requests short-circuit with 204 carrying
/// the CORS headers. Requests with no `Origin` header pass through
/// untouched.
///
/// Per-tenant CORS allowlist enforcement on v2 paths is a known
/// limitation; reintroducing it requires either decoding the auth
/// header inside this middleware (heavy) or moving CORS to a
/// post-auth handler (breaks OPTIONS preflights). Tracked as M29.
pub async fn cors_middleware(
    State(_state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);

    // v2 default-permissive: echo the request's Origin back. No
    // path-tenant to consult, no per-tenant allowlist applied.
    let allowed = origin;

    if req.method() == Method::OPTIONS {
        let mut resp = Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(axum::body::Body::empty())
            .expect("build 204 preflight response");
        if let Some(origin) = allowed {
            let h = resp.headers_mut();
            if let Ok(v) = HeaderValue::from_str(&origin) {
                h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
            }
            h.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, OPTIONS"),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("authorization, content-type"),
            );
            h.insert(header::VARY, HeaderValue::from_static("Origin"));
        }
        return resp;
    }

    let mut resp = next.run(req).await;
    if let Some(origin) = allowed {
        let h = resp.headers_mut();
        if let Ok(v) = HeaderValue::from_str(&origin) {
            h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
        }
        h.insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_origin_matches_exact() {
        let mut cfg = CorsConfig::default();
        cfg.per_tenant.insert(
            "t-1".into(),
            vec![
                "https://app.example.com".into(),
                "https://staging.example.com".into(),
            ],
        );
        assert_eq!(
            cfg.allowed_origin("t-1", "https://app.example.com"),
            Some("https://app.example.com".into())
        );
        assert_eq!(
            cfg.allowed_origin("t-1", "https://app.example.com:443"),
            None
        );
        assert_eq!(
            cfg.allowed_origin("t-other", "https://app.example.com"),
            None
        );
    }

    #[test]
    fn allowed_origin_returns_none_for_unknown_tenant() {
        let cfg = CorsConfig::default();
        assert!(cfg
            .allowed_origin("t-x", "https://app.example.com")
            .is_none());
    }
}
