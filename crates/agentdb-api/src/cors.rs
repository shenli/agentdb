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

/// Per-tenant CORS middleware for the subscribe routes. Reads the
/// `Origin` header on the request, looks up the tenant from the
/// path (`/api/tenants/:tenant_id/...`), and — if the origin is in
/// the per-tenant allowlist — echoes the origin back via
/// `Access-Control-Allow-Origin` plus a `Vary: Origin` hint.
///
/// `OPTIONS` requests are short-circuited with a 204 preflight
/// response carrying the CORS headers (when the origin is allowed).
/// The inner subscribe handler is never invoked on preflights.
///
/// Requests with no `Origin` header pass through untouched (no
/// CORS headers added). Same for tenants with no CORS config:
/// browsers without an allowlisted origin block the response, but
/// non-browser clients (CLI, server-to-server) are unaffected.
pub async fn cors_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);
    let tenant = extract_tenant_from_path(req.uri().path());
    let allowed = match (origin.as_deref(), tenant.as_deref()) {
        (Some(o), Some(t)) => state.cors_config.allowed_origin(t, o),
        _ => None,
    };

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
                HeaderValue::from_static("GET, OPTIONS"),
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

/// Pull the tenant id out of paths shaped like
/// `/api/tenants/<id>/...`. Returns `None` for non-tenant paths
/// (health, metrics, admin, …) — those skip CORS entirely because
/// the middleware is only layered on the subscribe routes.
fn extract_tenant_from_path(path: &str) -> Option<String> {
    let mut parts = path.split('/').filter(|s| !s.is_empty());
    if parts.next()? != "api" {
        return None;
    }
    if parts.next()? != "tenants" {
        return None;
    }
    Some(parts.next()?.to_string())
}

#[cfg(test)]
mod path_tests {
    use super::extract_tenant_from_path;

    #[test]
    fn extracts_tenant_from_subscribe_paths() {
        assert_eq!(
            extract_tenant_from_path("/api/tenants/t-1/subscribe"),
            Some("t-1".into())
        );
        assert_eq!(
            extract_tenant_from_path("/api/tenants/t-1/sessions/s-1/subscribe"),
            Some("t-1".into())
        );
        assert_eq!(extract_tenant_from_path("/health/ready"), None);
        assert_eq!(extract_tenant_from_path("/api/admin/nodes"), None);
    }
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
