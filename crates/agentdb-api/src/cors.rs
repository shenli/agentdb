//! Per-tenant CORS configuration for subscribe endpoints (spec § 6.13).
//!
//! Wired into the router in Task 10-2.

use std::collections::HashMap;

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
