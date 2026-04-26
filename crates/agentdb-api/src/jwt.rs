//! JWT verification for browser-direct subscribers (spec § 6.13).
//!
//! Per-tenant config supplies either an HMAC secret (HS256) or a JWKS URL
//! (RS256/ES256, implemented in Task 9b). Used by Task 9c's subscribe
//! middleware. Task 9d wires JWT-expiry close behavior into the live
//! SSE stream.

use std::collections::HashMap;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

/// Per-tenant JWT verification config.
///
/// - `Hmac`: HS256 with a shared secret. Suitable for Supabase Auth /
///   Auth0 setups that use a symmetric signing key.
/// - `Jwks`: fetches the issuer's JWKS for RS256/ES256. Implementation
///   lands in Task 9b; this variant currently rejects with
///   `JwtError::Invalid("jwks not implemented (Task 9b)")`.
#[derive(Debug, Clone)]
pub enum TenantJwtConfig {
    Hmac {
        secret: Vec<u8>,
        issuer: String,
        audience: Option<String>,
    },
    Jwks {
        url: String,
        issuer: String,
        audience: Option<String>,
    },
}

/// Tenant-keyed JWT config. A tenant absent from `per_tenant` means
/// "no JWT auth configured for this tenant" — the subscribe
/// middleware (Task 9c) will reject such requests at HTTP layer.
#[derive(Debug, Clone, Default)]
pub struct JwtConfig {
    pub per_tenant: HashMap<String, TenantJwtConfig>,
}

/// Claim shape AgentDB requires in browser-direct subscribe tokens.
///
/// `sessions` is either the wildcard `"*"` (subscribe to any session
/// in the tenant) or a list of allowed session ids. The URL-path
/// tenant is cross-checked against `tenant` after signature verify
/// to prevent a valid-but-cross-tenant token from working.
#[derive(Debug, Deserialize, Serialize)]
pub struct AgentDbClaims {
    pub iss: String,
    #[serde(default)]
    pub aud: Option<String>,
    pub sub: String,
    pub tenant: String,
    pub sessions: SessionsClaim,
    pub exp: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum SessionsClaim {
    /// `"*"` — any session in the tenant. Other strings are rejected
    /// by `allows`, so a typo like `"all"` won't accidentally grant.
    Wildcard(String),
    List(Vec<String>),
}

impl SessionsClaim {
    pub fn allows(&self, session: &str) -> bool {
        match self {
            SessionsClaim::Wildcard(s) if s == "*" => true,
            SessionsClaim::List(v) => v.iter().any(|s| s == session),
            _ => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("tenant not configured for JWT")]
    TenantNotConfigured,
    #[error("invalid jwt: {0}")]
    Invalid(String),
    #[error("expired")]
    Expired,
    #[error("issuer mismatch")]
    IssuerMismatch,
    #[error("tenant claim mismatch")]
    TenantMismatch,
    #[error("session not in jwt claim")]
    SessionForbidden,
}

/// Verify `token` against the JWT config registered for `tenant`.
///
/// On success returns the decoded claims. The `tenant` arg is the
/// URL-path tenant; we require it to match `claims.tenant` AFTER
/// signature verification to avoid leaking "this signature is valid"
/// signals across tenants.
pub async fn verify_for_tenant(
    cfg: &JwtConfig,
    tenant: &str,
    token: &str,
) -> Result<AgentDbClaims, JwtError> {
    let tcfg = cfg
        .per_tenant
        .get(tenant)
        .ok_or(JwtError::TenantNotConfigured)?;
    match tcfg {
        TenantJwtConfig::Hmac {
            secret,
            issuer,
            audience,
        } => {
            let mut v = Validation::new(Algorithm::HS256);
            v.set_issuer(&[issuer.as_str()]);
            if let Some(aud) = audience {
                v.set_audience(&[aud.as_str()]);
            } else {
                // Default Validation enables aud check; we want
                // "no aud configured" to mean "don't enforce aud".
                v.validate_aud = false;
            }
            v.validate_exp = true;
            let key = DecodingKey::from_secret(secret);
            let data = decode::<AgentDbClaims>(token, &key, &v).map_err(map_err)?;
            if data.claims.tenant != tenant {
                return Err(JwtError::TenantMismatch);
            }
            Ok(data.claims)
        }
        TenantJwtConfig::Jwks { .. } => {
            Err(JwtError::Invalid("jwks not implemented (Task 9b)".into()))
        }
    }
}

fn map_err(e: jsonwebtoken::errors::Error) -> JwtError {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => JwtError::Expired,
        ErrorKind::InvalidIssuer => JwtError::IssuerMismatch,
        _ => JwtError::Invalid(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs()
    }

    fn hmac_cfg(tenant: &str, secret: &[u8], issuer: &str) -> JwtConfig {
        let mut per_tenant = HashMap::new();
        per_tenant.insert(
            tenant.to_string(),
            TenantJwtConfig::Hmac {
                secret: secret.to_vec(),
                issuer: issuer.to_string(),
                audience: None,
            },
        );
        JwtConfig { per_tenant }
    }

    fn mint(secret: &[u8], claims: &AgentDbClaims) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(secret),
        )
        .expect("encode jwt for test")
    }

    #[tokio::test]
    async fn hmac_happy_path_returns_claims() {
        let secret = b"super-secret-key";
        let cfg = hmac_cfg("t-1", secret, "https://issuer.example");
        let claims = AgentDbClaims {
            iss: "https://issuer.example".into(),
            aud: None,
            sub: "user-1".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now_secs() + 3600,
        };
        let token = mint(secret, &claims);

        let out = verify_for_tenant(&cfg, "t-1", &token)
            .await
            .expect("verify ok");
        assert_eq!(out.sub, "user-1");
        assert_eq!(out.tenant, "t-1");
    }

    #[tokio::test]
    async fn rejects_expired_token() {
        let secret = b"k";
        let cfg = hmac_cfg("t-1", secret, "iss");
        // `jsonwebtoken` allows a default 60s leeway on `exp`; push
        // well past it so the test is deterministic.
        let claims = AgentDbClaims {
            iss: "iss".into(),
            aud: None,
            sub: "u".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now_secs() - 3600,
        };
        let token = mint(secret, &claims);
        let err = verify_for_tenant(&cfg, "t-1", &token)
            .await
            .expect_err("expired");
        assert!(matches!(err, JwtError::Expired), "got {err:?}");
    }

    #[tokio::test]
    async fn rejects_wrong_issuer() {
        let secret = b"k";
        let cfg = hmac_cfg("t-1", secret, "expected-iss");
        let claims = AgentDbClaims {
            iss: "other-iss".into(),
            aud: None,
            sub: "u".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now_secs() + 3600,
        };
        let token = mint(secret, &claims);
        let err = verify_for_tenant(&cfg, "t-1", &token)
            .await
            .expect_err("issuer mismatch");
        assert!(matches!(err, JwtError::IssuerMismatch), "got {err:?}");
    }

    #[tokio::test]
    async fn rejects_tenant_claim_mismatch() {
        let secret = b"k";
        // Config keyed under t-1 with the issuer; we'll mint a
        // token whose `tenant` claim is t-2 and verify against
        // t-1 — signature is valid, but cross-tenant attempt.
        let cfg = hmac_cfg("t-1", secret, "iss");
        let claims = AgentDbClaims {
            iss: "iss".into(),
            aud: None,
            sub: "u".into(),
            tenant: "t-2".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            exp: now_secs() + 3600,
        };
        let token = mint(secret, &claims);
        let err = verify_for_tenant(&cfg, "t-1", &token)
            .await
            .expect_err("tenant mismatch");
        assert!(matches!(err, JwtError::TenantMismatch), "got {err:?}");
    }

    #[test]
    fn sessions_claim_wildcard_allows_any() {
        let s = SessionsClaim::Wildcard("*".into());
        assert!(s.allows("anything"));
        assert!(s.allows(""));

        // Defensive: a non-"*" wildcard string must NOT pass.
        let bogus = SessionsClaim::Wildcard("all".into());
        assert!(!bogus.allows("anything"));
    }

    #[test]
    fn sessions_claim_list_only_allows_listed() {
        let s = SessionsClaim::List(vec!["s-1".into(), "s-2".into()]);
        assert!(s.allows("s-1"));
        assert!(s.allows("s-2"));
        assert!(!s.allows("s-3"));
    }

    #[tokio::test]
    async fn tenant_not_configured_returns_error() {
        let cfg = JwtConfig::default();
        // Token contents don't matter — config lookup fails first.
        let err = verify_for_tenant(&cfg, "t-x", "irrelevant.jwt.token")
            .await
            .expect_err("tenant not configured");
        assert!(matches!(err, JwtError::TenantNotConfigured), "got {err:?}");
    }
}
