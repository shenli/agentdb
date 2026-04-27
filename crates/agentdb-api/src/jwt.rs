//! JWT verification for browser-direct subscribers (spec § 6.13).
//!
//! Per-tenant config supplies either an HMAC secret (HS256) or a JWKS URL
//! (RS256, RS384, RS512, ES256). Used by Task 9c's subscribe middleware.
//! Task 9d wires JWT-expiry close behavior into the live SSE stream.

use std::collections::HashMap;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use self::jwks_cache::JwksCache;

/// Per-tenant JWT verification config.
///
/// - `Hmac`: HS256 with a shared secret. Suitable for Supabase Auth /
///   Auth0 setups that use a symmetric signing key.
/// - `Jwks`: fetches the issuer's JWKS for RS256 (the headline path —
///   most cloud IdPs sign with RSA). The keyset is cached in a
///   process-global TTL cache keyed by URL; see [`jwks_cache`].
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
    /// Phase 2: channel access for ephemeral broadcast.
    /// `None` (claim absent) — token has no broadcast access.
    /// `Some(Wildcard("*"))` — any channel in the tenant.
    /// `Some(List([...]))` — whitelist of channel names.
    /// See spec § 5.3.
    #[serde(default)]
    pub channels: Option<SessionsClaim>,
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
        TenantJwtConfig::Jwks {
            url,
            issuer,
            audience,
        } => {
            // The header tells us which key (`kid`) and which alg
            // signed the token. We pull the keyset from the cache
            // (fetching only on miss / TTL expiry), look up by
            // `kid`, then verify with `header.alg`.
            let header = jsonwebtoken::decode_header(token).map_err(map_err)?;
            let kid = header
                .kid
                .clone()
                .ok_or_else(|| JwtError::Invalid("missing kid".into()))?;
            // Restrict to algs we actually support. JWKS only
            // serves asymmetric keys, so reject HS* up front to
            // avoid the classic "alg=HS256 with public key as
            // secret" attack.
            match header.alg {
                Algorithm::RS256
                | Algorithm::RS384
                | Algorithm::RS512
                | Algorithm::ES256 => {}
                other => {
                    return Err(JwtError::Invalid(format!(
                        "alg {other:?} not allowed for jwks"
                    )));
                }
            }
            let jwks = JWKS_CACHE
                .fetch(url)
                .await
                .map_err(|e| JwtError::Invalid(format!("jwks fetch: {e}")))?;
            let key = jwks
                .find(&kid)
                .ok_or_else(|| JwtError::Invalid(format!("unknown kid {kid}")))?;
            let mut v = Validation::new(header.alg);
            v.set_issuer(&[issuer.as_str()]);
            if let Some(aud) = audience {
                v.set_audience(&[aud.as_str()]);
            } else {
                v.validate_aud = false;
            }
            v.validate_exp = true;
            let data = decode::<AgentDbClaims>(token, key.decoding_key(), &v).map_err(map_err)?;
            if data.claims.tenant != tenant {
                return Err(JwtError::TenantMismatch);
            }
            Ok(data.claims)
        }
    }
}

/// Process-global JWKS cache. Keyed by URL so multi-tenant
/// deployments with different IdPs share the cache safely.
/// 10-minute TTL matches the spec default.
static JWKS_CACHE: once_cell::sync::Lazy<JwksCache> =
    once_cell::sync::Lazy::new(|| JwksCache::new(std::time::Duration::from_secs(600)));

fn map_err(e: jsonwebtoken::errors::Error) -> JwtError {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => JwtError::Expired,
        ErrorKind::InvalidIssuer => JwtError::IssuerMismatch,
        _ => JwtError::Invalid(e.to_string()),
    }
}

/// JWKS keyset cache. Private — exposed only to `verify_for_tenant`
/// via `JWKS_CACHE`. Keyed by URL so different tenants pointing at
/// different IdPs don't share entries.
mod jwks_cache {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::RwLock;

    /// Error type for cache fetches. We just stringify the cause —
    /// the caller in `verify_for_tenant` wraps it in
    /// `JwtError::Invalid("jwks fetch: ...")` regardless.
    pub(super) type FetchError = Box<dyn std::error::Error + Send + Sync>;

    pub(super) struct JwksCache {
        ttl: Duration,
        // `Arc<Jwks>` so callers can hold the keyset across awaits
        // without locking the cache.
        entries: RwLock<HashMap<String, (Instant, Arc<Jwks>)>>,
    }

    pub(super) struct Jwks {
        keys: HashMap<String, JwksKey>,
    }

    pub(super) struct JwksKey {
        decoding_key: jsonwebtoken::DecodingKey,
    }

    impl JwksKey {
        pub(super) fn decoding_key(&self) -> &jsonwebtoken::DecodingKey {
            &self.decoding_key
        }
    }

    impl Jwks {
        pub(super) fn find(&self, kid: &str) -> Option<&JwksKey> {
            self.keys.get(kid)
        }
    }

    impl JwksCache {
        pub(super) fn new(ttl: Duration) -> Self {
            Self {
                ttl,
                entries: RwLock::new(HashMap::new()),
            }
        }

        /// Returns a cached keyset for `url` if fresh, else refetches.
        ///
        /// Two requests racing on a cold cache will both fetch — that's
        /// fine for our load profile (subscribe is rare on cold paths)
        /// and avoids the complexity of a per-URL singleflight.
        pub(super) async fn fetch(&self, url: &str) -> Result<Arc<Jwks>, FetchError> {
            {
                let g = self.entries.read().await;
                if let Some((fetched_at, jwks)) = g.get(url) {
                    if fetched_at.elapsed() < self.ttl {
                        return Ok(jwks.clone());
                    }
                }
            }
            let body: serde_json::Value = reqwest::get(url).await?.json().await?;
            let mut keys = HashMap::new();
            let empty = Vec::new();
            for k in body["keys"].as_array().unwrap_or(&empty) {
                let Some(kid) = k["kid"].as_str() else {
                    continue;
                };
                let kty = k["kty"].as_str().unwrap_or("");
                // RSA covers RS256/RS384/RS512. EC support deferred —
                // jsonwebtoken can do it but the JWK has `crv`+`x`+`y`
                // instead of `n`+`e` and we'd need a per-curve branch.
                if kty != "RSA" {
                    continue;
                }
                let n = k["n"].as_str().unwrap_or("");
                let e = k["e"].as_str().unwrap_or("");
                let dk = jsonwebtoken::DecodingKey::from_rsa_components(n, e)?;
                keys.insert(kid.to_string(), JwksKey { decoding_key: dk });
            }
            let jwks = Arc::new(Jwks { keys });
            self.entries
                .write()
                .await
                .insert(url.to_string(), (Instant::now(), jwks.clone()));
            Ok(jwks)
        }
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
            channels: None,
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
            channels: None,
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
            channels: None,
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
            channels: None,
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

    #[test]
    fn channels_claim_defaults_to_none() {
        let raw = r#"{
            "iss": "https://auth.test",
            "sub": "u",
            "tenant": "t-1",
            "sessions": "*",
            "exp": 9999999999
        }"#;
        let claims: AgentDbClaims = serde_json::from_str(raw).unwrap();
        assert!(claims.channels.is_none());
    }

    #[test]
    fn channels_claim_wildcard_parses() {
        let raw = r#"{
            "iss": "i", "sub": "s", "tenant": "t-1",
            "sessions": "*", "channels": "*", "exp": 9999999999
        }"#;
        let claims: AgentDbClaims = serde_json::from_str(raw).unwrap();
        let ch = claims.channels.unwrap();
        assert!(matches!(ch, SessionsClaim::Wildcard(ref s) if s == "*"));
        assert!(ch.allows("anything"));
    }

    #[test]
    fn channels_claim_list_parses_and_filters() {
        let raw = r#"{
            "iss": "i", "sub": "s", "tenant": "t-1",
            "sessions": "*", "channels": ["a", "b"], "exp": 9999999999
        }"#;
        let claims: AgentDbClaims = serde_json::from_str(raw).unwrap();
        let ch = claims.channels.unwrap();
        assert!(ch.allows("a"));
        assert!(ch.allows("b"));
        assert!(!ch.allows("c"));
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

    // -------- JWKS tests (Task 9b) --------
    //
    // The runtime path we exercise:
    //   1. mint an RS256 JWT signed with a freshly generated RSA key
    //   2. spawn a tiny axum stub serving a JWKS doc with the pub key
    //   3. point a tenant config at `http://127.0.0.1:<port>/jwks`
    //   4. verify_for_tenant -> JWKS_CACHE.fetch -> decode -> claims
    //
    // RSA keygen is the slow bit (~1–2s on a laptop). Acceptable.

    fn b64u(bytes: &[u8]) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Generate an RSA keypair and return (private PEM, JWK n, JWK e).
    fn rsa_keypair() -> (String, String, String) {
        use rsa::pkcs8::EncodePrivateKey;
        use rsa::traits::PublicKeyParts;
        use rsa::{RsaPrivateKey, RsaPublicKey};

        let mut rng = rand::thread_rng();
        // 2048 is the smallest size most IdPs ship; matches what
        // we'd see in production. Test takes a couple seconds.
        let priv_key = RsaPrivateKey::new(&mut rng, 2048).expect("rsa keygen");
        let pub_key = RsaPublicKey::from(&priv_key);
        let n = b64u(&pub_key.n().to_bytes_be());
        let e = b64u(&pub_key.e().to_bytes_be());
        let pem = priv_key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .expect("pkcs8 pem")
            .to_string();
        (pem, n, e)
    }

    fn mint_rs256(pem: &str, kid: &str, claims: &AgentDbClaims) -> String {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(kid.to_string());
        jsonwebtoken::encode(
            &header,
            claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).expect("rsa pem"),
        )
        .expect("encode rs256")
    }

    #[tokio::test]
    async fn jwks_happy_path_with_local_stub_server() {
        let (pem, n, e) = rsa_keypair();
        let kid = "k1".to_string();

        let jwks_doc = serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "alg": "RS256",
                "kid": kid,
                "use": "sig",
                "n": n,
                "e": e,
            }]
        });

        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let doc = jwks_doc.clone();
                async move { axum::Json(doc) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve jwks");
        });

        let claims = AgentDbClaims {
            iss: "https://auth.example.com".into(),
            aud: Some("agentdb".into()),
            sub: "u".into(),
            tenant: "t-1".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            channels: None,
            exp: now_secs() + 3600,
        };
        let token = mint_rs256(&pem, &kid, &claims);

        let mut per_tenant = HashMap::new();
        per_tenant.insert(
            "t-1".to_string(),
            TenantJwtConfig::Jwks {
                url: format!("http://{}/jwks", addr),
                issuer: "https://auth.example.com".into(),
                audience: Some("agentdb".into()),
            },
        );
        let cfg = JwtConfig { per_tenant };

        let out = verify_for_tenant(&cfg, "t-1", &token)
            .await
            .expect("verify ok");
        assert_eq!(out.sub, "u");
        assert_eq!(out.tenant, "t-1");
    }

    #[tokio::test]
    async fn jwks_caches_keyset() {
        // The cache is keyed by URL. We swap the JWKS body on the
        // server after the first verify; the second verify should
        // still succeed against the *original* key because the
        // cache TTL hasn't elapsed. If the cache silently re-fetched
        // every call, the second verify would fail with "unknown
        // kid" (the new doc only has kid=k2).

        use std::sync::{Arc, Mutex};

        let (pem, n, e) = rsa_keypair();

        // Initial doc has kid=k1. We later flip to kid=k2.
        let initial = serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "alg": "RS256",
                "kid": "k1",
                "use": "sig",
                "n": n,
                "e": e,
            }]
        });
        let body: Arc<Mutex<serde_json::Value>> = Arc::new(Mutex::new(initial));
        let body_for_route = body.clone();

        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let body = body_for_route.clone();
                async move {
                    let doc = body.lock().expect("jwks body mutex").clone();
                    axum::Json(doc)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve jwks");
        });

        let url = format!("http://{}/jwks", addr);
        let claims = AgentDbClaims {
            iss: "https://auth.example.com".into(),
            aud: None,
            sub: "u".into(),
            tenant: "t-cache".into(),
            sessions: SessionsClaim::Wildcard("*".into()),
            channels: None,
            exp: now_secs() + 3600,
        };
        let token = mint_rs256(&pem, "k1", &claims);

        let mut per_tenant = HashMap::new();
        per_tenant.insert(
            "t-cache".to_string(),
            TenantJwtConfig::Jwks {
                url: url.clone(),
                issuer: "https://auth.example.com".into(),
                audience: None,
            },
        );
        let cfg = JwtConfig { per_tenant };

        // 1st verify: cold cache, fetches the initial doc.
        verify_for_tenant(&cfg, "t-cache", &token)
            .await
            .expect("first verify ok");

        // Mutate the server's JWKS doc — different kid, no k1.
        // Even the new key material is bogus; if the cache
        // re-fetched, decoding the token would fail because the
        // new key wouldn't match the token's signature.
        *body.lock().expect("jwks body mutex") = serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "alg": "RS256",
                "kid": "k2",
                "use": "sig",
                "n": "AAAA",
                "e": "AQAB",
            }]
        });

        // 2nd verify: TTL is 600s — cache hit, original keyset wins.
        verify_for_tenant(&cfg, "t-cache", &token)
            .await
            .expect("second verify still uses cached keyset");
    }
}
