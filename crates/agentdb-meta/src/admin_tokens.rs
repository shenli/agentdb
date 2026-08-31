//! Admin-plane bearer tokens.
//!
//! Mint returns the plaintext token **once**; disk carries
//! only the SHA-256 hash. Token format is `agentdb_<43 base64
//! chars>` — 32 random bytes + a human-readable prefix so a
//! leaked token is obvious in a log line.
//!
//! Revocation is soft: the row stays, `revoked_at_ms` fills
//! in. Auth checks filter on `revoked_at_ms IS NULL`. This
//! preserves audit history (operators can answer "when was
//! token X revoked, and by whom?" — though we don't track
//! who, that's follow-up).

use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

/// Token plaintext prefix. Humans eyeballing a log line should
/// spot this and redact before posting.
pub const TOKEN_PREFIX: &str = "agentdb_";

/// Size of the random-bytes portion of a token. 32 bytes =
/// 2^256 entropy — the hash scheme (SHA-256) is irrelevant to
/// brute-force cost at this size.
pub const TOKEN_BYTES: usize = 32;

/// Scope of an admin token — what the bearer is allowed to do.
/// Checked by the admin-auth middleware against the HTTP
/// method of the incoming request.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AdminScope {
    /// Full access: every `/api/admin/*` endpoint, any method.
    #[default]
    Admin,
    /// Read-only: only `GET` against `/api/admin/*`. Mutations
    /// (POST/PUT/DELETE) return 403.
    Readonly,
    /// Data-plane access to exactly one tenant. The bound tenant id is
    /// stored alongside the token; the API middleware rejects any request
    /// outside `/api/tenants/{that_tenant}/...`.
    Tenant,
}

impl AdminScope {
    pub fn as_str(self) -> &'static str {
        match self {
            AdminScope::Admin => "admin",
            AdminScope::Readonly => "readonly",
            AdminScope::Tenant => "tenant",
        }
    }

    /// Parse from the persisted string form. Unknown values fail closed to
    /// read-only so corrupted or forward-version metadata never grants write
    /// access accidentally.
    pub fn from_stored(s: &str) -> Self {
        match s {
            "admin" => AdminScope::Admin,
            "readonly" => AdminScope::Readonly,
            "tenant" => AdminScope::Tenant,
            _ => AdminScope::Readonly,
        }
    }
}

/// Summary of a minted token. Never carries the plaintext.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TokenSummary {
    pub token_id: String,
    pub description: String,
    pub created_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub revoked_at_ms: Option<i64>,
    #[serde(default)]
    pub scope: AdminScope,
}

/// Return of [`mint`]. The `plaintext` field is the ONLY time
/// the raw token is ever available — callers must hand it to
/// the operator or fail closed.
#[derive(Debug)]
pub struct MintedToken {
    pub token_id: String,
    pub plaintext: String,
    pub summary: TokenSummary,
}

/// Create a new token. Server generates the random bytes so
/// callers can't accidentally mint weak tokens. `description`
/// is free-form — operators use it to remember what a token
/// is for ("grafana", "alice"). `scope` gates what the token
/// can do; see [`AdminScope`].
pub async fn mint(
    pool: &AnyPool,
    description: &str,
    scope: AdminScope,
    now_ms: i64,
) -> MetaResult<MintedToken> {
    mint_scoped(pool, description, scope, None, now_ms).await
}

/// `mint` with an optional tenant binding: required for `Tenant` scope,
/// rejected for the others.
pub async fn mint_scoped(
    pool: &AnyPool,
    description: &str,
    scope: AdminScope,
    tenant_id: Option<&str>,
    now_ms: i64,
) -> MetaResult<MintedToken> {
    match (scope, tenant_id) {
        (AdminScope::Tenant, None) => {
            return Err(crate::error::MetaError::Conflict(
                "tenant-scoped tokens require a tenant_id".into(),
            ))
        }
        (AdminScope::Admin | AdminScope::Readonly, Some(_)) => {
            return Err(crate::error::MetaError::Conflict(
                "tenant_id is only valid for tenant-scoped tokens".into(),
            ))
        }
        _ => {}
    }
    let mut raw = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut raw);
    let plaintext = format!("{TOKEN_PREFIX}{}", B64URL.encode(raw));
    let hash_b64 = B64.encode(sha256(plaintext.as_bytes()));
    // `token_id` is a 12-character URL-safe prefix of the hash — an opaque
    // stable identifier operators can use to revoke without putting the full
    // plaintext into CLI history.
    let token_id = short_id(&hash_b64);

    sqlx::query(
        "INSERT INTO admin_tokens (token_id, token_hash_b64, description, created_at_ms, revoked_at_ms, scope, tenant_id)
             VALUES ($1, $2, $3, $4, NULL, $5, $6)",
    )
    .bind(&token_id)
    .bind(&hash_b64)
    .bind(description)
    .bind(now_ms)
    .bind(scope.as_str())
    .bind(tenant_id)
    .execute(pool)
    .await?;

    Ok(MintedToken {
        summary: TokenSummary {
            token_id: token_id.clone(),
            description: description.to_string(),
            created_at_ms: now_ms,
            tenant_id: tenant_id.map(str::to_string),
            revoked_at_ms: None,
            scope,
        },
        token_id,
        plaintext,
    })
}

/// Verify a presented plaintext against the indexed SHA-256 hash. Returns
/// `Ok(Some(token_id))` when matched and `Ok(None)` when unknown or revoked.
/// Tokens contain 256 random bits, so querying by their digest does not turn
/// a low-entropy secret into an offline oracle and keeps request cost bounded.
/// A live (non-revoked) token matched by [`verify`].
#[derive(Debug, Clone)]
pub struct VerifiedToken {
    pub token_id: String,
    pub scope: AdminScope,
    /// Bound tenant; always `Some` when `scope` is [`AdminScope::Tenant`].
    pub tenant_id: Option<String>,
}

pub async fn verify(pool: &AnyPool, presented: &str) -> MetaResult<Option<VerifiedToken>> {
    let candidate = B64.encode(sha256(presented.as_bytes()));
    let matched: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT token_id, scope, tenant_id
           FROM admin_tokens
          WHERE token_hash_b64 = $1 AND revoked_at_ms IS NULL",
    )
    .bind(candidate)
    .fetch_optional(pool)
    .await?;
    Ok(matched.and_then(|(token_id, scope, tenant_id)| {
        let scope = AdminScope::from_stored(&scope);
        // Fail closed: a tenant-scoped row without its binding must never
        // authenticate as anything.
        if scope == AdminScope::Tenant && tenant_id.is_none() {
            return None;
        }
        Some(VerifiedToken {
            token_id,
            scope,
            tenant_id,
        })
    }))
}

type ActiveTokenRow = (String, String, i64, Option<i64>, String, Option<String>);

/// Every non-revoked token, ordered by `created_at_ms`.
pub async fn list_active(pool: &AnyPool) -> MetaResult<Vec<TokenSummary>> {
    let rows: Vec<ActiveTokenRow> = sqlx::query_as(
        "SELECT token_id, description, created_at_ms, revoked_at_ms, scope, tenant_id
           FROM admin_tokens
          WHERE revoked_at_ms IS NULL
          ORDER BY created_at_ms ASC, token_id ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, desc, created, revoked, scope, tenant_id)| TokenSummary {
                token_id: id,
                description: desc,
                created_at_ms: created,
                tenant_id,
                revoked_at_ms: revoked,
                scope: AdminScope::from_stored(&scope),
            },
        )
        .collect())
}

/// Mark a token revoked. Idempotent: revoking an already-
/// revoked or nonexistent token returns `false`.
pub async fn revoke(pool: &AnyPool, token_id: &str, now_ms: i64) -> MetaResult<bool> {
    let n = sqlx::query(
        "UPDATE admin_tokens SET revoked_at_ms = $1
           WHERE token_id = $2 AND revoked_at_ms IS NULL",
    )
    .bind(now_ms)
    .bind(token_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// Count of tokens that are currently active (not revoked).
/// Used by the auth middleware's bootstrap path — the first
/// mint is allowed without auth when this returns 0.
pub async fn count_active(pool: &AnyPool) -> MetaResult<i64> {
    let (n,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM admin_tokens WHERE revoked_at_ms IS NULL")
            .fetch_one(pool)
            .await?;
    Ok(n)
}

/// Number of token rows ever minted, including revoked rows. Bootstrap uses
/// this count so revoking the final active credential cannot silently reopen
/// the unauthenticated first-run path.
pub async fn count_all(pool: &AnyPool) -> MetaResult<i64> {
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM admin_tokens")
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// Stable cache key for a presented token: the same SHA-256/base64 form the
/// database stores, so callers can memoize verify() results without holding
/// plaintext credentials in memory.
pub fn fingerprint(presented: &str) -> String {
    B64.encode(sha256(presented.as_bytes()))
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let bytes = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

fn short_id(hash_b64: &str) -> String {
    // Re-encode the stored standard-base64 hash as URL-safe base64 before
    // truncating. Standard base64 can place `/` in an ID, which makes the ID
    // impossible to address as a single HTTP path segment.
    let hash = B64
        .decode(hash_b64)
        .expect("admin token hash is valid standard base64");
    let url_safe_hash = B64URL.encode(hash);
    // Take the first 12 characters of the URL-safe hash. Long
    // enough to be unique across any plausible cluster; short
    // enough for a CLI `revoke` argument.
    url_safe_hash.chars().take(12).collect()
}

/// Expose `MetaError` path for callers that want to map "not
/// found" from revoke() idempotence. Today no caller needs it.
#[allow(dead_code)]
fn _hush_unused() -> MetaError {
    MetaError::NotFound("".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn mint_returns_plaintext_and_persists_hash() {
        let p = pool().await;
        let minted = mint(&p, "bootstrap", AdminScope::Admin, 100).await.unwrap();
        assert!(minted.plaintext.starts_with(TOKEN_PREFIX));
        // Plaintext never hits the DB.
        let (rows,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM admin_tokens WHERE token_hash_b64 = $1")
                .bind(B64.encode(sha256(minted.plaintext.as_bytes())))
                .fetch_one(&p)
                .await
                .unwrap();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn verify_by_presented_plaintext_returns_token_id_and_scope() {
        let p = pool().await;
        let minted = mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        let found = verify(&p, &minted.plaintext).await.unwrap().unwrap();
        assert_eq!(found.token_id, minted.token_id);
        assert_eq!(found.scope, AdminScope::Admin);
        assert_eq!(found.tenant_id, None);
    }

    #[tokio::test]
    async fn verify_rejects_unknown_token() {
        let p = pool().await;
        mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        let found = verify(&p, "agentdb_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn verify_rejects_revoked_token() {
        let p = pool().await;
        let minted = mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        assert!(revoke(&p, &minted.token_id, 200).await.unwrap());
        let found = verify(&p, &minted.plaintext).await.unwrap();
        assert!(found.is_none(), "revoked tokens must not verify");
    }

    #[tokio::test]
    async fn list_active_excludes_revoked() {
        let p = pool().await;
        let a = mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        let b = mint(&p, "bob", AdminScope::Admin, 200).await.unwrap();
        revoke(&p, &b.token_id, 300).await.unwrap();
        let active = list_active(&p).await.unwrap();
        let ids: Vec<_> = active.iter().map(|t| t.token_id.as_str()).collect();
        assert_eq!(ids, vec![a.token_id.as_str()]);
    }

    #[tokio::test]
    async fn revoke_is_idempotent() {
        let p = pool().await;
        let t = mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        assert!(revoke(&p, &t.token_id, 200).await.unwrap());
        assert!(!revoke(&p, &t.token_id, 300).await.unwrap());
        assert!(!revoke(&p, "nonexistent", 400).await.unwrap());
    }

    #[tokio::test]
    async fn count_active_tracks_mints_and_revokes() {
        let p = pool().await;
        assert_eq!(count_active(&p).await.unwrap(), 0);
        assert_eq!(count_all(&p).await.unwrap(), 0);
        let a = mint(&p, "a", AdminScope::Admin, 100).await.unwrap();
        let _b = mint(&p, "b", AdminScope::Admin, 200).await.unwrap();
        assert_eq!(count_active(&p).await.unwrap(), 2);
        assert_eq!(count_all(&p).await.unwrap(), 2);
        revoke(&p, &a.token_id, 300).await.unwrap();
        assert_eq!(count_active(&p).await.unwrap(), 1);
        assert_eq!(count_all(&p).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn readonly_scope_roundtrips_through_list() {
        let p = pool().await;
        let _ = mint(&p, "grafana", AdminScope::Readonly, 100)
            .await
            .unwrap();
        let _ = mint(&p, "cron", AdminScope::Admin, 200).await.unwrap();
        let active = list_active(&p).await.unwrap();
        let scopes: std::collections::HashMap<&str, AdminScope> = active
            .iter()
            .map(|t| (t.description.as_str(), t.scope))
            .collect();
        assert_eq!(scopes.get("grafana").copied(), Some(AdminScope::Readonly));
        assert_eq!(scopes.get("cron").copied(), Some(AdminScope::Admin));
    }

    #[tokio::test]
    async fn verify_returns_the_token_s_scope() {
        let p = pool().await;
        let r = mint(&p, "r", AdminScope::Readonly, 10).await.unwrap();
        let a = mint(&p, "a", AdminScope::Admin, 20).await.unwrap();
        let v_r = verify(&p, &r.plaintext).await.unwrap().unwrap();
        let v_a = verify(&p, &a.plaintext).await.unwrap().unwrap();
        assert_eq!(v_r.scope, AdminScope::Readonly);
        assert_eq!(v_a.scope, AdminScope::Admin);
    }

    #[tokio::test]
    async fn tenant_scope_roundtrips_binding_and_validates_mint() {
        let p = pool().await;
        let minted = mint_scoped(&p, "workspace", AdminScope::Tenant, Some("t_1"), 100)
            .await
            .unwrap();
        assert_eq!(minted.summary.tenant_id.as_deref(), Some("t_1"));
        let found = verify(&p, &minted.plaintext).await.unwrap().unwrap();
        assert_eq!(found.scope, AdminScope::Tenant);
        assert_eq!(found.tenant_id.as_deref(), Some("t_1"));
        let listed = list_active(&p).await.unwrap();
        assert_eq!(listed[0].tenant_id.as_deref(), Some("t_1"));

        assert!(mint_scoped(&p, "no binding", AdminScope::Tenant, None, 200)
            .await
            .is_err());
        assert!(
            mint_scoped(&p, "admin bound", AdminScope::Admin, Some("t_1"), 300)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn tenant_row_without_binding_never_verifies() {
        let p = pool().await;
        let minted = mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        // Simulate a corrupted or hand-edited row: tenant scope, no binding.
        sqlx::query("UPDATE admin_tokens SET scope = 'tenant', tenant_id = NULL")
            .execute(&p)
            .await
            .unwrap();
        assert!(verify(&p, &minted.plaintext).await.unwrap().is_none());
    }

    #[test]
    fn unknown_persisted_scope_fails_closed() {
        assert_eq!(
            AdminScope::from_stored("future-scope"),
            AdminScope::Readonly
        );
    }
}
