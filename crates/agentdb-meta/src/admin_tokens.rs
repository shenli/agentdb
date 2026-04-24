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
use subtle::ConstantTimeEq;

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
}

impl AdminScope {
    pub fn as_str(self) -> &'static str {
        match self {
            AdminScope::Admin => "admin",
            AdminScope::Readonly => "readonly",
        }
    }

    /// Parse from the persisted string form. Unknown values map
    /// to `Admin` so an operator rolling back the code with the
    /// migrated schema still gets a functional token (the column
    /// default is `admin`, but this is belt-and-braces).
    pub fn from_stored(s: &str) -> Self {
        match s {
            "readonly" => AdminScope::Readonly,
            _ => AdminScope::Admin,
        }
    }
}

/// Summary of a minted token. Never carries the plaintext.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TokenSummary {
    pub token_id: String,
    pub description: String,
    pub created_at_ms: i64,
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
    let mut raw = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut raw);
    let plaintext = format!("{TOKEN_PREFIX}{}", B64URL.encode(raw));
    let hash_b64 = B64.encode(sha256(plaintext.as_bytes()));
    // `token_id` is the first 8 hex chars of the hash — an
    // opaque stable identifier operators can use to `revoke`
    // without pasting the full plaintext into CLI history.
    let token_id = short_id(&hash_b64);

    sqlx::query(
        "INSERT INTO admin_tokens (token_id, token_hash_b64, description, created_at_ms, revoked_at_ms, scope)
             VALUES (?, ?, ?, ?, NULL, ?)",
    )
    .bind(&token_id)
    .bind(&hash_b64)
    .bind(description)
    .bind(now_ms)
    .bind(scope.as_str())
    .execute(pool)
    .await?;

    Ok(MintedToken {
        summary: TokenSummary {
            token_id: token_id.clone(),
            description: description.to_string(),
            created_at_ms: now_ms,
            revoked_at_ms: None,
            scope,
        },
        token_id,
        plaintext,
    })
}

/// Constant-time verify a presented plaintext against the
/// stored hashes. Returns `Ok(Some(token_id))` when matched,
/// `Ok(None)` when not. Rejects revoked tokens the same way
/// as unknown — the caller can't distinguish the two.
///
/// Walks every active row and does a constant-time compare.
/// At prototype scale (handful of tokens) this is fine; a
/// real deployment would index on a prefix to cut the scan
/// down, but per-request cost is already a single atomic
/// per entry.
pub async fn verify(
    pool: &AnyPool,
    presented: &str,
) -> MetaResult<Option<(String, AdminScope)>> {
    let candidate = B64.encode(sha256(presented.as_bytes()));
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT token_id, token_hash_b64, scope FROM admin_tokens WHERE revoked_at_ms IS NULL",
    )
    .fetch_all(pool)
    .await?;
    // Walk all rows even after a match, to keep timing
    // independent of token position.
    let mut matched: Option<(String, AdminScope)> = None;
    for (id, hash, scope) in rows {
        if candidate
            .as_bytes()
            .ct_eq(hash.as_bytes())
            .unwrap_u8()
            == 1
        {
            // First match wins — the hash index is UNIQUE, so
            // there can't be two.
            if matched.is_none() {
                matched = Some((id, AdminScope::from_stored(&scope)));
            }
        }
    }
    Ok(matched)
}

/// Every non-revoked token, ordered by `created_at_ms`.
pub async fn list_active(pool: &AnyPool) -> MetaResult<Vec<TokenSummary>> {
    let rows: Vec<(String, String, i64, Option<i64>, String)> = sqlx::query_as(
        "SELECT token_id, description, created_at_ms, revoked_at_ms, scope
           FROM admin_tokens
          WHERE revoked_at_ms IS NULL
          ORDER BY created_at_ms ASC, token_id ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, desc, created, revoked, scope)| TokenSummary {
            token_id: id,
            description: desc,
            created_at_ms: created,
            revoked_at_ms: revoked,
            scope: AdminScope::from_stored(&scope),
        })
        .collect())
}

/// Mark a token revoked. Idempotent: revoking an already-
/// revoked or nonexistent token returns `false`.
pub async fn revoke(pool: &AnyPool, token_id: &str, now_ms: i64) -> MetaResult<bool> {
    let n = sqlx::query(
        "UPDATE admin_tokens SET revoked_at_ms = ?
           WHERE token_id = ? AND revoked_at_ms IS NULL",
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

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let bytes = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

fn short_id(hash_b64: &str) -> String {
    // Take the first 12 characters of the base64 hash. Long
    // enough to be unique across any plausible cluster; short
    // enough for a CLI `revoke` argument.
    hash_b64.chars().take(12).collect()
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
        let (rows,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM admin_tokens WHERE token_hash_b64 = ?",
        )
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
        assert_eq!(found.0, minted.token_id);
        assert_eq!(found.1, AdminScope::Admin);
    }

    #[tokio::test]
    async fn verify_rejects_unknown_token() {
        let p = pool().await;
        mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        let found = verify(&p, "agentdb_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
            .await
            .unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn verify_rejects_revoked_token() {
        let p = pool().await;
        let minted = mint(&p, "alice", AdminScope::Admin, 100).await.unwrap();
        assert!(revoke(&p, &minted.token_id, 200).await.unwrap());
        let found = verify(&p, &minted.plaintext).await.unwrap();
        assert_eq!(found, None, "revoked tokens must not verify");
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
        let a = mint(&p, "a", AdminScope::Admin, 100).await.unwrap();
        let _b = mint(&p, "b", AdminScope::Admin, 200).await.unwrap();
        assert_eq!(count_active(&p).await.unwrap(), 2);
        revoke(&p, &a.token_id, 300).await.unwrap();
        assert_eq!(count_active(&p).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn readonly_scope_roundtrips_through_list() {
        let p = pool().await;
        let _ = mint(&p, "grafana", AdminScope::Readonly, 100).await.unwrap();
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
        assert_eq!(v_r.1, AdminScope::Readonly);
        assert_eq!(v_a.1, AdminScope::Admin);
    }
}
