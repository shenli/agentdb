//! User accounts with SCRAM-SHA-256 credentials (M19).
//!
//! A user belongs to exactly one tenant. At creation time the
//! password runs through [`crate::scram::derive_credentials`];
//! only the derived `(iterations, salt, StoredKey, ServerKey)`
//! tuple lands on disk.
//!
//! For auth, callers fetch the user by id via [`get`] and verify
//! a SCRAM client proof against the stored credentials using
//! [`crate::scram::verify_client_proof`].

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};
use crate::scram::{self, Credentials};

/// Row from the `users` table, with credentials already decoded
/// from the base64 columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub user_id: String,
    pub tenant_external: String,
    pub credentials: Credentials,
    pub created_at_ms: i64,
}

/// Lightweight "who exists" view for list endpoints. Omits
/// credential fields — they aren't meant to leave this process
/// over admin surfaces.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserSummary {
    pub user_id: String,
    pub tenant_external: String,
    pub created_at_ms: i64,
}

/// Create a new user. Derives SCRAM credentials from `password`
/// internally (the plaintext never touches disk). Errors if
/// `user_id` already exists.
///
/// Returns the created user with credentials populated — useful
/// for tests that want to round-trip a handshake without
/// re-fetching.
pub async fn create(
    pool: &AnyPool,
    user_id: &str,
    tenant_external: &str,
    password: &str,
    iterations: u32,
    now_ms: i64,
) -> MetaResult<User> {
    let creds = scram::derive_credentials(password, iterations);
    let salt_b64 = B64.encode(&creds.salt);
    let stored_b64 = B64.encode(creds.stored_key);
    let server_b64 = B64.encode(creds.server_key);
    let result = sqlx::query(
        "INSERT INTO users
             (user_id, tenant_external, iterations,
              salt_b64, stored_key_b64, server_key_b64, created_at_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(tenant_external)
    .bind(iterations as i64)
    .bind(&salt_b64)
    .bind(&stored_b64)
    .bind(&server_b64)
    .bind(now_ms)
    .execute(pool)
    .await;
    match result {
        Ok(_) => Ok(User {
            user_id: user_id.to_string(),
            tenant_external: tenant_external.to_string(),
            credentials: creds,
            created_at_ms: now_ms,
        }),
        Err(e) => {
            // SQLite reports UNIQUE violations with code 2067 or
            // message containing "UNIQUE". Map to a typed error
            // so admin endpoints can 409 appropriately.
            let msg = e.to_string();
            if msg.contains("UNIQUE") {
                Err(MetaError::Conflict(format!(
                    "user {user_id} already exists"
                )))
            } else {
                Err(e.into())
            }
        }
    }
}

/// Look up a user by id. Returns `None` if absent. Decodes the
/// base64 credential columns; a malformed row (shouldn't happen
/// unless the DB was tampered with) yields a descriptive
/// `MetaError`.
pub async fn get(pool: &AnyPool, user_id: &str) -> MetaResult<Option<User>> {
    let row: Option<(String, String, i64, String, String, String, i64)> = sqlx::query_as(
        "SELECT user_id, tenant_external, iterations,
                salt_b64, stored_key_b64, server_key_b64, created_at_ms
         FROM users WHERE user_id = ?",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    match row {
        None => Ok(None),
        Some((uid, tid, iter, salt_b64, stored_b64, server_b64, created)) => {
            let creds = decode_credentials(iter as u32, &salt_b64, &stored_b64, &server_b64)?;
            Ok(Some(User {
                user_id: uid,
                tenant_external: tid,
                credentials: creds,
                created_at_ms: created,
            }))
        }
    }
}

/// Every user, ordered by `created_at_ms` ascending so newly-
/// added users appear at the tail. Returns summaries only —
/// credentials never leave this crate.
pub async fn list(pool: &AnyPool) -> MetaResult<Vec<UserSummary>> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT user_id, tenant_external, created_at_ms
           FROM users ORDER BY created_at_ms ASC, user_id ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(user_id, tenant_external, created_at_ms)| UserSummary {
            user_id,
            tenant_external,
            created_at_ms,
        })
        .collect())
}

/// Delete a user. Idempotent: deleting a nonexistent user is
/// not an error — returns `false`. Returns `true` on actual
/// deletion so admin endpoints can distinguish 204 vs. 404.
pub async fn delete(pool: &AnyPool, user_id: &str) -> MetaResult<bool> {
    let n = sqlx::query("DELETE FROM users WHERE user_id = ?")
        .bind(user_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

fn decode_credentials(
    iterations: u32,
    salt_b64: &str,
    stored_b64: &str,
    server_b64: &str,
) -> MetaResult<Credentials> {
    let salt = B64
        .decode(salt_b64)
        .map_err(|e| MetaError::Invariant(format!("salt_b64 decode: {e}")))?;
    let stored = B64
        .decode(stored_b64)
        .map_err(|e| MetaError::Invariant(format!("stored_key_b64 decode: {e}")))?;
    let server = B64
        .decode(server_b64)
        .map_err(|e| MetaError::Invariant(format!("server_key_b64 decode: {e}")))?;
    if stored.len() != scram::HASH_LEN || server.len() != scram::HASH_LEN {
        return Err(MetaError::Invariant(
            "stored/server key wrong length".into(),
        ));
    }
    let mut stored_key = [0u8; scram::HASH_LEN];
    stored_key.copy_from_slice(&stored);
    let mut server_key = [0u8; scram::HASH_LEN];
    server_key.copy_from_slice(&server);
    Ok(Credentials {
        iterations,
        salt,
        stored_key,
        server_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn create_and_get_roundtrip() {
        let p = pool().await;
        let created = create(&p, "alice", "t-alice", "pencil", 4096, 100)
            .await
            .unwrap();
        let fetched = get(&p, "alice").await.unwrap().unwrap();
        assert_eq!(created.user_id, fetched.user_id);
        assert_eq!(created.tenant_external, fetched.tenant_external);
        assert_eq!(created.credentials, fetched.credentials);
        assert_eq!(fetched.created_at_ms, 100);
    }

    #[tokio::test]
    async fn duplicate_user_id_errors() {
        let p = pool().await;
        create(&p, "alice", "t-alice", "pencil", 4096, 100)
            .await
            .unwrap();
        let err = create(&p, "alice", "t-alice", "other", 4096, 200)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_user() {
        let p = pool().await;
        assert!(get(&p, "ghost").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_all_users_ordered_by_created() {
        let p = pool().await;
        create(&p, "bob", "t-b", "pw", 4096, 200).await.unwrap();
        create(&p, "alice", "t-a", "pw", 4096, 100).await.unwrap();
        create(&p, "carol", "t-c", "pw", 4096, 300).await.unwrap();
        let all = list(&p).await.unwrap();
        let ids: Vec<_> = all.iter().map(|u| u.user_id.as_str()).collect();
        assert_eq!(ids, ["alice", "bob", "carol"]);
    }

    #[tokio::test]
    async fn delete_removes_user_and_is_idempotent() {
        let p = pool().await;
        create(&p, "alice", "t-a", "pw", 4096, 100).await.unwrap();
        assert!(delete(&p, "alice").await.unwrap());
        assert!(get(&p, "alice").await.unwrap().is_none());
        // Second delete is a no-op, reports false.
        assert!(!delete(&p, "alice").await.unwrap());
    }
}
