//! Tenant registry — the global mapping between external tenant id
//! (the string clients authenticate as) and the internal integer id
//! used for routing and KV keys.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub internal_id: i64,
    pub external_id: String,
    pub created_at_ms: i64,
}

/// Lookup by external id. Returns `Ok(None)` if the tenant doesn't exist.
pub async fn get_by_external(pool: &AnyPool, external_id: &str) -> MetaResult<Option<Tenant>> {
    let row: Option<(i64, String, i64)> = sqlx::query_as(
        "SELECT internal_id, external_id, created_at_ms FROM tenants WHERE external_id = ?",
    )
    .bind(external_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(i, e, t)| Tenant {
        internal_id: i,
        external_id: e,
        created_at_ms: t,
    }))
}

/// Reverse lookup: by internal id. Returns the external id that
/// the routing layer / auth layer needs to hand back to the
/// (currently external-id-keyed) per-box `TenantRegistry`. Used
/// by the M26 session-locator path to recover an external
/// tenant identity from the locator's `tenant_internal_id`.
pub async fn get_by_internal(pool: &AnyPool, internal_id: i64) -> MetaResult<Option<Tenant>> {
    let row: Option<(i64, String, i64)> = sqlx::query_as(
        "SELECT internal_id, external_id, created_at_ms FROM tenants WHERE internal_id = ?",
    )
    .bind(internal_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(i, e, t)| Tenant {
        internal_id: i,
        external_id: e,
        created_at_ms: t,
    }))
}

/// Create a new tenant. Errors with [`MetaError::Conflict`] if one
/// already exists under this external id. Use [`get_or_create`] if you
/// want idempotent creation.
pub async fn create(pool: &AnyPool, external_id: &str, now_ms: i64) -> MetaResult<Tenant> {
    // `RETURNING` works on SQLite (>=3.35) and Postgres —
    // dialect-agnostic and avoids SQLite's
    // `last_insert_rowid` (unsupported on AnyQueryResult).
    let result: Result<(i64,), _> = sqlx::query_as(
        "INSERT INTO tenants (external_id, created_at_ms) VALUES (?, ?) RETURNING internal_id",
    )
    .bind(external_id)
    .bind(now_ms)
    .fetch_one(pool)
    .await;
    match result {
        Ok((id,)) => Ok(Tenant {
            internal_id: id,
            external_id: external_id.to_string(),
            created_at_ms: now_ms,
        }),
        Err(sqlx::Error::Database(db)) if db.is_unique_violation() => Err(MetaError::Conflict(
            format!("tenant '{external_id}' already exists"),
        )),
        Err(e) => Err(MetaError::Sqlx(e)),
    }
}

/// Idempotent: return the existing tenant or create a new one.
pub async fn get_or_create(pool: &AnyPool, external_id: &str, now_ms: i64) -> MetaResult<Tenant> {
    if let Some(t) = get_by_external(pool, external_id).await? {
        return Ok(t);
    }
    match create(pool, external_id, now_ms).await {
        Ok(t) => Ok(t),
        // Lost the race with another caller; look it up.
        Err(MetaError::Conflict(_)) => Ok(get_by_external(pool, external_id)
            .await?
            .expect("tenant existed on conflict path")),
        Err(e) => Err(e),
    }
}

/// List all tenants, oldest first. Small helper for admin tooling.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<Tenant>> {
    let rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT internal_id, external_id, created_at_ms FROM tenants ORDER BY internal_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(i, e, t)| Tenant {
            internal_id: i,
            external_id: e,
            created_at_ms: t,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn create_and_lookup_roundtrip() {
        let p = pool().await;
        let t = create(&p, "alpha", 100).await.unwrap();
        assert_eq!(t.external_id, "alpha");
        assert!(t.internal_id > 0);
        let back = get_by_external(&p, "alpha").await.unwrap().unwrap();
        assert_eq!(back, t);
    }

    #[tokio::test]
    async fn duplicate_create_is_conflict() {
        let p = pool().await;
        create(&p, "alpha", 100).await.unwrap();
        let err = create(&p, "alpha", 200).await.unwrap_err();
        assert!(matches!(err, MetaError::Conflict(_)));
    }

    #[tokio::test]
    async fn get_or_create_is_idempotent() {
        let p = pool().await;
        let a = get_or_create(&p, "alpha", 100).await.unwrap();
        let b = get_or_create(&p, "alpha", 200).await.unwrap();
        assert_eq!(a.internal_id, b.internal_id);
        // Returned tenant should carry the original created_at_ms, not
        // the second call's `now`.
        assert_eq!(b.created_at_ms, 100);
    }

    #[tokio::test]
    async fn get_by_internal_round_trips() {
        let p = pool().await;
        let created = create(&p, "alpha", 100).await.unwrap();
        let back = get_by_internal(&p, created.internal_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(back, created);
    }

    #[tokio::test]
    async fn get_by_internal_returns_none_for_unknown_id() {
        let p = pool().await;
        assert!(get_by_internal(&p, 999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_all_is_sorted_by_internal_id() {
        let p = pool().await;
        create(&p, "alpha", 100).await.unwrap();
        create(&p, "bravo", 200).await.unwrap();
        create(&p, "charlie", 300).await.unwrap();
        let all = list_all(&p).await.unwrap();
        let names: Vec<_> = all.iter().map(|t| t.external_id.as_str()).collect();
        assert_eq!(names, ["alpha", "bravo", "charlie"]);
    }
}
