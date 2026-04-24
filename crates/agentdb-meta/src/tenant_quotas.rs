//! Global per-tenant rate limits. See migration 006 for the
//! schema and the trade-offs vs. per-box-per-tenant quotas.
//!
//! The in-memory enforcement registry lives in `agentdb-core`;
//! this module is the persistent source of truth. Server
//! startup loads every row into core's `TenantQuotaRegistry`.

use sqlx::AnyPool;

use crate::error::MetaResult;

/// Persisted record. `max_qps` and `burst` are both required
/// (no nullable "unlimited" sentinel — the schema CHECK
/// enforces > 0).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantQuota {
    pub tenant_id: String,
    pub max_qps: u64,
    pub burst: u64,
    pub updated_at_ms: i64,
}

/// Upsert a quota. Replaces the existing row for `tenant_id`.
pub async fn upsert(
    pool: &AnyPool,
    tenant_id: &str,
    max_qps: u64,
    burst: u64,
    now_ms: i64,
) -> MetaResult<()> {
    sqlx::query(
        "INSERT INTO tenant_quotas (tenant_id, max_qps, burst, updated_at_ms)
             VALUES (?, ?, ?, ?)
         ON CONFLICT(tenant_id) DO UPDATE SET
             max_qps = excluded.max_qps,
             burst = excluded.burst,
             updated_at_ms = excluded.updated_at_ms",
    )
    .bind(tenant_id)
    .bind(max_qps as i64)
    .bind(burst as i64)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Return the quota for `tenant_id`, or `None` if absent.
pub async fn get(pool: &AnyPool, tenant_id: &str) -> MetaResult<Option<TenantQuota>> {
    let row: Option<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT tenant_id, max_qps, burst, updated_at_ms FROM tenant_quotas WHERE tenant_id = ?",
    )
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(tid, qps, burst, updated)| TenantQuota {
        tenant_id: tid,
        max_qps: qps as u64,
        burst: burst as u64,
        updated_at_ms: updated,
    }))
}

/// Delete the quota. Returns `true` if a row existed.
pub async fn delete(pool: &AnyPool, tenant_id: &str) -> MetaResult<bool> {
    let n = sqlx::query("DELETE FROM tenant_quotas WHERE tenant_id = ?")
        .bind(tenant_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

/// Every quota currently configured. Used on startup to
/// populate the in-memory registry.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<TenantQuota>> {
    let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT tenant_id, max_qps, burst, updated_at_ms
           FROM tenant_quotas
          ORDER BY tenant_id ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(tid, qps, burst, updated)| TenantQuota {
            tenant_id: tid,
            max_qps: qps as u64,
            burst: burst as u64,
            updated_at_ms: updated,
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
    async fn upsert_roundtrip() {
        let p = pool().await;
        upsert(&p, "acme", 10, 20, 1000).await.unwrap();
        let q = get(&p, "acme").await.unwrap().unwrap();
        assert_eq!(q.tenant_id, "acme");
        assert_eq!(q.max_qps, 10);
        assert_eq!(q.burst, 20);
        assert_eq!(q.updated_at_ms, 1000);
    }

    #[tokio::test]
    async fn upsert_replaces_existing_row() {
        let p = pool().await;
        upsert(&p, "acme", 10, 20, 1000).await.unwrap();
        upsert(&p, "acme", 50, 100, 2000).await.unwrap();
        let q = get(&p, "acme").await.unwrap().unwrap();
        assert_eq!(q.max_qps, 50);
        assert_eq!(q.burst, 100);
        assert_eq!(q.updated_at_ms, 2000);
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_tenant() {
        let p = pool().await;
        assert_eq!(get(&p, "unknown").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let p = pool().await;
        upsert(&p, "acme", 10, 20, 1000).await.unwrap();
        assert!(delete(&p, "acme").await.unwrap());
        assert!(!delete(&p, "acme").await.unwrap());
        assert!(!delete(&p, "never-existed").await.unwrap());
    }

    #[tokio::test]
    async fn list_all_is_sorted_by_tenant_id() {
        let p = pool().await;
        upsert(&p, "zeta", 1, 1, 100).await.unwrap();
        upsert(&p, "alpha", 2, 2, 200).await.unwrap();
        upsert(&p, "mid", 3, 3, 300).await.unwrap();
        let all = list_all(&p).await.unwrap();
        let ids: Vec<&str> = all.iter().map(|q| q.tenant_id.as_str()).collect();
        assert_eq!(ids, vec!["alpha", "mid", "zeta"]);
    }

    #[tokio::test]
    async fn zero_values_are_rejected_by_check_constraint() {
        let p = pool().await;
        let err = upsert(&p, "acme", 0, 10, 0).await;
        assert!(err.is_err(), "max_qps=0 must violate CHECK");
        let err = upsert(&p, "acme", 10, 0, 0).await;
        assert!(err.is_err(), "burst=0 must violate CHECK");
    }
}
