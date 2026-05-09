//! Tenant placement, lifecycle, and encryption policy metadata.
//!
//! This is intentionally separate from agentdb-core's runtime quota/retention
//! policy. The data plane needs this metadata to choose the right placement
//! scope and to fence stale encryption/key-policy versions.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};
use crate::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;

type TenantPolicyRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    i64,
    i64,
    i64,
);

pub const DEFAULT_PLACEMENT_CLASS: &str = "shared";
pub const DEFAULT_ENCRYPTION_CLASS: &str = "pool";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TenantPolicy {
    pub tenant_id: String,
    pub placement_class: String,
    pub placement_scope_id: String,
    pub encryption_class: String,
    pub key_policy_id: Option<String>,
    pub encryption_policy_version: i64,
    pub policy_version: i64,
    pub updated_at_ms: i64,
}

impl TenantPolicy {
    pub fn default_for_tenant(tenant_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            placement_class: DEFAULT_PLACEMENT_CLASS.into(),
            placement_scope_id: DEFAULT_PLACEMENT_SCOPE_ID.into(),
            encryption_class: DEFAULT_ENCRYPTION_CLASS.into(),
            key_policy_id: None,
            encryption_policy_version: 0,
            policy_version: 0,
            updated_at_ms: 0,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &AnyPool,
    tenant_id: &str,
    placement_class: &str,
    placement_scope_id: &str,
    encryption_class: &str,
    key_policy_id: Option<&str>,
    encryption_policy_version: i64,
    policy_version: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate_policy(
        placement_class,
        placement_scope_id,
        encryption_class,
        encryption_policy_version,
        policy_version,
    )?;
    sqlx::query(
        "INSERT INTO tenant_policies
            (tenant_id, placement_class, placement_scope_id, encryption_class, key_policy_id,
             encryption_policy_version, policy_version, updated_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (tenant_id) DO UPDATE SET
            placement_class = excluded.placement_class,
            placement_scope_id = excluded.placement_scope_id,
            encryption_class = excluded.encryption_class,
            key_policy_id = excluded.key_policy_id,
            encryption_policy_version = excluded.encryption_policy_version,
            policy_version = excluded.policy_version,
            updated_at_ms = excluded.updated_at_ms",
    )
    .bind(tenant_id)
    .bind(placement_class)
    .bind(placement_scope_id)
    .bind(encryption_class)
    .bind(key_policy_id)
    .bind(encryption_policy_version)
    .bind(policy_version)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get(pool: &AnyPool, tenant_id: &str) -> MetaResult<Option<TenantPolicy>> {
    let row: Option<TenantPolicyRow> = sqlx::query_as(
        "SELECT tenant_id, placement_class, placement_scope_id, encryption_class, key_policy_id,
                encryption_policy_version, policy_version, updated_at_ms
           FROM tenant_policies
          WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_policy))
}

pub async fn effective(pool: &AnyPool, tenant_id: &str) -> MetaResult<TenantPolicy> {
    Ok(get(pool, tenant_id)
        .await?
        .unwrap_or_else(|| TenantPolicy::default_for_tenant(tenant_id)))
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<TenantPolicy>> {
    let rows: Vec<TenantPolicyRow> = sqlx::query_as(
        "SELECT tenant_id, placement_class, placement_scope_id, encryption_class, key_policy_id,
                encryption_policy_version, policy_version, updated_at_ms
           FROM tenant_policies
          ORDER BY tenant_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_policy).collect())
}

pub async fn list_by_placement_scope(
    pool: &AnyPool,
    placement_scope_id: &str,
) -> MetaResult<Vec<TenantPolicy>> {
    let rows: Vec<TenantPolicyRow> = sqlx::query_as(
        "SELECT tenant_id, placement_class, placement_scope_id, encryption_class, key_policy_id,
                encryption_policy_version, policy_version, updated_at_ms
           FROM tenant_policies
          WHERE placement_scope_id = $1
          ORDER BY tenant_id",
    )
    .bind(placement_scope_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_policy).collect())
}

pub async fn delete(pool: &AnyPool, tenant_id: &str) -> MetaResult<bool> {
    let rows = sqlx::query("DELETE FROM tenant_policies WHERE tenant_id = $1")
        .bind(tenant_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

fn validate_policy(
    placement_class: &str,
    placement_scope_id: &str,
    encryption_class: &str,
    encryption_policy_version: i64,
    policy_version: i64,
) -> MetaResult<()> {
    match placement_class {
        "shared" | "dedicated" => {}
        other => {
            return Err(MetaError::InvalidInput(format!(
                "invalid placement class `{other}`"
            )))
        }
    }
    if placement_scope_id.is_empty() {
        return Err(MetaError::InvalidInput(
            "placement_scope_id must not be empty".into(),
        ));
    }
    match encryption_class {
        "pool" | "tenant" => {}
        other => {
            return Err(MetaError::InvalidInput(format!(
                "invalid encryption class `{other}`"
            )))
        }
    }
    if placement_class == "dedicated" && encryption_class != "tenant" {
        return Err(MetaError::InvalidInput(
            "dedicated placement requires tenant encryption class".into(),
        ));
    }
    if encryption_policy_version < 0 {
        return Err(MetaError::InvalidInput(
            "encryption_policy_version must be non-negative".into(),
        ));
    }
    if policy_version < 0 {
        return Err(MetaError::InvalidInput(
            "policy_version must be non-negative".into(),
        ));
    }
    Ok(())
}

fn into_policy(
    (
        tenant_id,
        placement_class,
        placement_scope_id,
        encryption_class,
        key_policy_id,
        encryption_policy_version,
        policy_version,
        updated_at_ms,
    ): TenantPolicyRow,
) -> TenantPolicy {
    TenantPolicy {
        tenant_id,
        placement_class,
        placement_scope_id,
        encryption_class,
        key_policy_id,
        encryption_policy_version,
        policy_version,
        updated_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn effective_returns_default_for_missing_tenant() {
        let p = pool().await;
        let policy = effective(&p, "free-tenant").await.unwrap();
        assert_eq!(policy.tenant_id, "free-tenant");
        assert_eq!(policy.placement_class, "shared");
        assert_eq!(policy.placement_scope_id, DEFAULT_PLACEMENT_SCOPE_ID);
        assert_eq!(policy.encryption_class, "pool");
    }

    #[tokio::test]
    async fn upsert_and_get_roundtrip() {
        let p = pool().await;
        upsert(
            &p,
            "acme",
            "dedicated",
            "tenant/acme",
            "tenant",
            Some("key/acme"),
            3,
            7,
            100,
        )
        .await
        .unwrap();

        let policy = get(&p, "acme").await.unwrap().unwrap();
        assert_eq!(policy.tenant_id, "acme");
        assert_eq!(policy.placement_class, "dedicated");
        assert_eq!(policy.placement_scope_id, "tenant/acme");
        assert_eq!(policy.encryption_class, "tenant");
        assert_eq!(policy.key_policy_id.as_deref(), Some("key/acme"));
        assert_eq!(policy.encryption_policy_version, 3);
        assert_eq!(policy.policy_version, 7);
    }

    #[tokio::test]
    async fn upsert_replaces_existing_policy() {
        let p = pool().await;
        upsert(&p, "acme", "shared", "default", "pool", None, 0, 1, 100)
            .await
            .unwrap();
        upsert(
            &p,
            "acme",
            "dedicated",
            "tenant/acme",
            "tenant",
            Some("key/acme"),
            2,
            3,
            200,
        )
        .await
        .unwrap();

        let policy = effective(&p, "acme").await.unwrap();
        assert_eq!(policy.placement_class, "dedicated");
        assert_eq!(policy.placement_scope_id, "tenant/acme");
        assert_eq!(policy.policy_version, 3);
        assert_eq!(policy.updated_at_ms, 200);
    }

    #[tokio::test]
    async fn list_by_placement_scope_filters_policies() {
        let p = pool().await;
        upsert(&p, "free-a", "shared", "default", "pool", None, 0, 1, 100)
            .await
            .unwrap();
        upsert(&p, "free-b", "shared", "default", "pool", None, 0, 1, 100)
            .await
            .unwrap();
        upsert(
            &p,
            "acme",
            "dedicated",
            "tenant/acme",
            "tenant",
            None,
            0,
            1,
            100,
        )
        .await
        .unwrap();

        let tenants = list_by_placement_scope(&p, "default").await.unwrap();
        let ids: Vec<_> = tenants
            .iter()
            .map(|policy| policy.tenant_id.as_str())
            .collect();
        assert_eq!(ids, vec!["free-a", "free-b"]);
    }

    #[tokio::test]
    async fn validation_rejects_invalid_policy() {
        let p = pool().await;
        let err = upsert(
            &p,
            "acme",
            "dedicated",
            "tenant/acme",
            "pool",
            None,
            0,
            1,
            100,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));

        let err = upsert(&p, "acme", "unknown", "default", "pool", None, 0, 1, 100)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let p = pool().await;
        upsert(&p, "acme", "shared", "default", "pool", None, 0, 1, 100)
            .await
            .unwrap();
        assert!(delete(&p, "acme").await.unwrap());
        assert!(!delete(&p, "acme").await.unwrap());
    }
}
