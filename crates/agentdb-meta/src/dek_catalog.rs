//! Cluster-shared wrapped-DEK key catalog (encryption-at-rest).
//!
//! For a multi-node cluster, every node must serve the *same* per-tenant data
//! key, so the wrapped DEKs live in the shared control metadata rather than a
//! node-local store. This module is the durable table: one row per
//! `(tenant_id, purpose, version)` holding a DEK wrapped under the KEK (never
//! plaintext key material).
//!
//! Provisioning is race-safe *across nodes*: [`provision_if_absent`] uses
//! `INSERT ... ON CONFLICT DO NOTHING` and returns whichever row won, so two
//! nodes that concurrently mint a DEK for the same tenant converge on one — the
//! loser's key is simply never used. Serving nodes pre-load rows via
//! [`list_all`] and read a specific one with [`get_active`] / [`get_version`].

use sqlx::AnyPool;

use crate::error::MetaResult;

/// One wrapped-DEK record. Byte fields hold ciphertext, never plaintext keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DekCatalogRow {
    pub tenant_id: String,
    pub purpose: String,
    pub version: i64,
    pub wrapping_key_ref: String,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub status: String,
    pub created_at_ms: i64,
}

type DekRowTuple = (String, String, i64, String, Vec<u8>, Vec<u8>, String, i64);

fn row_from_tuple(t: DekRowTuple) -> DekCatalogRow {
    DekCatalogRow {
        tenant_id: t.0,
        purpose: t.1,
        version: t.2,
        wrapping_key_ref: t.3,
        nonce: t.4,
        ciphertext: t.5,
        status: t.6,
        created_at_ms: t.7,
    }
}

const SELECT_COLS: &str =
    "tenant_id, purpose, version, wrapping_key_ref, nonce, ciphertext, status, created_at_ms";

/// Insert a DEK record if `(tenant_id, purpose, version)` is not already
/// present, then return the row that is now stored — the caller's, or the one a
/// racing node inserted first. This is the coordinated provisioning primitive:
/// the winner's wrapped DEK is authoritative and every node reads it back.
pub async fn provision_if_absent(
    pool: &AnyPool,
    row: &DekCatalogRow,
) -> MetaResult<DekCatalogRow> {
    sqlx::query(
        "INSERT INTO dek_catalog \
         (tenant_id, purpose, version, wrapping_key_ref, nonce, ciphertext, status, created_at_ms) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (tenant_id, purpose, version) DO NOTHING",
    )
    .bind(&row.tenant_id)
    .bind(&row.purpose)
    .bind(row.version)
    .bind(&row.wrapping_key_ref)
    .bind(&row.nonce)
    .bind(&row.ciphertext)
    .bind(&row.status)
    .bind(row.created_at_ms)
    .execute(pool)
    .await?;
    get_version(pool, &row.tenant_id, &row.purpose, row.version)
        .await?
        .ok_or_else(|| {
            crate::error::MetaError::Invariant(
                "dek_catalog row vanished after provision".into(),
            )
        })
}

/// The active DEK for a `(tenant, purpose)`, if provisioned. The active row is
/// the highest-version row whose status is `active`.
pub async fn get_active(
    pool: &AnyPool,
    tenant_id: &str,
    purpose: &str,
) -> MetaResult<Option<DekCatalogRow>> {
    let row: Option<DekRowTuple> = sqlx::query_as(&format!(
        "SELECT {SELECT_COLS} FROM dek_catalog \
         WHERE tenant_id = $1 AND purpose = $2 AND status = 'active' \
         ORDER BY version DESC LIMIT 1"
    ))
    .bind(tenant_id)
    .bind(purpose)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_from_tuple))
}

/// A specific DEK version (for decrypting older ciphertext during/after
/// rotation), regardless of status — the caller enforces the status policy.
pub async fn get_version(
    pool: &AnyPool,
    tenant_id: &str,
    purpose: &str,
    version: i64,
) -> MetaResult<Option<DekCatalogRow>> {
    let row: Option<DekRowTuple> = sqlx::query_as(&format!(
        "SELECT {SELECT_COLS} FROM dek_catalog \
         WHERE tenant_id = $1 AND purpose = $2 AND version = $3"
    ))
    .bind(tenant_id)
    .bind(purpose)
    .bind(version)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_from_tuple))
}

/// Every DEK record, for a serving node to pre-load its in-memory key cache at
/// startup.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<DekCatalogRow>> {
    let rows: Vec<DekRowTuple> = sqlx::query_as(&format!(
        "SELECT {SELECT_COLS} FROM dek_catalog ORDER BY tenant_id, purpose, version"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_from_tuple).collect())
}

/// Set a DEK version's lifecycle status (`active|read_only|retired|revoked`),
/// e.g. for rotation or crypto-shred. Returns whether a row was updated.
pub async fn set_status(
    pool: &AnyPool,
    tenant_id: &str,
    purpose: &str,
    version: i64,
    status: &str,
) -> MetaResult<bool> {
    let result = sqlx::query(
        "UPDATE dek_catalog SET status = $1 \
         WHERE tenant_id = $2 AND purpose = $3 AND version = $4",
    )
    .bind(status)
    .bind(tenant_id)
    .bind(purpose)
    .bind(version)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{migrations, MetaDialect};
    use sqlx::any::AnyPoolOptions;

    async fn pool() -> AnyPool {
        crate::install_any_drivers_for_tests();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        migrations::apply(&pool, MetaDialect::Sqlite).await.unwrap();
        pool
    }

    fn row(tenant: &str, version: i64, ct: &[u8]) -> DekCatalogRow {
        DekCatalogRow {
            tenant_id: tenant.into(),
            purpose: "content".into(),
            version,
            wrapping_key_ref: "local:v1".into(),
            nonce: vec![1, 2, 3],
            ciphertext: ct.to_vec(),
            status: "active".into(),
            created_at_ms: 1_000,
        }
    }

    #[tokio::test]
    async fn provision_is_race_safe_and_idempotent() {
        let pool = pool().await;
        // First provision wins.
        let a = provision_if_absent(&pool, &row("ten_a", 1, b"wrapped-A"))
            .await
            .unwrap();
        assert_eq!(a.ciphertext, b"wrapped-A");
        // A racing provision with DIFFERENT bytes loses; the winner is returned.
        let b = provision_if_absent(&pool, &row("ten_a", 1, b"wrapped-B"))
            .await
            .unwrap();
        assert_eq!(b.ciphertext, b"wrapped-A", "first writer's DEK is authoritative");
    }

    #[tokio::test]
    async fn get_active_and_version_and_list() {
        let pool = pool().await;
        provision_if_absent(&pool, &row("ten_a", 1, b"a1")).await.unwrap();
        provision_if_absent(&pool, &row("ten_b", 1, b"b1")).await.unwrap();

        assert_eq!(
            get_active(&pool, "ten_a", "content").await.unwrap().unwrap().ciphertext,
            b"a1"
        );
        assert!(get_active(&pool, "ten_c", "content").await.unwrap().is_none());
        assert_eq!(
            get_version(&pool, "ten_b", "content", 1).await.unwrap().unwrap().ciphertext,
            b"b1"
        );
        assert_eq!(list_all(&pool).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn set_status_retires_a_version() {
        let pool = pool().await;
        provision_if_absent(&pool, &row("ten_a", 1, b"a1")).await.unwrap();
        assert!(set_status(&pool, "ten_a", "content", 1, "revoked").await.unwrap());
        // No longer the active row.
        assert!(get_active(&pool, "ten_a", "content").await.unwrap().is_none());
        // But still fetchable by version (status enforced by the caller).
        assert_eq!(
            get_version(&pool, "ten_a", "content", 1).await.unwrap().unwrap().status,
            "revoked"
        );
    }
}
