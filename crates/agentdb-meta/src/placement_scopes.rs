//! Placement scopes group virtual shards before they map to tablets.
//!
//! A dedicated paid tenant is one placement scope. A shared free-tier pool is
//! also one placement scope. Shards are only meaningful inside a scope.

use sqlx::AnyPool;

use crate::error::MetaResult;

pub const DEFAULT_PLACEMENT_SCOPE_ID: &str = "default";

type PlacementScopeRow = (String, String, i64, Option<String>, String, i64, i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementScope {
    pub placement_scope_id: String,
    pub kind: String,
    pub shard_count: i64,
    pub hash_seed: Option<String>,
    pub state: String,
    pub epoch: i64,
    pub created_at_ms: i64,
}

pub async fn get(pool: &AnyPool, placement_scope_id: &str) -> MetaResult<Option<PlacementScope>> {
    let row: Option<PlacementScopeRow> = sqlx::query_as(
        "SELECT placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms
           FROM placement_scopes WHERE placement_scope_id = $1",
    )
    .bind(placement_scope_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_scope))
}

pub async fn upsert(
    pool: &AnyPool,
    placement_scope_id: &str,
    kind: &str,
    shard_count: i64,
    hash_seed: Option<&str>,
    state: &str,
    now_ms: i64,
) -> MetaResult<()> {
    let existing = get(pool, placement_scope_id).await?;
    if let Some(scope) = existing {
        sqlx::query(
            "UPDATE placement_scopes
                SET kind = $1, shard_count = $2, hash_seed = $3, state = $4, epoch = $5, created_at_ms = $6
              WHERE placement_scope_id = $7",
        )
        .bind(kind)
        .bind(shard_count)
        .bind(hash_seed)
        .bind(state)
        .bind(scope.epoch + 1)
        .bind(now_ms)
        .bind(placement_scope_id)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(
            "INSERT INTO placement_scopes
                (placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms)
             VALUES ($1, $2, $3, $4, $5, 0, $6)",
        )
        .bind(placement_scope_id)
        .bind(kind)
        .bind(shard_count)
        .bind(hash_seed)
        .bind(state)
        .bind(now_ms)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<PlacementScope>> {
    let rows: Vec<PlacementScopeRow> = sqlx::query_as(
        "SELECT placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms
           FROM placement_scopes ORDER BY placement_scope_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_scope).collect())
}

fn into_scope(
    (placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms): PlacementScopeRow,
) -> PlacementScope {
    PlacementScope {
        placement_scope_id,
        kind,
        shard_count,
        hash_seed,
        state,
        epoch,
        created_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn migration_seeds_default_scope() {
        let p = pool().await;
        let scope = get(&p, DEFAULT_PLACEMENT_SCOPE_ID).await.unwrap().unwrap();
        assert_eq!(scope.kind, "shared_pool");
        assert_eq!(scope.shard_count, 16_384);
        assert_eq!(scope.state, "serving");
    }

    #[tokio::test]
    async fn upsert_creates_and_bumps_epoch() {
        let p = pool().await;
        upsert(
            &p,
            "tenant/t1",
            "dedicated_tenant",
            1024,
            None,
            "serving",
            10,
        )
        .await
        .unwrap();
        let first = get(&p, "tenant/t1").await.unwrap().unwrap();
        assert_eq!(first.epoch, 0);

        upsert(
            &p,
            "tenant/t1",
            "dedicated_tenant",
            1024,
            Some("seed"),
            "serving",
            20,
        )
        .await
        .unwrap();
        let second = get(&p, "tenant/t1").await.unwrap().unwrap();
        assert_eq!(second.epoch, 1);
        assert_eq!(second.hash_seed.as_deref(), Some("seed"));
    }
}
