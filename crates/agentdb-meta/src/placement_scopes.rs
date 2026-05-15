//! Placement scopes group virtual shards before they map to tablets.
//!
//! A dedicated paid tenant is one placement scope. A shared free-tier pool is
//! also one placement scope. Shards are only meaningful inside a scope.

use sqlx::AnyPool;

use crate::cells::{self, DEFAULT_CELL_ID};
use crate::error::MetaResult;

pub const DEFAULT_PLACEMENT_SCOPE_ID: &str = "default";

type PlacementScopeRow = (
    String,
    String,
    i64,
    Option<String>,
    String,
    i64,
    i64,
    String,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementScope {
    pub placement_scope_id: String,
    pub kind: String,
    pub shard_count: i64,
    pub hash_seed: Option<String>,
    pub state: String,
    pub epoch: i64,
    pub created_at_ms: i64,
    pub cell_id: String,
}

pub async fn get(pool: &AnyPool, placement_scope_id: &str) -> MetaResult<Option<PlacementScope>> {
    let row: Option<PlacementScopeRow> = sqlx::query_as(
        "SELECT placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms, cell_id
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
    upsert_with_cell(
        pool,
        placement_scope_id,
        kind,
        shard_count,
        hash_seed,
        state,
        DEFAULT_CELL_ID,
        now_ms,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_with_cell(
    pool: &AnyPool,
    placement_scope_id: &str,
    kind: &str,
    shard_count: i64,
    hash_seed: Option<&str>,
    state: &str,
    cell_id: &str,
    now_ms: i64,
) -> MetaResult<()> {
    cells::validate_cell_id(cell_id)?;
    let existing = get(pool, placement_scope_id).await?;
    if let Some(scope) = existing {
        sqlx::query(
            "UPDATE placement_scopes
                SET kind = $1, shard_count = $2, hash_seed = $3, state = $4, epoch = $5, created_at_ms = $6, cell_id = $7
              WHERE placement_scope_id = $8",
        )
        .bind(kind)
        .bind(shard_count)
        .bind(hash_seed)
        .bind(state)
        .bind(scope.epoch + 1)
        .bind(now_ms)
        .bind(cell_id)
        .bind(placement_scope_id)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(
            "INSERT INTO placement_scopes
                (placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms, cell_id)
             VALUES ($1, $2, $3, $4, $5, 0, $6, $7)",
        )
        .bind(placement_scope_id)
        .bind(kind)
        .bind(shard_count)
        .bind(hash_seed)
        .bind(state)
        .bind(now_ms)
        .bind(cell_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<PlacementScope>> {
    let rows: Vec<PlacementScopeRow> = sqlx::query_as(
        "SELECT placement_scope_id, kind, shard_count, hash_seed, state, epoch, created_at_ms, cell_id
           FROM placement_scopes ORDER BY placement_scope_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_scope).collect())
}

fn into_scope(
    (
        placement_scope_id,
        kind,
        shard_count,
        hash_seed,
        state,
        epoch,
        created_at_ms,
        cell_id,
    ): PlacementScopeRow,
) -> PlacementScope {
    PlacementScope {
        placement_scope_id,
        kind,
        shard_count,
        hash_seed,
        state,
        epoch,
        created_at_ms,
        cell_id,
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
        assert_eq!(scope.cell_id, DEFAULT_CELL_ID);
    }

    #[tokio::test]
    async fn upsert_creates_and_bumps_epoch() {
        let p = pool().await;
        upsert_with_cell(
            &p,
            "tenant/t1",
            "dedicated_tenant",
            1024,
            None,
            "serving",
            "cell-a",
            10,
        )
        .await
        .unwrap();
        let first = get(&p, "tenant/t1").await.unwrap().unwrap();
        assert_eq!(first.epoch, 0);
        assert_eq!(first.cell_id, "cell-a");

        upsert_with_cell(
            &p,
            "tenant/t1",
            "dedicated_tenant",
            1024,
            Some("seed"),
            "serving",
            "cell-b",
            20,
        )
        .await
        .unwrap();
        let second = get(&p, "tenant/t1").await.unwrap().unwrap();
        assert_eq!(second.epoch, 1);
        assert_eq!(second.hash_seed.as_deref(), Some("seed"));
        assert_eq!(second.cell_id, "cell-b");
    }
}
