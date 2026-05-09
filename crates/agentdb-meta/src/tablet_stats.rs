//! Per-node tablet runtime storage statistics.
//!
//! These rows are advisory control-plane signals. They are intentionally
//! best-effort: stale or missing stats may delay lifecycle automation, but
//! split/merge correctness still comes from movement catch-up and cutover
//! checks.

use std::collections::BTreeMap;

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

type TabletStatsRow = (
    i64,
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TabletStats {
    pub tablet_id: i64,
    pub node_id: String,
    pub key_count: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub storage_bytes: i64,
    pub applied_index: i64,
    pub sample_interval_ms: i64,
    pub write_ops_delta: i64,
    pub write_ops_per_sec_milli: i64,
    pub compaction_debt_ops: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TabletStatsAggregate {
    pub tablet_id: i64,
    pub replica_count: i64,
    pub key_count: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub storage_bytes: i64,
    pub applied_index: i64,
    pub sample_interval_ms: i64,
    pub write_ops_delta: i64,
    pub write_ops_per_sec_milli: i64,
    pub compaction_debt_ops: i64,
    pub updated_at_ms: i64,
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    key_count: i64,
    key_bytes: i64,
    value_bytes: i64,
    storage_bytes: i64,
    applied_index: i64,
    now_ms: i64,
) -> MetaResult<()> {
    upsert_with_rate(
        pool,
        tablet_id,
        node_id,
        key_count,
        key_bytes,
        value_bytes,
        storage_bytes,
        applied_index,
        0,
        0,
        0,
        0,
        now_ms,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_with_rate(
    pool: &AnyPool,
    tablet_id: i64,
    node_id: &str,
    key_count: i64,
    key_bytes: i64,
    value_bytes: i64,
    storage_bytes: i64,
    applied_index: i64,
    sample_interval_ms: i64,
    write_ops_delta: i64,
    write_ops_per_sec_milli: i64,
    compaction_debt_ops: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate_non_negative(tablet_id, "tablet_id")?;
    validate_non_empty(node_id, "node_id")?;
    validate_non_negative(key_count, "key_count")?;
    validate_non_negative(key_bytes, "key_bytes")?;
    validate_non_negative(value_bytes, "value_bytes")?;
    validate_non_negative(storage_bytes, "storage_bytes")?;
    validate_non_negative(applied_index, "applied_index")?;
    validate_non_negative(sample_interval_ms, "sample_interval_ms")?;
    validate_non_negative(write_ops_delta, "write_ops_delta")?;
    validate_non_negative(write_ops_per_sec_milli, "write_ops_per_sec_milli")?;
    validate_non_negative(compaction_debt_ops, "compaction_debt_ops")?;
    validate_non_negative(now_ms, "now_ms")?;
    sqlx::query(
        "INSERT INTO tablet_stats
            (tablet_id, node_id, key_count, key_bytes, value_bytes,
             storage_bytes, applied_index, sample_interval_ms, write_ops_delta,
             write_ops_per_sec_milli, compaction_debt_ops, updated_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (tablet_id, node_id) DO UPDATE SET
            key_count = excluded.key_count,
            key_bytes = excluded.key_bytes,
            value_bytes = excluded.value_bytes,
            storage_bytes = excluded.storage_bytes,
            applied_index = excluded.applied_index,
            sample_interval_ms = excluded.sample_interval_ms,
            write_ops_delta = excluded.write_ops_delta,
            write_ops_per_sec_milli = excluded.write_ops_per_sec_milli,
            compaction_debt_ops = excluded.compaction_debt_ops,
            updated_at_ms = excluded.updated_at_ms",
    )
    .bind(tablet_id)
    .bind(node_id)
    .bind(key_count)
    .bind(key_bytes)
    .bind(value_bytes)
    .bind(storage_bytes)
    .bind(applied_index)
    .bind(sample_interval_ms)
    .bind(write_ops_delta)
    .bind(write_ops_per_sec_milli)
    .bind(compaction_debt_ops)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<TabletStats>> {
    let rows: Vec<TabletStatsRow> = sqlx::query_as(
        "SELECT tablet_id, node_id, key_count, key_bytes, value_bytes,
                storage_bytes, applied_index, sample_interval_ms,
                write_ops_delta, write_ops_per_sec_milli, compaction_debt_ops,
                updated_at_ms
           FROM tablet_stats
          ORDER BY tablet_id, node_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_stats).collect())
}

pub async fn list_by_tablet(pool: &AnyPool, tablet_id: i64) -> MetaResult<Vec<TabletStats>> {
    let rows: Vec<TabletStatsRow> = sqlx::query_as(
        "SELECT tablet_id, node_id, key_count, key_bytes, value_bytes,
                storage_bytes, applied_index, sample_interval_ms,
                write_ops_delta, write_ops_per_sec_milli, compaction_debt_ops,
                updated_at_ms
           FROM tablet_stats
          WHERE tablet_id = $1
          ORDER BY storage_bytes DESC, write_ops_per_sec_milli DESC, node_id",
    )
    .bind(tablet_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_stats).collect())
}

pub async fn list_aggregates(pool: &AnyPool) -> MetaResult<Vec<TabletStatsAggregate>> {
    let mut aggregates = BTreeMap::<i64, TabletStatsAggregate>::new();
    for stats in list_all(pool).await? {
        aggregates
            .entry(stats.tablet_id)
            .and_modify(|aggregate| {
                aggregate.replica_count += 1;
                aggregate.key_count = aggregate.key_count.max(stats.key_count);
                aggregate.key_bytes = aggregate.key_bytes.max(stats.key_bytes);
                aggregate.value_bytes = aggregate.value_bytes.max(stats.value_bytes);
                aggregate.storage_bytes = aggregate.storage_bytes.max(stats.storage_bytes);
                aggregate.applied_index = aggregate.applied_index.max(stats.applied_index);
                aggregate.sample_interval_ms =
                    aggregate.sample_interval_ms.max(stats.sample_interval_ms);
                aggregate.write_ops_delta = aggregate.write_ops_delta.max(stats.write_ops_delta);
                aggregate.write_ops_per_sec_milli = aggregate
                    .write_ops_per_sec_milli
                    .max(stats.write_ops_per_sec_milli);
                aggregate.compaction_debt_ops =
                    aggregate.compaction_debt_ops.max(stats.compaction_debt_ops);
                aggregate.updated_at_ms = aggregate.updated_at_ms.max(stats.updated_at_ms);
            })
            .or_insert(TabletStatsAggregate {
                tablet_id: stats.tablet_id,
                replica_count: 1,
                key_count: stats.key_count,
                key_bytes: stats.key_bytes,
                value_bytes: stats.value_bytes,
                storage_bytes: stats.storage_bytes,
                applied_index: stats.applied_index,
                sample_interval_ms: stats.sample_interval_ms,
                write_ops_delta: stats.write_ops_delta,
                write_ops_per_sec_milli: stats.write_ops_per_sec_milli,
                compaction_debt_ops: stats.compaction_debt_ops,
                updated_at_ms: stats.updated_at_ms,
            });
    }
    Ok(aggregates.into_values().collect())
}

pub async fn delete_stale(pool: &AnyPool, older_than_ms: i64) -> MetaResult<u64> {
    validate_non_negative(older_than_ms, "older_than_ms")?;
    let rows = sqlx::query("DELETE FROM tablet_stats WHERE updated_at_ms < $1")
        .bind(older_than_ms)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows)
}

fn validate_non_empty(value: &str, name: &str) -> MetaResult<()> {
    if value.trim().is_empty() {
        return Err(MetaError::InvalidInput(format!("{name} must be non-empty")));
    }
    Ok(())
}

fn validate_non_negative(value: i64, name: &str) -> MetaResult<()> {
    if value < 0 {
        return Err(MetaError::InvalidInput(format!(
            "{name} must be non-negative"
        )));
    }
    Ok(())
}

fn into_stats(
    (
        tablet_id,
        node_id,
        key_count,
        key_bytes,
        value_bytes,
        storage_bytes,
        applied_index,
        sample_interval_ms,
        write_ops_delta,
        write_ops_per_sec_milli,
        compaction_debt_ops,
        updated_at_ms,
    ): TabletStatsRow,
) -> TabletStats {
    TabletStats {
        tablet_id,
        node_id,
        key_count,
        key_bytes,
        value_bytes,
        storage_bytes,
        applied_index,
        sample_interval_ms,
        write_ops_delta,
        write_ops_per_sec_milli,
        compaction_debt_ops,
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
    async fn upsert_lists_and_aggregates_tablet_stats() {
        let p = pool().await;
        upsert(&p, 7, "node-a", 10, 100, 1_000, 1_100, 12, 100)
            .await
            .unwrap();
        upsert(&p, 7, "node-b", 8, 80, 900, 980, 11, 110)
            .await
            .unwrap();
        upsert(&p, 8, "node-a", 1, 5, 20, 25, 2, 120).await.unwrap();

        let rows = list_by_tablet(&p, 7).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].node_id, "node-a");
        assert_eq!(rows[0].storage_bytes, 1_100);

        let aggregates = list_aggregates(&p).await.unwrap();
        let aggregate = aggregates
            .iter()
            .find(|aggregate| aggregate.tablet_id == 7)
            .unwrap();
        assert_eq!(aggregate.replica_count, 2);
        assert_eq!(aggregate.storage_bytes, 1_100);
        assert_eq!(aggregate.applied_index, 12);
        assert_eq!(aggregate.write_ops_per_sec_milli, 0);
        assert_eq!(aggregate.compaction_debt_ops, 0);
    }

    #[tokio::test]
    async fn upsert_with_rate_lists_and_aggregates_write_rate() {
        let p = pool().await;
        upsert_with_rate(
            &p, 7, "node-a", 10, 100, 1_000, 1_100, 24, 1_000, 12, 12_000, 500, 200,
        )
        .await
        .unwrap();
        upsert_with_rate(
            &p, 7, "node-b", 8, 80, 900, 980, 20, 1_000, 8, 8_000, 250, 210,
        )
        .await
        .unwrap();

        let rows = list_by_tablet(&p, 7).await.unwrap();
        assert_eq!(rows[0].sample_interval_ms, 1_000);
        assert_eq!(rows[0].write_ops_delta, 12);
        assert_eq!(rows[0].write_ops_per_sec_milli, 12_000);

        let aggregate = list_aggregates(&p)
            .await
            .unwrap()
            .into_iter()
            .find(|aggregate| aggregate.tablet_id == 7)
            .unwrap();
        assert_eq!(aggregate.write_ops_delta, 12);
        assert_eq!(aggregate.write_ops_per_sec_milli, 12_000);
        assert_eq!(aggregate.compaction_debt_ops, 500);
    }

    #[tokio::test]
    async fn delete_stale_removes_old_rows() {
        let p = pool().await;
        upsert(&p, 7, "node-a", 10, 100, 1_000, 1_100, 12, 100)
            .await
            .unwrap();
        upsert(&p, 7, "node-b", 10, 100, 1_000, 1_100, 12, 200)
            .await
            .unwrap();

        assert_eq!(delete_stale(&p, 150).await.unwrap(), 1);
        assert_eq!(list_all(&p).await.unwrap().len(), 1);
    }
}
