//! Tablets — runtime units that own shard ranges. See design §2.5.
//!
//! Every tablet row carries an `epoch` that increments on every
//! ownership change. Writes to object storage are tagged with their
//! epoch; stale-epoch writes are rejected at read time — the fencing-
//! token pattern committed to in design §5.5.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tablet {
    pub tablet_id: i64,
    pub owner_node_id: Option<String>,
    pub epoch: i64,
    pub lease_expires_at_ms: i64,
    pub created_at_ms: i64,
}

/// Idempotent create: insert the tablet row if absent; do nothing if
/// already present. Returns the row as it now exists on-disk.
pub async fn get_or_create(pool: &AnyPool, tablet_id: i64, now_ms: i64) -> MetaResult<Tablet> {
    // Try insert first; if the PK already exists we fall through to
    // lookup. `INSERT OR IGNORE` is idempotent and doesn't error.
    sqlx::query(
        "INSERT OR IGNORE INTO tablets (tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms)
         VALUES (?, NULL, 0, 0, ?)",
    )
    .bind(tablet_id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    get(pool, tablet_id)
        .await?
        .ok_or_else(|| MetaError::NotFound(format!("tablet {tablet_id} after insert-or-ignore")))
}

pub async fn get(pool: &AnyPool, tablet_id: i64) -> MetaResult<Option<Tablet>> {
    let row: Option<(i64, Option<String>, i64, i64, i64)> = sqlx::query_as(
        "SELECT tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms
         FROM tablets WHERE tablet_id = ?",
    )
    .bind(tablet_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, owner, epoch, lease, created)| Tablet {
        tablet_id: id,
        owner_node_id: owner,
        epoch,
        lease_expires_at_ms: lease,
        created_at_ms: created,
    }))
}

/// Atomically claim or renew ownership. Bumps `epoch` and sets
/// `owner_node_id` + `lease_expires_at_ms`. If the current row's
/// `epoch` doesn't match `expected_epoch`, returns
/// [`MetaError::Conflict`] — the caller lost a race and should
/// re-read before retrying.
///
/// Pass `expected_epoch = current.epoch` for a renewal; pass it the
/// same way for a take-over — the scheduler reads, waits for lease
/// expiry, then calls this with the epoch it read at decision time.
pub async fn assume_ownership(
    pool: &AnyPool,
    tablet_id: i64,
    expected_epoch: i64,
    new_owner: &str,
    new_lease_expires_at_ms: i64,
) -> MetaResult<Tablet> {
    let rows = sqlx::query(
        "UPDATE tablets
           SET owner_node_id       = ?,
               epoch               = epoch + 1,
               lease_expires_at_ms = ?
         WHERE tablet_id = ? AND epoch = ?",
    )
    .bind(new_owner)
    .bind(new_lease_expires_at_ms)
    .bind(tablet_id)
    .bind(expected_epoch)
    .execute(pool)
    .await?
    .rows_affected();
    if rows == 0 {
        return Err(MetaError::Conflict(format!(
            "tablet {tablet_id} epoch mismatch (expected {expected_epoch})"
        )));
    }
    get(pool, tablet_id)
        .await?
        .ok_or_else(|| MetaError::NotFound(format!("tablet {tablet_id}")))
}

/// Sweep every tablet whose lease expired longer than
/// `safety_margin_ms` ago and clear its ownership (sets
/// `owner_node_id = NULL`, `lease_expires_at_ms = 0`, `epoch + 1`).
/// Returns the list of `tablet_id`s that were cleared, so callers
/// can log + increment metrics.
///
/// Called only from the scheduler leader (see
/// `agentdb_core::reaper::StaleTabletReaper`). Idempotent: once a
/// row is cleared, subsequent calls leave it alone because
/// `owner_node_id IS NULL` excludes it from the predicate.
///
/// Safety-margin rationale: the condition is
/// `lease_expires_at_ms + safety_margin_ms < now_ms`. This gives
/// the current owner a grace period beyond nominal expiry before
/// the reaper intervenes — a node whose renewal is momentarily
/// slow, but which is still alive, has time to catch up.
pub async fn clear_expired_ownership(
    pool: &AnyPool,
    safety_margin_ms: i64,
    now_ms: i64,
) -> MetaResult<Vec<i64>> {
    // Two-step: first fetch the ids we'll reap (so we can log /
    // return them), then do the bulk update. Race-free because the
    // predicate on the UPDATE repeats the time check: even if a row
    // got renewed between our SELECT and UPDATE, the UPDATE's WHERE
    // would exclude it and the returned list would just be slightly
    // over-reported. To keep reporting honest, do the work inside a
    // single transaction.
    let mut tx = pool.begin().await?;
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT tablet_id FROM tablets
         WHERE owner_node_id IS NOT NULL
           AND lease_expires_at_ms + ? < ?
         ORDER BY tablet_id",
    )
    .bind(safety_margin_ms)
    .bind(now_ms)
    .fetch_all(&mut *tx)
    .await?;
    let ids: Vec<i64> = rows.into_iter().map(|(id,)| id).collect();
    if !ids.is_empty() {
        sqlx::query(
            "UPDATE tablets
               SET owner_node_id = NULL,
                   lease_expires_at_ms = 0,
                   epoch = epoch + 1
             WHERE owner_node_id IS NOT NULL
               AND lease_expires_at_ms + ? < ?",
        )
        .bind(safety_margin_ms)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(ids)
}

/// Every tablet row, ordered by `tablet_id`. Point-in-time snapshot —
/// ownership can change between this return and the caller acting on
/// it. Used by `GET /api/admin/tablets` and by the scheduler when it
/// needs a cluster-wide view of which tablets exist.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<Tablet>> {
    let rows: Vec<(i64, Option<String>, i64, i64, i64)> = sqlx::query_as(
        "SELECT tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms
           FROM tablets ORDER BY tablet_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, owner, epoch, lease, created)| Tablet {
            tablet_id: id,
            owner_node_id: owner,
            epoch,
            lease_expires_at_ms: lease,
            created_at_ms: created,
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
    async fn create_and_get() {
        let p = pool().await;
        let t = get_or_create(&p, 7, 100).await.unwrap();
        assert_eq!(t.tablet_id, 7);
        assert_eq!(t.epoch, 0);
        assert_eq!(t.owner_node_id, None);
        let back = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(back, t);
    }

    #[tokio::test]
    async fn get_or_create_is_idempotent() {
        let p = pool().await;
        let a = get_or_create(&p, 7, 100).await.unwrap();
        let b = get_or_create(&p, 7, 200).await.unwrap();
        assert_eq!(a, b, "second get_or_create returns the original row");
    }

    #[tokio::test]
    async fn assume_ownership_bumps_epoch_and_sets_owner() {
        let p = pool().await;
        let t0 = get_or_create(&p, 1, 100).await.unwrap();
        assert_eq!(t0.epoch, 0);
        let t1 = assume_ownership(&p, 1, 0, "node-a", 500).await.unwrap();
        assert_eq!(t1.epoch, 1);
        assert_eq!(t1.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(t1.lease_expires_at_ms, 500);
    }

    #[tokio::test]
    async fn list_all_returns_every_tablet_in_id_order() {
        let p = pool().await;
        get_or_create(&p, 2, 100).await.unwrap();
        get_or_create(&p, 1, 100).await.unwrap();
        get_or_create(&p, 3, 100).await.unwrap();
        assume_ownership(&p, 2, 0, "node-a", 500).await.unwrap();
        let all = list_all(&p).await.unwrap();
        let ids: Vec<_> = all.iter().map(|t| t.tablet_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        // tablet 2 has an owner now; the others are still unowned.
        assert_eq!(all[1].owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(all[0].owner_node_id, None);
        assert_eq!(all[2].owner_node_id, None);
    }

    #[tokio::test]
    async fn assume_ownership_fails_on_epoch_mismatch() {
        let p = pool().await;
        get_or_create(&p, 1, 100).await.unwrap();
        // First claim at epoch 0 succeeds → now at epoch 1
        assume_ownership(&p, 1, 0, "node-a", 500).await.unwrap();
        // Second claim still at epoch 0 must fail
        let err = assume_ownership(&p, 1, 0, "node-b", 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict(_)));
    }

    #[tokio::test]
    async fn clear_expired_ownership_returns_empty_on_fresh_db() {
        let p = pool().await;
        let cleared = clear_expired_ownership(&p, 5_000, 1_000_000).await.unwrap();
        assert!(cleared.is_empty());
    }

    #[tokio::test]
    async fn clear_expired_ownership_leaves_live_leases_alone() {
        let p = pool().await;
        get_or_create(&p, 1, 100).await.unwrap();
        // Claim at epoch 0 → now at epoch 1, lease until 10_000.
        assume_ownership(&p, 1, 0, "node-a", 10_000).await.unwrap();
        // Sweep with now=5000 — lease still live.
        let cleared = clear_expired_ownership(&p, 0, 5_000).await.unwrap();
        assert!(cleared.is_empty(), "live lease must not be reaped");
        let row = get(&p, 1).await.unwrap().unwrap();
        assert_eq!(row.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(row.epoch, 1, "epoch unchanged when nothing reaped");
    }

    #[tokio::test]
    async fn clear_expired_ownership_leaves_within_safety_margin_alone() {
        let p = pool().await;
        get_or_create(&p, 1, 100).await.unwrap();
        assume_ownership(&p, 1, 0, "node-a", 10_000).await.unwrap();
        // now=11_000: lease expired by 1000ms, but safety_margin=5000 keeps
        // it protected.
        let cleared = clear_expired_ownership(&p, 5_000, 11_000).await.unwrap();
        assert!(
            cleared.is_empty(),
            "within safety margin, lease must not be reaped"
        );
    }

    #[tokio::test]
    async fn clear_expired_ownership_clears_past_safety_margin() {
        let p = pool().await;
        get_or_create(&p, 1, 100).await.unwrap();
        get_or_create(&p, 2, 100).await.unwrap();
        get_or_create(&p, 3, 100).await.unwrap();
        // 1 and 3 are expired-and-past-safety. 2 is still live.
        assume_ownership(&p, 1, 0, "node-a", 1_000).await.unwrap();
        assume_ownership(&p, 2, 0, "node-b", 50_000).await.unwrap();
        assume_ownership(&p, 3, 0, "node-c", 2_000).await.unwrap();
        // now = 10_000, safety_margin = 1_000. Both 1 and 3 are past
        // (1+1000=2000 < 10000) and (2+1000=3000 < 10000). 2 is live.
        let cleared = clear_expired_ownership(&p, 1_000, 10_000).await.unwrap();
        assert_eq!(cleared, vec![1, 3], "expired rows reaped in id order");
        // Verify the DB state.
        let t1 = get(&p, 1).await.unwrap().unwrap();
        assert_eq!(t1.owner_node_id, None);
        assert_eq!(t1.lease_expires_at_ms, 0);
        assert_eq!(t1.epoch, 2, "epoch bumped by reap");
        let t2 = get(&p, 2).await.unwrap().unwrap();
        assert_eq!(t2.owner_node_id.as_deref(), Some("node-b"));
        assert_eq!(t2.epoch, 1, "live lease untouched");
        let t3 = get(&p, 3).await.unwrap().unwrap();
        assert_eq!(t3.owner_node_id, None);
    }

    #[tokio::test]
    async fn clear_expired_ownership_is_idempotent() {
        let p = pool().await;
        get_or_create(&p, 1, 100).await.unwrap();
        assume_ownership(&p, 1, 0, "node-a", 1_000).await.unwrap();
        let first = clear_expired_ownership(&p, 0, 10_000).await.unwrap();
        assert_eq!(first, vec![1]);
        // Second call: the row's owner is NULL, so the predicate
        // excludes it.
        let second = clear_expired_ownership(&p, 0, 10_000).await.unwrap();
        assert!(second.is_empty());
    }
}
