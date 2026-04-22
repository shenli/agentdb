//! Scheduler leader election via a singleton row in the control
//! plane. Mirrors `tablets::assume_ownership` — a CAS on the current
//! row's `epoch` is the fencing guarantee. The SQL migration
//! (002_scheduler_leader.sql) uses `CHECK (id = 1)` so the database
//! refuses to hold more than one row.
//!
//! The *consumers* of leadership (the scheduler loop that reassigns
//! stale-node tablets, orchestrates drain, etc.) live in the core
//! crate's `scheduler` module. This meta-crate module only manages
//! the row.

use sqlx::sqlite::SqlitePool;

use crate::error::{MetaError, MetaResult};

/// Raw view of the `scheduler_leader` row. `None` from `get` means
/// no leader has ever been elected on this cluster. Callers interpret
/// `expires_at_ms` against their own wall clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderRow {
    pub node_id: String,
    pub epoch: i64,
    pub expires_at_ms: i64,
    pub acquired_at_ms: i64,
}

impl LeaderRow {
    /// Convenience: leader is "live" when the lease has not yet
    /// expired by the supplied wall clock.
    pub fn is_live(&self, now_ms: i64) -> bool {
        now_ms < self.expires_at_ms
    }
}

/// Try to acquire or take over the leader role. Returns `Ok(Some)`
/// on success (this process is now the leader), `Ok(None)` when a
/// live leader already holds it, and `Err` on a SQL error.
///
/// Semantics:
/// - No row yet (cluster has never had a leader): insert. `epoch = 1`.
/// - Row exists and live: refuse. Return `Ok(None)`.
/// - Row exists but lease expired: CAS-bump epoch and take over.
/// - Row exists and already held by `node_id`: refresh (renewal path
///   also calls this on boot to avoid a separate "am I already
///   leader" query).
pub async fn try_acquire(
    pool: &SqlitePool,
    node_id: &str,
    lease_duration_ms: i64,
    now_ms: i64,
) -> MetaResult<Option<LeaderRow>> {
    let new_expiry = now_ms + lease_duration_ms;
    let existing: Option<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT node_id, epoch, expires_at_ms, acquired_at_ms
           FROM scheduler_leader WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    match existing {
        None => {
            // Fresh cluster — insert the singleton row.
            sqlx::query(
                "INSERT INTO scheduler_leader
                   (id, node_id, epoch, expires_at_ms, acquired_at_ms)
                 VALUES (1, ?, 1, ?, ?)",
            )
            .bind(node_id)
            .bind(new_expiry)
            .bind(now_ms)
            .execute(pool)
            .await?;
            Ok(Some(LeaderRow {
                node_id: node_id.into(),
                epoch: 1,
                expires_at_ms: new_expiry,
                acquired_at_ms: now_ms,
            }))
        }
        Some((existing_node, existing_epoch, existing_expiry, existing_acquired)) => {
            let live = now_ms < existing_expiry;
            if live && existing_node != node_id {
                // Someone else holds a live lease. Don't steal it.
                return Ok(None);
            }
            // Take over (or refresh, if we already hold it). CAS on
            // epoch so two challengers racing for a dead leader only
            // let one win.
            let new_epoch = existing_epoch + 1;
            let new_acquired = if existing_node == node_id {
                existing_acquired
            } else {
                now_ms
            };
            let rows = sqlx::query(
                "UPDATE scheduler_leader
                   SET node_id = ?, epoch = ?, expires_at_ms = ?, acquired_at_ms = ?
                 WHERE id = 1 AND epoch = ?",
            )
            .bind(node_id)
            .bind(new_epoch)
            .bind(new_expiry)
            .bind(new_acquired)
            .bind(existing_epoch)
            .execute(pool)
            .await?
            .rows_affected();
            if rows == 0 {
                // Lost the race to another challenger. Treat as "not
                // leader right now"; caller retries on the next tick.
                return Ok(None);
            }
            Ok(Some(LeaderRow {
                node_id: node_id.into(),
                epoch: new_epoch,
                expires_at_ms: new_expiry,
                acquired_at_ms: new_acquired,
            }))
        }
    }
}

/// Extend the leader lease. Succeeds only if this process still owns
/// the row at `expected_epoch`; returns `Conflict` if the epoch has
/// moved (which means another node stole the role or the row was
/// wiped). Matches the tablet-renew shape.
pub async fn renew(
    pool: &SqlitePool,
    node_id: &str,
    expected_epoch: i64,
    new_expires_at_ms: i64,
) -> MetaResult<LeaderRow> {
    let rows = sqlx::query(
        "UPDATE scheduler_leader
           SET expires_at_ms = ?
         WHERE id = 1 AND node_id = ? AND epoch = ?",
    )
    .bind(new_expires_at_ms)
    .bind(node_id)
    .bind(expected_epoch)
    .execute(pool)
    .await?
    .rows_affected();
    if rows == 0 {
        return Err(MetaError::Conflict(format!(
            "scheduler leader renew: node={node_id} expected_epoch={expected_epoch}"
        )));
    }
    get(pool)
        .await?
        .ok_or_else(|| MetaError::NotFound("scheduler_leader row after renew".into()))
}

/// Read the current row, if any. Does not evaluate liveness —
/// callers compare `expires_at_ms` against their own wall clock.
pub async fn get(pool: &SqlitePool) -> MetaResult<Option<LeaderRow>> {
    let row: Option<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT node_id, epoch, expires_at_ms, acquired_at_ms
           FROM scheduler_leader WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(n, e, x, a)| LeaderRow {
        node_id: n,
        epoch: e,
        expires_at_ms: x,
        acquired_at_ms: a,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations;

    async fn pool() -> SqlitePool {
        let p = SqlitePool::connect("sqlite::memory:").await.unwrap();
        migrations::apply(&p).await.unwrap();
        p
    }

    #[tokio::test]
    async fn first_acquire_inserts_the_row_with_epoch_one() {
        let p = pool().await;
        let lease = try_acquire(&p, "node-a", 30_000, 1_000).await.unwrap();
        let lease = lease.expect("first acquire must succeed");
        assert_eq!(lease.node_id, "node-a");
        assert_eq!(lease.epoch, 1);
        assert_eq!(lease.expires_at_ms, 31_000);
        assert_eq!(lease.acquired_at_ms, 1_000);
        assert_eq!(get(&p).await.unwrap(), Some(lease));
    }

    #[tokio::test]
    async fn second_node_cannot_steal_a_live_lease() {
        let p = pool().await;
        try_acquire(&p, "node-a", 30_000, 1_000)
            .await
            .unwrap()
            .expect("first acquire");
        // node-b tries while node-a's lease is live — should be denied.
        let try_b = try_acquire(&p, "node-b", 30_000, 2_000).await.unwrap();
        assert!(try_b.is_none(), "live leader must not be displaced");
        let current = get(&p).await.unwrap().expect("row exists");
        assert_eq!(current.node_id, "node-a");
        assert_eq!(current.epoch, 1, "epoch unchanged by failed steal");
    }

    #[tokio::test]
    async fn expired_lease_can_be_taken_over_with_epoch_bump() {
        let p = pool().await;
        try_acquire(&p, "node-a", 1_000, 1_000)
            .await
            .unwrap()
            .expect("first acquire");
        // 5 seconds later — lease (1s duration) is long expired.
        let took = try_acquire(&p, "node-b", 30_000, 6_000).await.unwrap();
        let took = took.expect("stale-lease takeover must succeed");
        assert_eq!(took.node_id, "node-b");
        assert_eq!(took.epoch, 2, "epoch bumps on takeover");
        assert_eq!(took.acquired_at_ms, 6_000);
    }

    #[tokio::test]
    async fn acquire_when_already_leader_refreshes_expiry_and_bumps_epoch() {
        // The acquire path is idempotent for the current leader —
        // callers that use try_acquire on boot and then switch to
        // renew should get sensible behavior either way.
        let p = pool().await;
        try_acquire(&p, "node-a", 10_000, 1_000)
            .await
            .unwrap()
            .expect("first acquire");
        let refreshed = try_acquire(&p, "node-a", 30_000, 5_000)
            .await
            .unwrap()
            .expect("self-refresh");
        assert_eq!(refreshed.node_id, "node-a");
        assert_eq!(refreshed.epoch, 2, "self-refresh still bumps epoch");
        assert_eq!(refreshed.expires_at_ms, 35_000);
        assert_eq!(
            refreshed.acquired_at_ms, 1_000,
            "acquired_at is preserved on self-refresh"
        );
    }

    #[tokio::test]
    async fn renew_succeeds_at_expected_epoch_and_advances_expiry() {
        let p = pool().await;
        let initial = try_acquire(&p, "node-a", 10_000, 1_000)
            .await
            .unwrap()
            .expect("acquire");
        let renewed = renew(&p, "node-a", initial.epoch, 42_000).await.unwrap();
        assert_eq!(renewed.node_id, "node-a");
        assert_eq!(renewed.epoch, initial.epoch, "renew leaves epoch alone");
        assert_eq!(renewed.expires_at_ms, 42_000);
    }

    #[tokio::test]
    async fn renew_fails_with_conflict_when_epoch_has_moved() {
        let p = pool().await;
        let initial = try_acquire(&p, "node-a", 1_000, 1_000)
            .await
            .unwrap()
            .expect("acquire");
        // node-b takes over after expiry.
        try_acquire(&p, "node-b", 30_000, 5_000)
            .await
            .unwrap()
            .expect("takeover");
        // node-a still has its old epoch cached and tries to renew.
        let err = renew(&p, "node-a", initial.epoch, 99_000)
            .await
            .unwrap_err();
        assert!(
            matches!(err, MetaError::Conflict(_)),
            "expected Conflict, got {err:?}"
        );
    }

    #[tokio::test]
    async fn is_live_compares_against_supplied_wall_clock() {
        let row = LeaderRow {
            node_id: "x".into(),
            epoch: 1,
            expires_at_ms: 100,
            acquired_at_ms: 0,
        };
        assert!(row.is_live(50));
        assert!(!row.is_live(100));
        assert!(!row.is_live(200));
    }

    #[tokio::test]
    async fn get_returns_none_on_fresh_db() {
        let p = pool().await;
        assert!(get(&p).await.unwrap().is_none());
    }
}
