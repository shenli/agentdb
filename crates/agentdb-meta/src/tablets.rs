//! Tablets — runtime units that own shard ranges. See design §2.5.
//!
//! Every tablet row carries an `epoch` that increments on every
//! ownership change. Writes to object storage are tagged with their
//! epoch; stale-epoch writes are rejected at read time — the fencing-
//! token pattern committed to in design §5.5.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

/// On-disk shape of every `tablets` SELECT in this module. Aliased so the
/// `query_as` annotations don't have to spell the tuple at every call site.
type TabletRow = (
    i64,
    Option<String>,
    i64,
    i64,
    i64,
    Option<String>,
    String,
    String,
    Option<String>,
    i64,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tablet {
    pub tablet_id: i64,
    pub owner_node_id: Option<String>,
    pub epoch: i64,
    pub lease_expires_at_ms: i64,
    pub created_at_ms: i64,
    pub placement_scope_id: String,
    pub state: String,
    pub raft_group_id: Option<String>,
    pub encryption_policy_version: i64,
    /// Set by the leader when it wants the current owner to
    /// hand the tablet off cooperatively. Owner reads this
    /// in its TabletReconciler, drains its in-process box,
    /// then atomically transfers ownership via
    /// `complete_handoff` (which clears this column).
    /// Persisted across same-owner renewals so a slow
    /// reconciler doesn't lose the leader's intent.
    pub handoff_target_node_id: Option<String>,
}

/// Idempotent create: insert the tablet row if absent; do nothing if
/// already present. Returns the row as it now exists on-disk.
pub async fn get_or_create(pool: &AnyPool, tablet_id: i64, now_ms: i64) -> MetaResult<Tablet> {
    // Try insert first; if the PK already exists we fall through to
    // lookup. `ON CONFLICT DO NOTHING` is idempotent across SQLite/Postgres.
    sqlx::query(
        "INSERT INTO tablets (tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms)
         VALUES ($1, NULL, 0, 0, $2)
         ON CONFLICT (tablet_id) DO NOTHING",
    )
    .bind(tablet_id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    get(pool, tablet_id)
        .await?
        .ok_or_else(|| MetaError::NotFound(format!("tablet {tablet_id} after insert")))
}

pub async fn create_with_placement(
    pool: &AnyPool,
    tablet_id: i64,
    placement_scope_id: &str,
    state: &str,
    raft_group_id: Option<&str>,
    encryption_policy_version: i64,
    now_ms: i64,
) -> MetaResult<Tablet> {
    validate_state(state)?;
    if encryption_policy_version < 0 {
        return Err(MetaError::InvalidInput(
            "encryption_policy_version must be non-negative".into(),
        ));
    }
    if get(pool, tablet_id).await?.is_some() {
        return Err(MetaError::Conflict(format!("tablet {tablet_id} exists")));
    }
    sqlx::query(
        "INSERT INTO tablets
            (tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms,
             placement_scope_id, state, raft_group_id, encryption_policy_version)
         VALUES ($1, NULL, 0, 0, $2, $3, $4, $5, $6)",
    )
    .bind(tablet_id)
    .bind(now_ms)
    .bind(placement_scope_id)
    .bind(state)
    .bind(raft_group_id)
    .bind(encryption_policy_version)
    .execute(pool)
    .await?;
    get(pool, tablet_id)
        .await?
        .ok_or_else(|| MetaError::NotFound(format!("tablet {tablet_id} after create")))
}

pub async fn get(pool: &AnyPool, tablet_id: i64) -> MetaResult<Option<Tablet>> {
    let row: Option<TabletRow> = sqlx::query_as(
        "SELECT tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms,
                handoff_target_node_id, placement_scope_id, state, raft_group_id,
                encryption_policy_version
         FROM tablets WHERE tablet_id = $1",
    )
    .bind(tablet_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_tablet))
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
           SET owner_node_id       = $1,
               epoch               = epoch + 1,
               lease_expires_at_ms = $2
         WHERE tablet_id = $3 AND epoch = $4",
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
           AND lease_expires_at_ms + $1 < $2
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
               AND lease_expires_at_ms + $1 < $2",
        )
        .bind(safety_margin_ms)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(ids)
}

/// Forcibly relinquish a single tablet — clear `owner_node_id`,
/// zero `lease_expires_at_ms`, and bump `epoch`. Returns `true`
/// when the row was relinquished by this call, `false` when the
/// row was already unowned (the call was a no-op) or absent.
///
/// Used by the leader's active-rebalance pass to take a tablet
/// away from an over-loaded alive node so the placement pass
/// can hand it to an under-loaded one. The previous owner's
/// in-memory lease becomes stale on its next renewal — the
/// epoch bump means `assume_ownership` returns `Conflict`,
/// which the lease machinery surfaces as `LeaseLost` to the
/// data path. In-flight writes against the relinquished tablet
/// fail fast with a 503; that is the same behavior as a real
/// failover, just operator-triggered.
///
/// Cooperative ("please give up") handoff is explicit follow-up
/// work; today's loop uses force-relinquish because the
/// blast radius is bounded and the code path is shared with
/// the existing failover mechanism.
pub async fn relinquish(pool: &AnyPool, tablet_id: i64) -> MetaResult<bool> {
    let rows = sqlx::query(
        "UPDATE tablets \
            SET owner_node_id = NULL, lease_expires_at_ms = 0, epoch = epoch + 1 \
            WHERE tablet_id = $1 AND owner_node_id IS NOT NULL",
    )
    .bind(tablet_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Pre-claim an unowned tablet for `node_id` without granting a
/// lease. Returns `true` when the row was assigned by this call,
/// `false` when it was already owned (by anyone — including us)
/// and the call was a no-op.
///
/// This is the placement primitive used by `TabletPlacement` on
/// the scheduler leader. It deliberately does **not** bump the
/// epoch — the receiving node will bump it when it acquires its
/// lease via `assume_ownership`. Until then the row is "promised
/// to" but not "owned by" the target node, with
/// `lease_expires_at_ms` left untouched (typically 0). Callers
/// that route via `LocationProxy` must therefore continue to
/// treat `lease_expires_at_ms < now` as "not authoritatively
/// owned".
pub async fn assign_to(pool: &AnyPool, tablet_id: i64, node_id: &str) -> MetaResult<bool> {
    let rows = sqlx::query(
        "UPDATE tablets SET owner_node_id = $1 \
           WHERE tablet_id = $2 AND owner_node_id IS NULL",
    )
    .bind(node_id)
    .bind(tablet_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Every tablet currently owned by `node_id`. Used by the
/// per-node reconciler to discover tablets it should be running
/// (see `agentdb_core::tablet_reconciler`).
pub async fn list_owned_by(pool: &AnyPool, node_id: &str) -> MetaResult<Vec<Tablet>> {
    let rows: Vec<TabletRow> = sqlx::query_as(
        "SELECT tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms,
                handoff_target_node_id, placement_scope_id, state, raft_group_id,
                encryption_policy_version
           FROM tablets WHERE owner_node_id = $1 ORDER BY tablet_id",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_tablet).collect())
}

/// Every tablet row, ordered by `tablet_id`. Point-in-time snapshot —
/// ownership can change between this return and the caller acting on
/// it. Used by `GET /api/admin/tablets` and by the scheduler when it
/// needs a cluster-wide view of which tablets exist.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<Tablet>> {
    let rows: Vec<TabletRow> = sqlx::query_as(
        "SELECT tablet_id, owner_node_id, epoch, lease_expires_at_ms, created_at_ms,
                handoff_target_node_id, placement_scope_id, state, raft_group_id,
                encryption_policy_version
           FROM tablets ORDER BY tablet_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_tablet).collect())
}

pub async fn set_state(pool: &AnyPool, tablet_id: i64, state: &str) -> MetaResult<bool> {
    validate_state(state)?;
    let rows = sqlx::query("UPDATE tablets SET state = $1, epoch = epoch + 1 WHERE tablet_id = $2")
        .bind(state)
        .bind(tablet_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

pub async fn set_raft_group(
    pool: &AnyPool,
    tablet_id: i64,
    raft_group_id: Option<&str>,
) -> MetaResult<bool> {
    let rows = sqlx::query("UPDATE tablets SET raft_group_id = $1 WHERE tablet_id = $2")
        .bind(raft_group_id)
        .bind(tablet_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

pub async fn set_encryption_policy_version(
    pool: &AnyPool,
    tablet_id: i64,
    encryption_policy_version: i64,
) -> MetaResult<bool> {
    if encryption_policy_version < 0 {
        return Err(MetaError::InvalidInput(
            "encryption_policy_version must be non-negative".into(),
        ));
    }
    let rows = sqlx::query(
        "UPDATE tablets
            SET encryption_policy_version = $1, epoch = epoch + 1
          WHERE tablet_id = $2",
    )
    .bind(encryption_policy_version)
    .bind(tablet_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Mark a still-owned tablet for cooperative handoff to
/// `target_node_id`. Returns `true` when the row had its
/// signal set (or already pointed at the same target — the
/// call is idempotent), `false` when the tablet has no
/// current owner (no point asking for a handoff) or the row
/// is absent.
///
/// Pairs with `complete_handoff`: the source's
/// `TabletReconciler` reads this column, drains, and calls
/// the completion. Until completion, the row's `owner` and
/// `epoch` stay put — the source is still serving writes
/// for the tablet.
pub async fn mark_for_handoff(
    pool: &AnyPool,
    tablet_id: i64,
    target_node_id: &str,
) -> MetaResult<bool> {
    let rows = sqlx::query(
        "UPDATE tablets \
            SET handoff_target_node_id = $1 \
            WHERE tablet_id = $2 AND owner_node_id IS NOT NULL",
    )
    .bind(target_node_id)
    .bind(tablet_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Atomically transfer ownership from the current owner
/// (`expected_source`) to whatever node id was previously
/// recorded in `handoff_target_node_id`. Clears the handoff
/// signal, zeros the lease, and bumps the epoch in one
/// statement.
///
/// Returns `Some(new_owner)` on success and `None` when:
///   * the row's owner was someone other than
///     `expected_source` (a takeover happened mid-handoff);
///   * the row had no `handoff_target_node_id` set (the
///     leader cancelled, or this is just a stale read).
///
/// Called by the source's `TabletReconciler` after it has
/// drained its in-process box.
pub async fn complete_handoff(
    pool: &AnyPool,
    tablet_id: i64,
    expected_source: &str,
) -> MetaResult<Option<String>> {
    // Read first so we can return the new owner. Then UPDATE
    // with a stricter predicate so a concurrent admin DELETE
    // or another node's `assume_ownership` doesn't get
    // clobbered. Race-free under one transaction.
    let mut tx = pool.begin().await?;
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT owner_node_id, handoff_target_node_id \
            FROM tablets WHERE tablet_id = $1",
    )
    .bind(tablet_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((owner, target)) = row else {
        tx.rollback().await?;
        return Ok(None);
    };
    let (Some(owner), Some(target)) = (owner, target) else {
        tx.rollback().await?;
        return Ok(None);
    };
    if owner != expected_source {
        tx.rollback().await?;
        return Ok(None);
    }
    let updated = sqlx::query(
        "UPDATE tablets \
            SET owner_node_id = $1, \
                handoff_target_node_id = NULL, \
                lease_expires_at_ms = 0, \
                epoch = epoch + 1 \
            WHERE tablet_id = $2 AND owner_node_id = $3 AND handoff_target_node_id = $4",
    )
    .bind(&target)
    .bind(tablet_id)
    .bind(&owner)
    .bind(&target)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok((updated > 0).then_some(target))
}

/// Drop the handoff signal without changing ownership. Used
/// by the leader to retract a previously-issued handoff
/// (e.g., the original target died before the source
/// completed).
pub async fn cancel_handoff(pool: &AnyPool, tablet_id: i64) -> MetaResult<bool> {
    let rows = sqlx::query(
        "UPDATE tablets SET handoff_target_node_id = NULL \
            WHERE tablet_id = $1 AND handoff_target_node_id IS NOT NULL",
    )
    .bind(tablet_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

fn validate_state(state: &str) -> MetaResult<()> {
    match state {
        "creating" | "serving" | "splitting" | "merging" | "draining" | "tombstoned" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid tablet state `{other}`"
        ))),
    }
}

fn into_tablet(
    (
        tablet_id,
        owner_node_id,
        epoch,
        lease_expires_at_ms,
        created_at_ms,
        handoff_target_node_id,
        placement_scope_id,
        state,
        raft_group_id,
        encryption_policy_version,
    ): TabletRow,
) -> Tablet {
    Tablet {
        tablet_id,
        owner_node_id,
        epoch,
        lease_expires_at_ms,
        created_at_ms,
        placement_scope_id,
        state,
        raft_group_id,
        encryption_policy_version,
        handoff_target_node_id,
    }
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
        assert_eq!(t.placement_scope_id, "default");
        assert_eq!(t.state, "serving");
        assert_eq!(t.raft_group_id, None);
        assert_eq!(t.encryption_policy_version, 0);
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
    async fn create_with_placement_sets_target_metadata() {
        let p = pool().await;
        let t = create_with_placement(&p, 7, "tenant/acme", "creating", Some("raft/7"), 3, 100)
            .await
            .unwrap();
        assert_eq!(t.tablet_id, 7);
        assert_eq!(t.placement_scope_id, "tenant/acme");
        assert_eq!(t.state, "creating");
        assert_eq!(t.raft_group_id.as_deref(), Some("raft/7"));
        assert_eq!(t.encryption_policy_version, 3);
    }

    #[tokio::test]
    async fn create_with_placement_rejects_existing_tablet() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        let err = create_with_placement(&p, 7, "tenant/acme", "creating", None, 0, 100)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict(_)));
    }

    #[tokio::test]
    async fn set_state_bumps_epoch() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assert!(set_state(&p, 7, "splitting").await.unwrap());
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(t.state, "splitting");
        assert_eq!(t.epoch, 1);
    }

    #[tokio::test]
    async fn set_state_rejects_unknown_state() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        let err = set_state(&p, 7, "half-split").await.unwrap_err();
        assert!(matches!(err, MetaError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn set_raft_group_updates_metadata_without_epoch_bump() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assert!(set_raft_group(&p, 7, Some("raft/7")).await.unwrap());
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(t.raft_group_id.as_deref(), Some("raft/7"));
        assert_eq!(t.epoch, 0);
    }

    #[tokio::test]
    async fn set_encryption_policy_version_bumps_epoch() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assert!(set_encryption_policy_version(&p, 7, 5).await.unwrap());
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(t.encryption_policy_version, 5);
        assert_eq!(t.epoch, 1);
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
    async fn mark_for_handoff_only_acts_on_owned_rows() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        // Unowned → no-op.
        assert!(!mark_for_handoff(&p, 7, "node-b").await.unwrap());
        let t = get(&p, 7).await.unwrap().unwrap();
        assert!(t.handoff_target_node_id.is_none());
        // Now claim and re-mark — should write.
        assume_ownership(&p, 7, 0, "node-a", 5_000).await.unwrap();
        assert!(mark_for_handoff(&p, 7, "node-b").await.unwrap());
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(t.handoff_target_node_id.as_deref(), Some("node-b"));
        // Owner and epoch are NOT touched — source is still
        // serving writes for this tablet until it acknowledges.
        assert_eq!(t.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(t.epoch, 1);
    }

    #[tokio::test]
    async fn complete_handoff_atomically_transfers_clears_signal_and_bumps_epoch() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assume_ownership(&p, 7, 0, "node-a", 5_000).await.unwrap();
        mark_for_handoff(&p, 7, "node-b").await.unwrap();

        let new_owner = complete_handoff(&p, 7, "node-a").await.unwrap();
        assert_eq!(new_owner.as_deref(), Some("node-b"));
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(t.owner_node_id.as_deref(), Some("node-b"));
        assert!(t.handoff_target_node_id.is_none());
        assert_eq!(t.lease_expires_at_ms, 0, "lease must be reset");
        assert_eq!(
            t.epoch, 2,
            "epoch must advance so the source's lease is fenced"
        );
    }

    #[tokio::test]
    async fn complete_handoff_returns_none_when_source_does_not_match() {
        // A takeover happened mid-handoff (e.g., source died,
        // reaper cleared and a new node took over). The expected
        // source no longer matches; complete_handoff must NOT
        // overwrite the new ownership.
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assume_ownership(&p, 7, 0, "current-owner", 5_000)
            .await
            .unwrap();
        mark_for_handoff(&p, 7, "node-b").await.unwrap();

        let result = complete_handoff(&p, 7, "stale-owner").await.unwrap();
        assert!(result.is_none());
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(
            t.owner_node_id.as_deref(),
            Some("current-owner"),
            "row must be untouched when expected_source mismatches"
        );
        assert_eq!(t.handoff_target_node_id.as_deref(), Some("node-b"));
    }

    #[tokio::test]
    async fn complete_handoff_returns_none_when_no_signal_was_set() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assume_ownership(&p, 7, 0, "node-a", 5_000).await.unwrap();
        // No mark_for_handoff call.
        let result = complete_handoff(&p, 7, "node-a").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn cancel_handoff_clears_signal_idempotently() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        assume_ownership(&p, 7, 0, "node-a", 5_000).await.unwrap();
        mark_for_handoff(&p, 7, "node-b").await.unwrap();
        assert!(cancel_handoff(&p, 7).await.unwrap());
        // Owner stays put; only the signal goes away.
        let t = get(&p, 7).await.unwrap().unwrap();
        assert_eq!(t.owner_node_id.as_deref(), Some("node-a"));
        assert!(t.handoff_target_node_id.is_none());
        // Second call is a no-op.
        assert!(!cancel_handoff(&p, 7).await.unwrap());
    }

    #[tokio::test]
    async fn relinquish_clears_owner_zeros_lease_and_bumps_epoch() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        // Owner+lease in place at epoch 1.
        let claimed = assume_ownership(&p, 7, 0, "node-a", 5_000).await.unwrap();
        assert_eq!(claimed.epoch, 1);
        assert_eq!(claimed.owner_node_id.as_deref(), Some("node-a"));

        assert!(relinquish(&p, 7).await.unwrap());
        let after = get(&p, 7).await.unwrap().unwrap();
        assert!(after.owner_node_id.is_none(), "owner must be cleared");
        assert_eq!(after.lease_expires_at_ms, 0);
        assert_eq!(after.epoch, 2, "epoch must monotonically advance");
        // The previous owner's `assume_ownership(expected_epoch=1)`
        // would now fail — locked-out semantics, same as failover.
        let err = assume_ownership(&p, 7, 1, "node-a", 9_000)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Conflict(_)));
    }

    #[tokio::test]
    async fn relinquish_is_idempotent_and_skips_unowned_or_unknown() {
        let p = pool().await;
        get_or_create(&p, 7, 100).await.unwrap();
        // Unowned row — no-op.
        assert!(!relinquish(&p, 7).await.unwrap());
        // Unknown id — no-op.
        assert!(!relinquish(&p, 999).await.unwrap());
        // Now claim and relinquish twice; second call is no-op.
        assume_ownership(&p, 7, 0, "node-a", 5_000).await.unwrap();
        assert!(relinquish(&p, 7).await.unwrap());
        assert!(!relinquish(&p, 7).await.unwrap());
    }

    #[tokio::test]
    async fn assign_to_claims_unowned_only() {
        let p = pool().await;
        get_or_create(&p, 1, 100).await.unwrap();
        get_or_create(&p, 2, 100).await.unwrap();
        // Tablet 1 is unowned → assign succeeds.
        assert!(assign_to(&p, 1, "node-a").await.unwrap());
        let t1 = get(&p, 1).await.unwrap().unwrap();
        assert_eq!(t1.owner_node_id.as_deref(), Some("node-a"));
        // Epoch and lease must NOT change — the row is "promised
        // to" node-a, not "owned by" node-a until it acquires.
        assert_eq!(t1.epoch, 0);
        assert_eq!(t1.lease_expires_at_ms, 0);
        // Already-owned tablet → no-op.
        assume_ownership(&p, 2, 0, "node-b", 5_000).await.unwrap();
        assert!(!assign_to(&p, 2, "node-c").await.unwrap());
        let t2 = get(&p, 2).await.unwrap().unwrap();
        assert_eq!(t2.owner_node_id.as_deref(), Some("node-b"));
    }

    #[tokio::test]
    async fn assign_to_unknown_tablet_is_a_noop() {
        // Predicate doesn't match → 0 rows affected → false.
        // Idempotent: useful when callers list candidates and a
        // race deleted the row between list and assign.
        let p = pool().await;
        assert!(!assign_to(&p, 999, "node-a").await.unwrap());
    }

    #[tokio::test]
    async fn list_owned_by_filters_to_one_node() {
        let p = pool().await;
        for id in [1, 2, 3, 4] {
            get_or_create(&p, id, 100).await.unwrap();
        }
        assume_ownership(&p, 1, 0, "node-a", 500).await.unwrap();
        assume_ownership(&p, 3, 0, "node-a", 500).await.unwrap();
        assume_ownership(&p, 2, 0, "node-b", 500).await.unwrap();
        // Tablet 4 stays unowned.

        let mine = list_owned_by(&p, "node-a").await.unwrap();
        let ids: Vec<_> = mine.iter().map(|t| t.tablet_id).collect();
        assert_eq!(ids, vec![1, 3]);

        let theirs = list_owned_by(&p, "node-b").await.unwrap();
        assert_eq!(theirs.len(), 1);
        assert_eq!(theirs[0].tablet_id, 2);

        let none = list_owned_by(&p, "ghost").await.unwrap();
        assert!(none.is_empty());
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
