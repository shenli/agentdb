//! Store-incarnation admission — the control-plane authority on which
//! physical store currently owns each node id.
//!
//! The local `IDENTITY.json` (agentdb_storage::store_identity) proves a
//! data directory is internally consistent, but it cannot answer two
//! questions that only the cluster knows: has this incarnation been
//! *retired* (its disk deliberately replaced), and does *another* live
//! incarnation already hold this node id? This module answers both from
//! the `store_incarnations` table so startup can fail closed.
//!
//! Model: one row per `(node_id, store_incarnation)`; at most one row per
//! node id is `active`. Admission (`admit`) is idempotent for a normal
//! reboot and refuses the dangerous cases. Replacing a store is an
//! explicit two-step — `retire` the old incarnation, then the new one is
//! admitted on its next boot — so a swapped-in fresh disk cannot silently
//! displace a still-running node, and a retired disk cannot rejoin.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

/// One incarnation's registry row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incarnation {
    pub node_id: String,
    pub store_incarnation: String,
    pub state: String,
    pub admitted_at_ms: i64,
    pub retired_at_ms: Option<i64>,
}

/// What admission did for a booting store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// First time this node id has ever registered.
    First,
    /// This exact incarnation was already active — a normal reboot.
    Rejoin,
    /// A previous incarnation was retired and this fresh one takes over.
    Replaced,
}

const STATE_ACTIVE: &str = "active";
const STATE_RETIRED: &str = "retired";

type IncarnationRow = (String, String, String, i64, Option<i64>);

fn row_to_incarnation(row: IncarnationRow) -> Incarnation {
    Incarnation {
        node_id: row.0,
        store_incarnation: row.1,
        state: row.2,
        admitted_at_ms: row.3,
        retired_at_ms: row.4,
    }
}

/// Every incarnation ever recorded for a node id, newest admission first.
pub async fn list_for_node(pool: &AnyPool, node_id: &str) -> MetaResult<Vec<Incarnation>> {
    let rows: Vec<IncarnationRow> = sqlx::query_as(
        "SELECT node_id, store_incarnation, state, admitted_at_ms, retired_at_ms \
         FROM store_incarnations WHERE node_id = $1 ORDER BY admitted_at_ms DESC",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_to_incarnation).collect())
}

/// Admit a booting store's incarnation, or fail closed.
///
/// - No rows for this node id → register active, `First`.
/// - This incarnation already active → `Rejoin` (idempotent normal boot).
/// - This incarnation is retired → **error**: a replaced disk must not
///   rejoin.
/// - A *different* incarnation is active → **error**: another live store
///   holds this node id; the operator must retire it first.
/// - Only retired incarnations exist (no active) → register active,
///   `Replaced`.
pub async fn admit(
    pool: &AnyPool,
    node_id: &str,
    store_incarnation: &str,
    now_ms: i64,
) -> MetaResult<Admission> {
    let existing = list_for_node(pool, node_id).await?;

    if let Some(mine) = existing
        .iter()
        .find(|row| row.store_incarnation == store_incarnation)
    {
        return match mine.state.as_str() {
            STATE_ACTIVE => Ok(Admission::Rejoin),
            STATE_RETIRED => Err(MetaError::Conflict(format!(
                "store incarnation {store_incarnation} for node {node_id} was retired; \
                 a replaced store must not rejoin (mint a fresh data directory)"
            ))),
            other => Err(MetaError::Invariant(format!(
                "store incarnation {store_incarnation} for node {node_id} has unknown state {other:?}"
            ))),
        };
    }

    if let Some(active) = existing.iter().find(|row| row.state == STATE_ACTIVE) {
        return Err(MetaError::Conflict(format!(
            "node {node_id} is already held by live store incarnation {}; refusing to admit {} \
             (retire the old incarnation first if this is a deliberate replacement)",
            active.store_incarnation, store_incarnation
        )));
    }

    // No active incarnation: either first boot or a clean replacement.
    let outcome = if existing.is_empty() {
        Admission::First
    } else {
        Admission::Replaced
    };
    sqlx::query(
        "INSERT INTO store_incarnations \
         (node_id, store_incarnation, state, admitted_at_ms, retired_at_ms) \
         VALUES ($1, $2, $3, $4, NULL)",
    )
    .bind(node_id)
    .bind(store_incarnation)
    .bind(STATE_ACTIVE)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(outcome)
}

/// Retire the currently-active incarnation for a node id so a replacement
/// can be admitted. Idempotent: retiring when nothing is active is a
/// no-op. Returns the incarnation that was retired, if any.
pub async fn retire_active(
    pool: &AnyPool,
    node_id: &str,
    now_ms: i64,
) -> MetaResult<Option<String>> {
    let active = list_for_node(pool, node_id)
        .await?
        .into_iter()
        .find(|row| row.state == STATE_ACTIVE);
    let Some(active) = active else {
        return Ok(None);
    };
    sqlx::query(
        "UPDATE store_incarnations SET state = $1, retired_at_ms = $2 \
         WHERE node_id = $3 AND store_incarnation = $4",
    )
    .bind(STATE_RETIRED)
    .bind(now_ms)
    .bind(node_id)
    .bind(&active.store_incarnation)
    .execute(pool)
    .await?;
    Ok(Some(active.store_incarnation))
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

    #[tokio::test]
    async fn first_boot_then_reboot_is_idempotent() {
        let pool = pool().await;
        assert_eq!(
            admit(&pool, "node-a", "inc-1", 1_000).await.unwrap(),
            Admission::First
        );
        // Same incarnation reboots.
        assert_eq!(
            admit(&pool, "node-a", "inc-1", 2_000).await.unwrap(),
            Admission::Rejoin
        );
        assert_eq!(list_for_node(&pool, "node-a").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn conflicting_live_incarnation_fails_closed() {
        let pool = pool().await;
        admit(&pool, "node-a", "inc-1", 1_000).await.unwrap();
        // A fresh disk claiming the same node id while inc-1 is still live.
        let err = admit(&pool, "node-a", "inc-2", 2_000).await.unwrap_err();
        assert!(format!("{err}").contains("already held by live store incarnation"));
    }

    #[tokio::test]
    async fn retired_incarnation_cannot_rejoin() {
        let pool = pool().await;
        admit(&pool, "node-a", "inc-1", 1_000).await.unwrap();
        assert_eq!(
            retire_active(&pool, "node-a", 1_500).await.unwrap(),
            Some("inc-1".to_string())
        );
        // The retired disk tries to come back.
        let err = admit(&pool, "node-a", "inc-1", 2_000).await.unwrap_err();
        assert!(format!("{err}").contains("was retired"));
    }

    #[tokio::test]
    async fn replacement_admitted_after_retire() {
        let pool = pool().await;
        admit(&pool, "node-a", "inc-1", 1_000).await.unwrap();
        retire_active(&pool, "node-a", 1_500).await.unwrap();
        assert_eq!(
            admit(&pool, "node-a", "inc-2", 2_000).await.unwrap(),
            Admission::Replaced
        );
        // inc-2 is now the live one; retiring again returns it.
        assert_eq!(
            retire_active(&pool, "node-a", 3_000).await.unwrap(),
            Some("inc-2".to_string())
        );
    }

    #[tokio::test]
    async fn retire_with_nothing_active_is_noop() {
        let pool = pool().await;
        assert_eq!(retire_active(&pool, "node-a", 1_000).await.unwrap(), None);
    }
}
