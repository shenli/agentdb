//! Cluster root — the control-plane record of *which cluster* a metadata
//! database is, and its bootstrap generation.
//!
//! This is the last cross-check of the durable-identity story
//! (`distributed-safety-hardening.md` §3). The local `IDENTITY.json`
//! (`agentdb_storage::store_identity`) proves a data directory belongs to a
//! given cluster id, and `store_admission` proves no other live incarnation
//! holds a node id — but neither catches a node pointed at the *wrong metadata
//! database*: a stale `AGENTDB_META_URL`, a copied or restored control plane,
//! or two clusters accidentally sharing one DB. This module lets startup fail
//! closed on that: a node cross-checks its store cluster id against the single
//! `cluster_root` row.
//!
//! `bootstrap_generation` is a monotonically increasing counter, bumped on a
//! deliberate re-bootstrap or metadata restore. Nodes persist the highest
//! generation they have seen (a local watermark); a metadata DB presenting a
//! *lower* generation than a node already observed signals a rollback
//! (restore-to-older-backup) and fails the node closed rather than risk
//! split-brain.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

const SINGLETON_ID: i64 = 1;

/// The cluster root row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRoot {
    pub cluster_id: String,
    pub bootstrap_generation: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

type ClusterRootRow = (String, i64, i64, i64);

fn row_to_root(row: ClusterRootRow) -> ClusterRoot {
    ClusterRoot {
        cluster_id: row.0,
        bootstrap_generation: row.1,
        created_at_ms: row.2,
        updated_at_ms: row.3,
    }
}

/// Read the cluster root, or `None` if the cluster has not been rooted yet.
pub async fn read(pool: &AnyPool) -> MetaResult<Option<ClusterRoot>> {
    let row: Option<ClusterRootRow> = sqlx::query_as(
        "SELECT cluster_id, bootstrap_generation, created_at_ms, updated_at_ms \
         FROM cluster_root WHERE id = $1",
    )
    .bind(SINGLETON_ID)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_to_root))
}

/// Establish the cluster root on first boot, or return the existing one.
///
/// The first node to boot roots the cluster with its own store `cluster_id`
/// at generation 1. Every later boot reads that row back. This is idempotent
/// and races safely: a concurrent insert by another node loses the unique-id
/// primary key and we re-read the winner.
pub async fn ensure(pool: &AnyPool, cluster_id: &str, now_ms: i64) -> MetaResult<ClusterRoot> {
    if let Some(existing) = read(pool).await? {
        return Ok(existing);
    }
    // Best-effort insert; `ON CONFLICT DO NOTHING` keeps a two-node race from
    // erroring, and we re-read to return whichever insert won.
    sqlx::query(
        "INSERT INTO cluster_root \
         (id, cluster_id, bootstrap_generation, created_at_ms, updated_at_ms) \
         VALUES ($1, $2, 1, $3, $3) ON CONFLICT (id) DO NOTHING",
    )
    .bind(SINGLETON_ID)
    .bind(cluster_id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    read(pool)
        .await?
        .ok_or_else(|| MetaError::Invariant("cluster_root vanished after insert".into()))
}

/// Cross-check a booting node against the cluster root and fail closed on a
/// mismatch. `seen_generation` is the highest bootstrap generation this node
/// has previously observed (its local watermark), or `None` if it has never
/// recorded one.
///
/// `enforce_cluster_id` gates the cluster-id match: pass `true` only when the
/// node's cluster id was *explicitly configured* (`AGENTDB_CLUSTER_ID`), so it
/// is a shared cross-node invariant. When it is `false` — the cluster id was
/// minted per-node because none was configured — there is no shared identifier
/// to compare, so the id match is skipped and only the generation check runs.
/// This mirrors `store_identity`, which likewise enforces a cluster id only
/// when one is configured.
///
/// - **Cluster-id mismatch** (only when `enforce_cluster_id`) → error: this
///   store belongs to a different cluster than the metadata DB describes (wrong
///   `AGENTDB_META_URL`, or the control plane was replaced).
/// - **Generation rollback** (`root.generation < seen_generation`) → error:
///   the metadata DB was restored to an older state than this node already
///   saw; joining it risks split-brain.
/// - Otherwise `Ok(root)`; the caller then persists `root.bootstrap_generation`
///   as its new watermark.
pub async fn verify_join(
    pool: &AnyPool,
    store_cluster_id: &str,
    enforce_cluster_id: bool,
    seen_generation: Option<i64>,
    now_ms: i64,
) -> MetaResult<ClusterRoot> {
    let root = ensure(pool, store_cluster_id, now_ms).await?;
    if enforce_cluster_id && root.cluster_id != store_cluster_id {
        return Err(MetaError::Conflict(format!(
            "this store belongs to cluster {:?}, but the metadata database is rooted for cluster \
             {:?}; refusing to join the wrong cluster (check AGENTDB_META_URL / the control plane)",
            store_cluster_id, root.cluster_id
        )));
    }
    if let Some(seen) = seen_generation {
        if root.bootstrap_generation < seen {
            return Err(MetaError::Conflict(format!(
                "metadata database presents bootstrap generation {} but this node already saw {}; \
                 the control plane appears rolled back (restore-to-older-backup) — refusing to join \
                 to avoid split-brain",
                root.bootstrap_generation, seen
            )));
        }
    }
    Ok(root)
}

/// Bump the bootstrap generation — the deliberate re-bootstrap / post-restore
/// signal that tells every node "the control plane moved forward on purpose."
/// Returns the new generation. Errors if the cluster has not been rooted yet.
pub async fn bump_generation(pool: &AnyPool, now_ms: i64) -> MetaResult<i64> {
    let updated = sqlx::query(
        "UPDATE cluster_root SET bootstrap_generation = bootstrap_generation + 1, \
         updated_at_ms = $1 WHERE id = $2",
    )
    .bind(now_ms)
    .bind(SINGLETON_ID)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(MetaError::NotFound(
            "cluster_root is not established; cannot bump generation".into(),
        ));
    }
    let root = read(pool)
        .await?
        .ok_or_else(|| MetaError::Invariant("cluster_root vanished after bump".into()))?;
    Ok(root.bootstrap_generation)
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
    async fn first_node_roots_the_cluster_and_others_read_it() {
        let pool = pool().await;
        assert!(read(&pool).await.unwrap().is_none());
        let root = ensure(&pool, "cluster-a", 1_000).await.unwrap();
        assert_eq!(root.cluster_id, "cluster-a");
        assert_eq!(root.bootstrap_generation, 1);
        // A second node with the same cluster id reads the same root (idempotent).
        let again = ensure(&pool, "cluster-a", 2_000).await.unwrap();
        assert_eq!(again, root);
    }

    #[tokio::test]
    async fn wrong_cluster_id_fails_closed_when_enforced() {
        let pool = pool().await;
        ensure(&pool, "cluster-a", 1_000).await.unwrap();
        let err = verify_join(&pool, "cluster-b", true, None, 2_000)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("wrong cluster"));
    }

    #[tokio::test]
    async fn per_node_minted_ids_join_when_not_enforced() {
        // No AGENTDB_CLUSTER_ID: each node mints its own random cluster id, so
        // they legitimately differ. With enforcement off, a second node joins
        // the root the first one established instead of failing closed.
        let pool = pool().await;
        ensure(&pool, "cluster-node-a-minted", 1_000).await.unwrap();
        let root = verify_join(&pool, "cluster-node-b-minted", false, None, 2_000)
            .await
            .unwrap();
        assert_eq!(root.cluster_id, "cluster-node-a-minted");
        // The generation guard still applies even without id enforcement.
        let err = verify_join(&pool, "cluster-node-c-minted", false, Some(9), 3_000)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("rolled back"));
    }

    #[tokio::test]
    async fn matching_cluster_and_advancing_generation_join_cleanly() {
        let pool = pool().await;
        ensure(&pool, "cluster-a", 1_000).await.unwrap();
        // Never-seen-before node: no watermark, joins.
        let root = verify_join(&pool, "cluster-a", true, None, 2_000)
            .await
            .unwrap();
        assert_eq!(root.bootstrap_generation, 1);
        // A re-bootstrap bumps the generation; a node that saw gen 1 accepts gen 2.
        assert_eq!(bump_generation(&pool, 3_000).await.unwrap(), 2);
        let root = verify_join(&pool, "cluster-a", true, Some(1), 4_000)
            .await
            .unwrap();
        assert_eq!(root.bootstrap_generation, 2);
    }

    #[tokio::test]
    async fn generation_rollback_fails_closed() {
        let pool = pool().await;
        ensure(&pool, "cluster-a", 1_000).await.unwrap();
        // This node previously saw generation 5, but the metadata DB is at 1
        // (restored from an older backup): refuse to join.
        let err = verify_join(&pool, "cluster-a", true, Some(5), 2_000)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("rolled back"));
    }

    #[tokio::test]
    async fn bump_before_root_is_an_error() {
        let pool = pool().await;
        let err = bump_generation(&pool, 1_000).await.unwrap_err();
        assert!(format!("{err}").contains("not established"));
    }
}
