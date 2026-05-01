//! Canonical session locator (M26 Phase 1).
//!
//! One row per globally-unique external session id, recording
//! the tenant that owns it, the tablet that currently hosts
//! it, and the shard slot it lives in. This is the metadata
//! that the runtime routing design centers on — every routing decision for
//! an existing session, once the M26 work lands fully, is a
//! lookup into this table rather than `FNV(tenant) % num_boxes`.
//!
//! Phase 1 (this PR) populates locators on session creation
//! but does NOT yet consult them on the routing path. Later
//! phases swap session-scoped reads, fork's parent lookup, and
//! per-session locking onto the locator. Keeping populate
//! decoupled from consume lets us roll forward safely: even if
//! a follow-up PR is reverted, the locator rows are harmless
//! historical metadata.
//!
//! See `docs/ARCHITECTURE.md`.

use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::AnyPool;

use crate::error::MetaResult;

/// Test-only counter incremented on every `get()` call. Used by
/// hot-path assertion tests to verify steady-state append does not
/// read the locator. Production reads of this counter are fine —
/// it's a `Relaxed` atomic.
pub static LOCATOR_GETS: AtomicU64 = AtomicU64::new(0);

/// (session_id, tenant_internal_id, tablet_id, shard_id,
/// created_at_ms, tenant_external_id). Aliased so the
/// `query_as` annotations don't have to spell the tuple at
/// every call site.
type LocatorRow = (String, i64, Option<i64>, i32, i64, Option<String>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLocator {
    pub session_id: String,
    /// **Informational only as of migration 012.** This is the
    /// per-process `TenantRegistry`'s local id and not
    /// guaranteed to be globally meaningful. Use
    /// `tenant_external_id` for routing and auth.
    pub tenant_internal_id: i64,
    /// Globally-unique external tenant identity. Added by
    /// migration 012 (M26 Phase 1b) and populated on mint
    /// going forward; older rows have `None` and callers
    /// should fall back to body-supplied `tenant_id` for those.
    pub tenant_external_id: Option<String>,
    /// `None` when the session is registered before its
    /// hosting tablet is recorded (rare; typically only during
    /// migration). Routing must fall back to tenant-hash when
    /// absent rather than refuse the request.
    pub tablet_id: Option<i64>,
    pub shard_id: i32,
    pub created_at_ms: i64,
}

/// Upsert idempotency policy:
///
/// - `INSERT OR IGNORE` on the PK. If the locator already
///   exists, do not overwrite — the existing record's
///   tablet_id is the authoritative current placement, and
///   blindly replacing it on every write would erase the
///   updates that handoff/migration paths make.
/// - To explicitly retarget a session (e.g., after a tablet
///   handoff completes), call [`update_tablet`].
///
/// Returns `true` when this call inserted a new row, `false`
/// when one was already present. Callers can use the bool to
/// gate idempotent writes (e.g., audit log entries that
/// shouldn't fire twice).
pub async fn upsert(
    pool: &AnyPool,
    session_id: &str,
    tenant_internal_id: i64,
    tenant_external_id: &str,
    tablet_id: Option<i64>,
    shard_id: i32,
    created_at_ms: i64,
) -> MetaResult<bool> {
    let rows = sqlx::query(
        "INSERT OR IGNORE INTO session_locators \
            (session_id, tenant_internal_id, tenant_external_id, tablet_id, shard_id, created_at_ms) \
            VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(session_id)
    .bind(tenant_internal_id)
    .bind(tenant_external_id)
    .bind(tablet_id)
    .bind(shard_id)
    .bind(created_at_ms)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Look up a single locator. Returns `None` when no row
/// matches — older sessions created before M26 don't have
/// locators and the routing layer must fall back to the
/// pre-locator path.
pub async fn get(pool: &AnyPool, session_id: &str) -> MetaResult<Option<SessionLocator>> {
    LOCATOR_GETS.fetch_add(1, Ordering::Relaxed);
    let row: Option<LocatorRow> = sqlx::query_as(
        "SELECT session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id \
            FROM session_locators WHERE session_id = ?",
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_locator))
}

/// Retarget a session to a new tablet. Used by tablet
/// handoff/migration paths so subsequent locator-routed
/// reads land on the new owner.
///
/// Returns `true` when the row existed and was updated,
/// `false` when no such session id is in the locator table.
/// Idempotent: a no-change update (same tablet_id) still
/// reports `true` because the row matched the WHERE clause.
pub async fn update_tablet(
    pool: &AnyPool,
    session_id: &str,
    new_tablet_id: i64,
) -> MetaResult<bool> {
    let rows = sqlx::query("UPDATE session_locators SET tablet_id = ? WHERE session_id = ?")
        .bind(new_tablet_id)
        .bind(session_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

/// Drop a locator row. Used when a session is permanently
/// removed (e.g., admin delete). Returns `true` when a row
/// existed prior; `false` when the call was a no-op.
pub async fn delete(pool: &AnyPool, session_id: &str) -> MetaResult<bool> {
    let rows = sqlx::query("DELETE FROM session_locators WHERE session_id = ?")
        .bind(session_id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

/// Cursor-style listing keyed on the **external** tenant id.
/// Auth-bound and cross-cluster stable; the variant the wire
/// find handler uses for multi-tablet fanout (M26 Phase 3).
///
/// Older locator rows that predate migration 012 have a NULL
/// `tenant_external_id` and are deliberately skipped here —
/// the caller should fall back to box-local enumeration when
/// it sees an empty result on a known-non-empty tenant.
pub async fn list_by_tenant_external(
    pool: &AnyPool,
    tenant_external_id: &str,
    after_session_id: Option<&str>,
    limit: i64,
) -> MetaResult<Vec<SessionLocator>> {
    let limit = limit.max(1);
    let rows: Vec<LocatorRow> = match after_session_id {
        Some(after) => sqlx::query_as(
            "SELECT session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id \
                FROM session_locators \
                WHERE tenant_external_id = ? AND session_id > ? \
                ORDER BY session_id ASC \
                LIMIT ?",
        )
        .bind(tenant_external_id)
        .bind(after)
        .bind(limit)
        .fetch_all(pool)
        .await?,
        None => sqlx::query_as(
            "SELECT session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id \
                FROM session_locators \
                WHERE tenant_external_id = ? \
                ORDER BY session_id ASC \
                LIMIT ?",
        )
        .bind(tenant_external_id)
        .bind(limit)
        .fetch_all(pool)
        .await?,
    };
    Ok(rows.into_iter().map(into_locator).collect())
}

/// Fetch every session locator, ordered by session id. Used by routing snapshot
/// refreshes so the HTTP data path can evaluate placement without per-request
/// metadata reads.
pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<SessionLocator>> {
    let rows: Vec<LocatorRow> = sqlx::query_as(
        "SELECT session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id \
            FROM session_locators \
            ORDER BY session_id ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_locator).collect())
}

/// Cursor-style listing: every locator owned by `tenant`,
/// ordered by `session_id`, with optional resumption after
/// `after_session_id` (exclusive). Caps at `limit` rows.
///
/// **Note**: this variant uses `tenant_internal_id`, which is
/// the per-process registry id and is NOT cross-cluster
/// stable. Prefer [`list_by_tenant_external`] in production
/// code paths; this remains for diagnostics.
pub async fn list_by_tenant(
    pool: &AnyPool,
    tenant_internal_id: i64,
    after_session_id: Option<&str>,
    limit: i64,
) -> MetaResult<Vec<SessionLocator>> {
    let limit = limit.max(1);
    let rows: Vec<LocatorRow> = match after_session_id {
        Some(after) => sqlx::query_as(
            "SELECT session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id \
                FROM session_locators \
                WHERE tenant_internal_id = ? AND session_id > ? \
                ORDER BY session_id ASC \
                LIMIT ?",
        )
        .bind(tenant_internal_id)
        .bind(after)
        .bind(limit)
        .fetch_all(pool)
        .await?,
        None => sqlx::query_as(
            "SELECT session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id \
                FROM session_locators \
                WHERE tenant_internal_id = ? \
                ORDER BY session_id ASC \
                LIMIT ?",
        )
        .bind(tenant_internal_id)
        .bind(limit)
        .fetch_all(pool)
        .await?,
    };
    Ok(rows.into_iter().map(into_locator).collect())
}

fn into_locator(row: LocatorRow) -> SessionLocator {
    let (session_id, tenant_internal_id, tablet_id, shard_id, created_at_ms, tenant_external_id) =
        row;
    SessionLocator {
        session_id,
        tenant_internal_id,
        tenant_external_id,
        tablet_id,
        shard_id,
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
    async fn upsert_and_get_round_trips() {
        let p = pool().await;
        assert!(upsert(&p, "s-1", 7, "tenant-7", Some(3), 42, 1_000)
            .await
            .unwrap());
        let l = get(&p, "s-1").await.unwrap().unwrap();
        assert_eq!(l.session_id, "s-1");
        assert_eq!(l.tenant_internal_id, 7);
        assert_eq!(l.tenant_external_id.as_deref(), Some("tenant-7"));
        assert_eq!(l.tablet_id, Some(3));
        assert_eq!(l.shard_id, 42);
        assert_eq!(l.created_at_ms, 1_000);
    }

    #[tokio::test]
    async fn upsert_is_idempotent_and_does_not_overwrite_tablet_id() {
        let p = pool().await;
        upsert(&p, "s-1", 7, "tenant-7", Some(3), 42, 1_000)
            .await
            .unwrap();
        // Simulate a handoff completion having moved this
        // session: tablet 3 → tablet 9 via update_tablet.
        update_tablet(&p, "s-1", 9).await.unwrap();
        // A subsequent populate-on-write must NOT clobber the
        // updated tablet_id. The old code's "first call wins"
        // semantics is what keeps the locator authoritative.
        let inserted = upsert(&p, "s-1", 7, "tenant-7", Some(3), 42, 2_000)
            .await
            .unwrap();
        assert!(
            !inserted,
            "second upsert returns false — row already present"
        );
        let l = get(&p, "s-1").await.unwrap().unwrap();
        assert_eq!(l.tablet_id, Some(9), "update_tablet wins over re-upsert");
        assert_eq!(l.created_at_ms, 1_000, "original timestamp preserved");
    }

    #[tokio::test]
    async fn upsert_with_null_tablet_works() {
        // Created-before-placed case: a session whose
        // hosting tablet isn't yet known. Must round-trip
        // None and not violate any constraint.
        let p = pool().await;
        upsert(&p, "s-pending", 1, "tenant-1", None, 0, 100)
            .await
            .unwrap();
        let l = get(&p, "s-pending").await.unwrap().unwrap();
        assert!(l.tablet_id.is_none());
    }

    #[tokio::test]
    async fn update_tablet_reports_whether_row_existed() {
        let p = pool().await;
        // Unknown session id → false.
        assert!(!update_tablet(&p, "s-ghost", 9).await.unwrap());
        // Known session → true.
        upsert(&p, "s-1", 7, "tenant-7", Some(3), 42, 1_000)
            .await
            .unwrap();
        assert!(update_tablet(&p, "s-1", 9).await.unwrap());
        let l = get(&p, "s-1").await.unwrap().unwrap();
        assert_eq!(l.tablet_id, Some(9));
    }

    #[tokio::test]
    async fn delete_reports_whether_row_existed() {
        let p = pool().await;
        assert!(!delete(&p, "s-ghost").await.unwrap());
        upsert(&p, "s-1", 7, "tenant-7", Some(3), 42, 1_000)
            .await
            .unwrap();
        assert!(delete(&p, "s-1").await.unwrap());
        assert!(get(&p, "s-1").await.unwrap().is_none());
        // Idempotent second delete.
        assert!(!delete(&p, "s-1").await.unwrap());
    }

    #[tokio::test]
    async fn list_by_tenant_returns_locators_ordered_by_id() {
        let p = pool().await;
        upsert(&p, "s-c", 1, "t-1", Some(0), 0, 0).await.unwrap();
        upsert(&p, "s-a", 1, "t-1", Some(0), 0, 0).await.unwrap();
        upsert(&p, "s-b", 1, "t-1", Some(0), 0, 0).await.unwrap();
        // Different tenant — must NOT show up.
        upsert(&p, "s-other", 2, "t-2", Some(0), 0, 0)
            .await
            .unwrap();

        let mine = list_by_tenant(&p, 1, None, 100).await.unwrap();
        let ids: Vec<_> = mine.iter().map(|l| l.session_id.as_str()).collect();
        assert_eq!(ids, ["s-a", "s-b", "s-c"]);
    }

    #[tokio::test]
    async fn list_by_tenant_external_filters_to_one_tenant() {
        let p = pool().await;
        upsert(&p, "s-c", 1, "t-1", Some(0), 0, 0).await.unwrap();
        upsert(&p, "s-a", 1, "t-1", Some(0), 0, 0).await.unwrap();
        upsert(&p, "s-b", 1, "t-1", Some(0), 0, 0).await.unwrap();
        upsert(&p, "s-other", 2, "t-2", Some(0), 0, 0)
            .await
            .unwrap();
        let mine = list_by_tenant_external(&p, "t-1", None, 100).await.unwrap();
        let ids: Vec<_> = mine.iter().map(|l| l.session_id.as_str()).collect();
        assert_eq!(ids, ["s-a", "s-b", "s-c"]);
    }

    #[tokio::test]
    async fn list_all_returns_every_locator_ordered_by_session_id() {
        let p = pool().await;
        upsert(&p, "s-c", 1, "t-1", Some(3), 3, 0).await.unwrap();
        upsert(&p, "s-a", 2, "t-2", Some(1), 1, 0).await.unwrap();
        upsert(&p, "s-b", 1, "t-1", Some(2), 2, 0).await.unwrap();

        let all = list_all(&p).await.unwrap();

        let ids: Vec<_> = all.iter().map(|l| l.session_id.as_str()).collect();
        assert_eq!(ids, ["s-a", "s-b", "s-c"]);
        assert_eq!(all[0].tenant_external_id.as_deref(), Some("t-2"));
        assert_eq!(all[1].tablet_id, Some(2));
    }

    #[tokio::test]
    async fn list_by_tenant_external_paginates_via_after_cursor() {
        let p = pool().await;
        for id in ["s-a", "s-b", "s-c", "s-d"] {
            upsert(&p, id, 1, "t-1", Some(0), 0, 0).await.unwrap();
        }
        let page1 = list_by_tenant_external(&p, "t-1", None, 2).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].session_id, "s-a");
        let page2 = list_by_tenant_external(&p, "t-1", Some("s-b"), 2)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].session_id, "s-c");
    }

    #[tokio::test]
    async fn list_by_tenant_paginates_via_after_cursor() {
        let p = pool().await;
        for id in ["s-a", "s-b", "s-c", "s-d"] {
            upsert(&p, id, 1, "t-1", Some(0), 0, 0).await.unwrap();
        }
        let page1 = list_by_tenant(&p, 1, None, 2).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].session_id, "s-a");
        assert_eq!(page1[1].session_id, "s-b");
        let page2 = list_by_tenant(&p, 1, Some("s-b"), 2).await.unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].session_id, "s-c");
        assert_eq!(page2[1].session_id, "s-d");
        // Final page yields no rows.
        let page3 = list_by_tenant(&p, 1, Some("s-d"), 2).await.unwrap();
        assert!(page3.is_empty());
    }
}
