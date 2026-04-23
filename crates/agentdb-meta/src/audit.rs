//! Admin-plane audit log.
//!
//! One row per request that reaches the admin-auth middleware,
//! including 401s. See migration 005 for the schema and why
//! the column set is this one.
//!
//! This module stays narrowly focused: insert a row and list
//! rows with a filter. The middleware is the only writer; the
//! `/api/admin/audit` handler is the only reader. No
//! retention / compaction — documented as future work.

use sqlx::sqlite::SqlitePool;

use crate::error::MetaResult;

/// Everything we persist per admin request. Field set matches
/// the `admin_audit` schema one-for-one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditRecord {
    pub id: i64,
    pub ts_ms: i64,
    /// `None` when the request was unauthenticated (dev mode,
    /// bootstrap mint, or 401). Operators distinguish bootstrap
    /// from failed-auth by the paired `status`.
    pub actor_token_id: Option<String>,
    pub method: String,
    pub path: String,
    /// URL query string sans leading `?`. `None` when absent.
    pub query: Option<String>,
    pub status: u16,
    pub client_ip: Option<String>,
}

/// Filter for [`list`]. All fields are optional and compose
/// with AND semantics.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// Inclusive lower bound on `ts_ms`.
    pub since_ms: Option<i64>,
    /// Exclusive upper bound on `ts_ms`. `None` means "now".
    pub until_ms: Option<i64>,
    /// Exact match on the actor token id.
    pub actor_token_id: Option<String>,
    /// Prefix filter on `path`. Matches a literal string against
    /// the start of the path.
    pub path_prefix: Option<String>,
    /// Cap the number of returned rows. Caller should set this;
    /// `None` falls back to [`DEFAULT_LIMIT`].
    pub limit: Option<i64>,
}

/// Default `limit` when [`AuditFilter::limit`] is `None`.
/// Keeps accidental full-scans out of the hot path.
pub const DEFAULT_LIMIT: i64 = 200;

/// Maximum `limit` we honor regardless of caller input. Past
/// this, clients should paginate via `until_ms`.
pub const MAX_LIMIT: i64 = 10_000;

/// Insert one audit row. Returns the allocated `id`.
///
/// Takes owned strings rather than `&str` to keep the call-site
/// ergonomic from the axum middleware where the input is mostly
/// borrowed from the request — the middleware clones anyway
/// because it needs to move values across the `next.run(req)`
/// await point.
#[allow(clippy::too_many_arguments)]
pub async fn insert(
    pool: &SqlitePool,
    ts_ms: i64,
    actor_token_id: Option<String>,
    method: String,
    path: String,
    query: Option<String>,
    status: u16,
    client_ip: Option<String>,
) -> MetaResult<i64> {
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO admin_audit
             (ts_ms, actor_token_id, method, path, query, status, client_ip)
           VALUES (?, ?, ?, ?, ?, ?, ?)
         RETURNING id",
    )
    .bind(ts_ms)
    .bind(actor_token_id)
    .bind(method)
    .bind(path)
    .bind(query)
    .bind(status as i64)
    .bind(client_ip)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Newest rows first, bounded by [`AuditFilter::limit`] (or
/// [`DEFAULT_LIMIT`]). Clients paginate by passing the oldest
/// returned `ts_ms` as the next `until_ms`.
pub async fn list(pool: &SqlitePool, filter: &AuditFilter) -> MetaResult<Vec<AuditRecord>> {
    let limit = filter
        .limit
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT);

    // Build the SQL with bound parameters. Keeping the WHERE
    // clause dynamic avoids materializing a CTE for absent
    // filters.
    let mut sql = String::from(
        "SELECT id, ts_ms, actor_token_id, method, path, query, status, client_ip
           FROM admin_audit
          WHERE 1=1",
    );
    if filter.since_ms.is_some() {
        sql.push_str(" AND ts_ms >= ?");
    }
    if filter.until_ms.is_some() {
        sql.push_str(" AND ts_ms < ?");
    }
    if filter.actor_token_id.is_some() {
        sql.push_str(" AND actor_token_id = ?");
    }
    if filter.path_prefix.is_some() {
        // SQLite `LIKE` with `ESCAPE '\'` so a prefix
        // containing `%` or `_` doesn't accidentally widen
        // the match.
        sql.push_str(r" AND path LIKE ? ESCAPE '\'");
    }
    sql.push_str(" ORDER BY id DESC LIMIT ?");

    let mut q = sqlx::query_as::<_, AuditRow>(&sql);
    if let Some(since) = filter.since_ms {
        q = q.bind(since);
    }
    if let Some(until) = filter.until_ms {
        q = q.bind(until);
    }
    if let Some(actor) = &filter.actor_token_id {
        q = q.bind(actor.clone());
    }
    if let Some(prefix) = &filter.path_prefix {
        q = q.bind(escape_like_prefix(prefix));
    }
    q = q.bind(limit);

    let rows = q.fetch_all(pool).await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// Escape `\`, `%`, and `_` so a literal prefix string does not
/// get interpreted as a LIKE pattern. Appends `%` to convert
/// the escaped prefix into a LIKE-compatible pattern.
fn escape_like_prefix(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 1);
    for c in s.chars() {
        match c {
            '\\' | '%' | '_' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out.push('%');
    out
}

/// Row shape used for `sqlx::query_as`. We translate into
/// `AuditRecord` at the crate boundary so the public type
/// owns its `u16` status.
#[derive(sqlx::FromRow)]
struct AuditRow {
    id: i64,
    ts_ms: i64,
    actor_token_id: Option<String>,
    method: String,
    path: String,
    query: Option<String>,
    status: i64,
    client_ip: Option<String>,
}

impl From<AuditRow> for AuditRecord {
    fn from(r: AuditRow) -> Self {
        Self {
            id: r.id,
            ts_ms: r.ts_ms,
            actor_token_id: r.actor_token_id,
            method: r.method,
            path: r.path,
            query: r.query,
            status: r.status as u16,
            client_ip: r.client_ip,
        }
    }
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

    async fn seed(p: &SqlitePool) {
        insert(
            p,
            1000,
            Some("tok_alice".into()),
            "POST".into(),
            "/api/admin/users".into(),
            None,
            200,
            Some("10.0.0.1".into()),
        )
        .await
        .unwrap();
        insert(
            p,
            2000,
            Some("tok_bob".into()),
            "GET".into(),
            "/api/admin/tablets".into(),
            Some("hot=true".into()),
            200,
            None,
        )
        .await
        .unwrap();
        insert(
            p,
            3000,
            None,
            "POST".into(),
            "/api/admin/tokens".into(),
            None,
            401,
            None,
        )
        .await
        .unwrap();
        insert(
            p,
            4000,
            Some("tok_alice".into()),
            "POST".into(),
            "/api/admin/tablets/42/shed".into(),
            None,
            200,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn insert_allocates_monotonic_ids() {
        let p = pool().await;
        let a = insert(
            &p, 10, None, "GET".into(), "/api/admin/x".into(), None, 200, None,
        )
        .await
        .unwrap();
        let b = insert(
            &p, 20, None, "GET".into(), "/api/admin/y".into(), None, 200, None,
        )
        .await
        .unwrap();
        assert!(b > a, "expected monotonic ids, got {a} then {b}");
    }

    #[tokio::test]
    async fn list_returns_newest_first() {
        let p = pool().await;
        seed(&p).await;
        let rows = list(&p, &AuditFilter::default()).await.unwrap();
        let ts: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
        assert_eq!(ts, vec![4000, 3000, 2000, 1000]);
    }

    #[tokio::test]
    async fn filter_by_since_ms() {
        let p = pool().await;
        seed(&p).await;
        let rows = list(
            &p,
            &AuditFilter {
                since_ms: Some(2500),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ts: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
        assert_eq!(ts, vec![4000, 3000]);
    }

    #[tokio::test]
    async fn filter_by_until_ms_is_exclusive() {
        let p = pool().await;
        seed(&p).await;
        let rows = list(
            &p,
            &AuditFilter {
                until_ms: Some(3000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ts: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
        assert_eq!(ts, vec![2000, 1000]);
    }

    #[tokio::test]
    async fn filter_by_actor() {
        let p = pool().await;
        seed(&p).await;
        let rows = list(
            &p,
            &AuditFilter {
                actor_token_id: Some("tok_alice".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["/api/admin/tablets/42/shed", "/api/admin/users"]
        );
    }

    #[tokio::test]
    async fn filter_by_path_prefix_is_literal() {
        let p = pool().await;
        // Insert a row whose path contains a % so the LIKE
        // escape path is exercised.
        insert(
            &p,
            5000,
            None,
            "GET".into(),
            "/api/admin/x%y".into(),
            None,
            200,
            None,
        )
        .await
        .unwrap();
        insert(
            &p,
            6000,
            None,
            "GET".into(),
            "/api/admin/xay".into(),
            None,
            200,
            None,
        )
        .await
        .unwrap();
        let rows = list(
            &p,
            &AuditFilter {
                path_prefix: Some("/api/admin/x%".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["/api/admin/x%y"],
            "LIKE wildcard must not match a literal `%`"
        );
    }

    #[tokio::test]
    async fn limit_is_clamped_and_defaulted() {
        let p = pool().await;
        seed(&p).await;
        // limit 0 → clamped to 1
        let rows = list(
            &p,
            &AuditFilter {
                limit: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        // limit > MAX_LIMIT → clamped to MAX_LIMIT (and our 4
        // rows all come back)
        let rows = list(
            &p,
            &AuditFilter {
                limit: Some(MAX_LIMIT + 100),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 4);
    }

    #[tokio::test]
    async fn unauthenticated_attempts_are_recorded() {
        let p = pool().await;
        seed(&p).await;
        let rows = list(
            &p,
            &AuditFilter {
                actor_token_id: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            rows.iter().any(|r| r.actor_token_id.is_none() && r.status == 401),
            "401s should be visible in an unfiltered listing"
        );
    }
}
