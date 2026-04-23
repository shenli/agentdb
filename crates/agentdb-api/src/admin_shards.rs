//! `GET /api/admin/shards/:id` — resolve a single shard id to its
//! current tablet assignment. Also supports `GET /api/admin/shards?
//! session_id=...` so operators (and the `agentdb show-shard` CLI)
//! can ask "where would session X route to?" without computing the
//! shard hash client-side.
//!
//! Reads route through the process-local [`LocationProxy`] so the
//! answer matches what the write path would see; a control-plane
//! override applied a moment ago won't be visible until the proxy's
//! next refresh.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::{shard as core_shard, LocationProxy};
use agentdb_meta::{shards as meta_shards, MetaStore};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ShardAssignment {
    /// Shard id the request resolved to.
    pub shard_id: u16,
    /// If the caller provided `?session_id=...`, echo it back so
    /// scripts and the CLI can correlate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Tablet currently assigned to this shard. `null` means "no
    /// override — routing falls back to the default (hash % num
    /// boxes) rule the client should already know about".
    pub tablet_id: Option<i64>,
    /// Wall-clock ms of the last successful refresh from meta; if
    /// the shown `tablet_id` is stale, it's never more stale than
    /// `refresh_interval` seconds.
    pub last_refresh_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ShardsUnavailable {
    pub message: String,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ShardByIdQuery {
    /// If set, the response echoes this back under `session_id`.
    /// (The path already carries `:id`, so this is only for scripted
    /// correlation.)
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ShardBySessionQuery {
    /// External session id. Required.
    pub session_id: String,
}

#[utoipa::path(
    get,
    path = "/api/admin/shards/{id}",
    tag = "admin",
    params(
        ("id" = u16, Path, description = "Shard id (0..16384)"),
        ShardByIdQuery
    ),
    responses(
        (status = 200, description = "Current assignment", body = ShardAssignment),
        (status = 400, description = "id out of range", body = ShardsUnavailable),
        (status = 503, description = "LocationProxy not configured", body = ShardsUnavailable),
    ),
)]
pub async fn get_shard_by_id(
    proxy: Option<Extension<Arc<LocationProxy>>>,
    Path(id): Path<u16>,
    Query(q): Query<ShardByIdQuery>,
) -> Result<Json<ShardAssignment>, (StatusCode, Json<ShardsUnavailable>)> {
    let Some(Extension(proxy)) = proxy else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ShardsUnavailable {
                message: "LocationProxy is not running on this server".into(),
            }),
        ));
    };
    if (id as u32) >= core_shard::SHARD_COUNT {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ShardsUnavailable {
                message: format!(
                    "shard id {id} is out of range; valid ids are 0..{}",
                    core_shard::SHARD_COUNT
                ),
            }),
        ));
    }
    Ok(Json(ShardAssignment {
        shard_id: id,
        session_id: q.session_id,
        tablet_id: proxy.tablet_for_shard(id),
        last_refresh_ms: proxy.last_refresh_ms(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/admin/shards",
    tag = "admin",
    params(ShardBySessionQuery),
    responses(
        (status = 200, description = "Shard + assignment for the given session id", body = ShardAssignment),
        (status = 503, description = "LocationProxy not configured", body = ShardsUnavailable),
    ),
)]
pub async fn get_shard_by_session(
    proxy: Option<Extension<Arc<LocationProxy>>>,
    Query(q): Query<ShardBySessionQuery>,
) -> Result<Json<ShardAssignment>, (StatusCode, Json<ShardsUnavailable>)> {
    let Some(Extension(proxy)) = proxy else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ShardsUnavailable {
                message: "LocationProxy is not running on this server".into(),
            }),
        ));
    };
    let shard = core_shard::shard_id(&q.session_id);
    Ok(Json(ShardAssignment {
        shard_id: shard,
        session_id: Some(q.session_id),
        tablet_id: proxy.tablet_for_shard(shard),
        last_refresh_ms: proxy.last_refresh_ms(),
    }))
}

/// Body for `POST /api/admin/shards/:id`. `tablet_id: null` reverts
/// the shard to default (hash) routing; a set value pins it.
#[derive(Debug, Deserialize, ToSchema)]
pub struct SetShardRequest {
    pub tablet_id: Option<i64>,
}

#[utoipa::path(
    post,
    path = "/api/admin/shards/{id}",
    tag = "admin",
    params(("id" = u16, Path, description = "Shard id (0..16384)")),
    request_body = SetShardRequest,
    responses(
        (status = 200, description = "Assignment set; post-update proxy view", body = ShardAssignment),
        (status = 400, description = "id out of range", body = ShardsUnavailable),
        (status = 503, description = "meta / LocationProxy not configured", body = ShardsUnavailable),
    ),
)]
pub async fn set_shard(
    meta: Option<Extension<Arc<MetaStore>>>,
    proxy: Option<Extension<Arc<LocationProxy>>>,
    Path(id): Path<u16>,
    Json(body): Json<SetShardRequest>,
) -> Result<Json<ShardAssignment>, (StatusCode, Json<ShardsUnavailable>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ShardsUnavailable {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    let Some(Extension(proxy)) = proxy else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ShardsUnavailable {
                message: "LocationProxy is not running on this server".into(),
            }),
        ));
    };
    if (id as u32) >= core_shard::SHARD_COUNT {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ShardsUnavailable {
                message: format!(
                    "shard id {id} is out of range; valid ids are 0..{}",
                    core_shard::SHARD_COUNT
                ),
            }),
        ));
    }
    let now = now_ms();
    meta_shards::set_assignment(meta.pool(), id as i64, body.tablet_id, now)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ShardsUnavailable {
                    message: format!("meta write failed: {e}"),
                }),
            )
        })?;
    // Refresh immediately so the returned snapshot reflects the
    // just-written value. Without this, the caller would see the
    // old cached row until the next periodic refresh — confusing
    // for an operator running `agentdb set-shard` and then
    // `agentdb show-shard`.
    if let Err(e) = proxy.refresh().await {
        // Non-fatal — the write landed in meta, the next periodic
        // refresh will pick it up. Log via WARN from here would
        // require tracing plumbing; just fall through and return
        // the old-cache view.
        tracing::warn!(%e, "post-set refresh failed; next periodic refresh will pick it up");
    }
    Ok(Json(ShardAssignment {
        shard_id: id,
        session_id: None,
        tablet_id: proxy.tablet_for_shard(id),
        last_refresh_ms: proxy.last_refresh_ms(),
    }))
}

/// Response for `GET /api/admin/shards/list`. Only shards with an
/// explicit assignment are returned — 16384 rows is too many for a
/// human-scale listing.
#[derive(Debug, Serialize, ToSchema)]
pub struct ShardOverridesList {
    pub overrides: Vec<ShardOverride>,
    pub total_count: usize,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ShardOverride {
    pub shard_id: i64,
    pub tablet_id: i64,
    pub updated_at_ms: i64,
}

#[utoipa::path(
    get,
    path = "/api/admin/shards/list",
    tag = "admin",
    responses(
        (status = 200, description = "Shards that have explicit tablet overrides", body = ShardOverridesList),
        (status = 503, description = "meta store not configured", body = ShardsUnavailable),
    ),
)]
pub async fn list_shard_overrides(
    meta: Option<Extension<Arc<MetaStore>>>,
) -> Result<Json<ShardOverridesList>, (StatusCode, Json<ShardsUnavailable>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ShardsUnavailable {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    let rows = meta_shards::list_overrides(meta.pool())
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ShardsUnavailable {
                    message: format!("meta query failed: {e}"),
                }),
            )
        })?;
    let overrides: Vec<ShardOverride> = rows
        .into_iter()
        .filter_map(|r| {
            r.tablet_id.map(|t| ShardOverride {
                shard_id: r.shard_id,
                tablet_id: t,
                updated_at_ms: r.updated_at_ms,
            })
        })
        .collect();
    let total_count = overrides.len();
    Ok(Json(ShardOverridesList {
        overrides,
        total_count,
    }))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_meta::{migrations, shards as meta_shards, MetaStore};

    async fn proxy_with(overrides: &[(u16, i64)]) -> Arc<LocationProxy> {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        migrations::apply(meta.pool()).await.unwrap();
        meta_shards::seed_shards(meta.pool(), core_shard::SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        for (shard, tablet) in overrides {
            meta_shards::set_assignment(meta.pool(), *shard as i64, Some(*tablet), 0)
                .await
                .unwrap();
        }
        LocationProxy::new(meta).await.unwrap()
    }

    #[tokio::test]
    async fn returns_503_when_proxy_extension_is_absent() {
        let (code, body) =
            get_shard_by_id(None, Path(0), Query(ShardByIdQuery { session_id: None }))
                .await
                .expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("LocationProxy"));
    }

    #[tokio::test]
    async fn returns_400_when_shard_id_is_out_of_range() {
        let proxy = proxy_with(&[]).await;
        let (code, body) = get_shard_by_id(
            Some(Extension(proxy)),
            Path(u16::MAX),
            Query(ShardByIdQuery { session_id: None }),
        )
        .await
        .expect_err("400");
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(body.0.message.contains("out of range"));
    }

    #[tokio::test]
    async fn get_shard_by_id_returns_none_when_no_override() {
        let proxy = proxy_with(&[]).await;
        let Json(out) = get_shard_by_id(
            Some(Extension(proxy)),
            Path(42),
            Query(ShardByIdQuery { session_id: None }),
        )
        .await
        .expect("200");
        assert_eq!(out.shard_id, 42);
        assert_eq!(out.tablet_id, None);
        assert!(out.last_refresh_ms > 0);
    }

    #[tokio::test]
    async fn get_shard_by_id_reflects_overrides_after_refresh() {
        let proxy = proxy_with(&[(100, 7), (200, 9)]).await;
        let Json(out) = get_shard_by_id(
            Some(Extension(proxy)),
            Path(100),
            Query(ShardByIdQuery { session_id: None }),
        )
        .await
        .expect("200");
        assert_eq!(out.tablet_id, Some(7));
    }

    #[tokio::test]
    async fn get_shard_by_session_hashes_and_echoes_back_the_id() {
        let expected_shard = core_shard::shard_id("session-probe");
        let proxy = proxy_with(&[(expected_shard, 55)]).await;
        let Json(out) = get_shard_by_session(
            Some(Extension(proxy)),
            Query(ShardBySessionQuery {
                session_id: "session-probe".to_string(),
            }),
        )
        .await
        .expect("200");
        assert_eq!(out.shard_id, expected_shard);
        assert_eq!(out.session_id.as_deref(), Some("session-probe"));
        assert_eq!(out.tablet_id, Some(55));
    }

    async fn meta_and_proxy() -> (Arc<MetaStore>, Arc<LocationProxy>) {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        migrations::apply(meta.pool()).await.unwrap();
        meta_shards::seed_shards(meta.pool(), core_shard::SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        let proxy = LocationProxy::new(meta.clone()).await.unwrap();
        (meta, proxy)
    }

    #[tokio::test]
    async fn set_shard_writes_and_refreshes_proxy_so_lookup_sees_new_value() {
        let (meta, proxy) = meta_and_proxy().await;
        assert_eq!(proxy.tablet_for_shard(11), None);
        let Json(out) = set_shard(
            Some(Extension(meta)),
            Some(Extension(proxy.clone())),
            Path(11),
            Json(SetShardRequest {
                tablet_id: Some(77),
            }),
        )
        .await
        .expect("200");
        assert_eq!(out.shard_id, 11);
        assert_eq!(out.tablet_id, Some(77));
        // Post-call, the proxy reflects the write without needing
        // a periodic-refresh wait.
        assert_eq!(proxy.tablet_for_shard(11), Some(77));
    }

    #[tokio::test]
    async fn set_shard_with_null_reverts_to_default_routing() {
        let (meta, proxy) = meta_and_proxy().await;
        // Set then clear.
        let Json(_) = set_shard(
            Some(Extension(meta.clone())),
            Some(Extension(proxy.clone())),
            Path(7),
            Json(SetShardRequest { tablet_id: Some(9) }),
        )
        .await
        .expect("set ok");
        assert_eq!(proxy.tablet_for_shard(7), Some(9));
        let Json(out) = set_shard(
            Some(Extension(meta)),
            Some(Extension(proxy.clone())),
            Path(7),
            Json(SetShardRequest { tablet_id: None }),
        )
        .await
        .expect("clear ok");
        assert_eq!(out.tablet_id, None);
        assert_eq!(proxy.tablet_for_shard(7), None);
    }

    #[tokio::test]
    async fn set_shard_returns_400_on_out_of_range_id() {
        let (meta, proxy) = meta_and_proxy().await;
        let (code, _body) = set_shard(
            Some(Extension(meta)),
            Some(Extension(proxy)),
            Path(u16::MAX),
            Json(SetShardRequest { tablet_id: Some(1) }),
        )
        .await
        .expect_err("400");
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_overrides_returns_only_customized_shards() {
        let (meta, proxy) = meta_and_proxy().await;
        // Set three overrides among 16384 shards.
        for (s, t) in [(0u16, 10i64), (7146, 42), (16000, 99)] {
            let Json(_) = set_shard(
                Some(Extension(meta.clone())),
                Some(Extension(proxy.clone())),
                Path(s),
                Json(SetShardRequest { tablet_id: Some(t) }),
            )
            .await
            .expect("set ok");
        }
        let Json(out) = list_shard_overrides(Some(Extension(meta)))
            .await
            .expect("200");
        assert_eq!(out.total_count, 3);
        let ids: Vec<_> = out.overrides.iter().map(|o| o.shard_id).collect();
        assert_eq!(ids, vec![0i64, 7146, 16000]);
        let tablets: Vec<_> = out.overrides.iter().map(|o| o.tablet_id).collect();
        assert_eq!(tablets, vec![10i64, 42, 99]);
    }

    #[tokio::test]
    async fn list_overrides_is_empty_on_fresh_cluster() {
        let (meta, _proxy) = meta_and_proxy().await;
        let Json(out) = list_shard_overrides(Some(Extension(meta)))
            .await
            .expect("200");
        assert_eq!(out.total_count, 0);
        assert!(out.overrides.is_empty());
    }
}
