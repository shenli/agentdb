//! `GET /api/admin/scheduler` — who holds the scheduler-leader role
//! right now, and is *this* process the leader?
//!
//! Two sources of truth combine:
//! - The local `LeaderElection` (background election loop in
//!   `agentdb_core::scheduler`) — gives `this_node_is_leader` from
//!   an atomic load, no DB round-trip.
//! - The meta-store `scheduler_leader` row — gives the canonical
//!   leader view including *other* nodes. Read on every request so
//!   the answer reflects current state, not cached.
//!
//! Both are optional extensions — if either isn't layered on the
//! router, the endpoint gracefully 503s rather than panicking. Tests
//! that build a router without leader-election configured (the
//! common case pre-M22-proper) hit the 503 path.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::{LeaderElection, NodeReaper, StaleTabletReaper, TabletPlacement};
use agentdb_meta::{scheduler as meta_scheduler, MetaStore};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LeaderView {
    pub node_id: String,
    pub epoch: i64,
    pub expires_at_ms: i64,
    pub acquired_at_ms: i64,
    /// True when `expires_at_ms > now_ms` at the moment of the read.
    /// A `false` here on a long-quiet cluster indicates either the
    /// last leader died without a successor acquiring, or the
    /// `lease_duration` is large compared to read cadence.
    pub is_live: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SchedulerStatus {
    /// Node id this process registered as.
    pub this_node_id: String,
    /// `true` if this process's `LeaderElection::is_leader()` says
    /// so. Derived from the last successful acquire/renew — may be
    /// up to `renew_interval` stale after a loss of leadership, but
    /// never falsely `true` past `lease_duration`.
    pub this_node_is_leader: bool,
    /// Current epoch observed by this process's election loop. Zero
    /// before the first acquire attempt completes.
    pub this_node_epoch: i64,
    /// Canonical leader row from the control plane. `None` means no
    /// leader has ever been elected on this cluster.
    pub leader: Option<LeaderView>,
    /// Total number of tablet leases this process has cleared as
    /// part of the stale-lease reaper sweep. Only advances while
    /// this process is the leader — followers report 0 even if
    /// another node in the cluster has reaped many leases.
    pub reassignments_total: u64,
    /// Total `alive -> dead` node transitions this process has
    /// performed as leader. Same leader-gated semantics as
    /// `reassignments_total`.
    pub nodes_marked_dead_total: u64,
    /// Total long-dead node rows this process has GC'd from the
    /// meta table as leader.
    pub nodes_gc_removed_total: u64,
    /// Total tablet placements made by this process while
    /// leader (`owner_node_id` NULL → some alive node). Same
    /// leader-gated semantics; followers report 0.
    pub tablet_placements_total: u64,
    /// Total tablets relinquished by this process during the
    /// active-rebalance pass. Each relinquish causes a brief
    /// data-path disruption on the source node, so this
    /// counter doubles as an SLO signal: a leader inflicting
    /// many forced-evictions per minute means the cluster has
    /// chronic node churn.
    pub tablet_rebalances_total: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SchedulerUnavailable {
    pub message: String,
}

#[utoipa::path(
    get,
    path = "/api/admin/scheduler",
    tag = "admin",
    responses(
        (status = 200, description = "Scheduler leader-election status", body = SchedulerStatus),
        (status = 503, description = "Control plane or election loop not configured", body = SchedulerUnavailable),
    ),
)]
pub async fn get_scheduler(
    meta: Option<Extension<Arc<MetaStore>>>,
    election: Option<Extension<Arc<LeaderElection>>>,
    reaper: Option<Extension<Arc<StaleTabletReaper>>>,
    node_reaper: Option<Extension<Arc<NodeReaper>>>,
    placement: Option<Extension<Arc<TabletPlacement>>>,
) -> Result<Json<SchedulerStatus>, (StatusCode, Json<SchedulerUnavailable>)> {
    let Some(Extension(meta)) = meta else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SchedulerUnavailable {
                message: "meta store is not configured on this server".into(),
            }),
        ));
    };
    let Some(Extension(election)) = election else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SchedulerUnavailable {
                message: "leader election loop is not running on this server".into(),
            }),
        ));
    };
    let leader_row = match meta_scheduler::get(meta.pool()).await {
        Ok(row) => row,
        Err(e) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(SchedulerUnavailable {
                    message: format!("meta query failed: {e}"),
                }),
            ));
        }
    };
    let now = now_ms();
    let leader = leader_row.map(|r| LeaderView {
        is_live: r.is_live(now),
        node_id: r.node_id,
        epoch: r.epoch,
        expires_at_ms: r.expires_at_ms,
        acquired_at_ms: r.acquired_at_ms,
    });
    // Reaper is optional — tests that build a router without one
    // report 0 rather than 503. Election is the core concept; the
    // reaper is a per-leader detail.
    let reassignments_total = reaper
        .as_ref()
        .map(|Extension(r)| r.reassignments_total())
        .unwrap_or(0);
    let (nodes_marked_dead_total, nodes_gc_removed_total) = node_reaper
        .as_ref()
        .map(|Extension(r)| (r.marked_dead_total(), r.gc_removed_total()))
        .unwrap_or((0, 0));
    let (tablet_placements_total, tablet_rebalances_total) = placement
        .as_ref()
        .map(|Extension(p)| (p.assignments_total(), p.rebalances_total()))
        .unwrap_or((0, 0));
    Ok(Json(SchedulerStatus {
        this_node_id: election.node_id().to_string(),
        this_node_is_leader: election.is_leader(),
        this_node_epoch: election.current_epoch(),
        leader,
        reassignments_total,
        nodes_marked_dead_total,
        nodes_gc_removed_total,
        tablet_placements_total,
        tablet_rebalances_total,
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
    use agentdb_core::{LeaderElection, LeaderElectionConfig, ReaperConfig, StaleTabletReaper};
    use std::time::Duration;

    #[tokio::test]
    async fn returns_503_when_meta_extension_is_absent() {
        let (code, body) = get_scheduler(None, None, None, None, None)
            .await
            .expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("meta store"));
    }

    #[tokio::test]
    async fn returns_503_when_election_extension_is_absent() {
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        let (code, body) = get_scheduler(Some(Extension(meta)), None, None, None, None)
            .await
            .expect_err("503");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.0.message.contains("leader election"));
    }

    #[tokio::test]
    async fn reports_this_node_as_leader_after_election_converges() {
        // Use a shared file-backed SQLite so the election loop's
        // writes are visible to the meta-read path.
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&path).await.unwrap());
        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let election = LeaderElection::start(meta.clone(), "test-node".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader(), "should have won election by now");

        let Json(status) = get_scheduler(
            Some(Extension(meta.clone())),
            Some(Extension(election.clone())),
            None, // no reaper in this test — counter reports 0
            None, // no node_reaper either
            None, // no placement either
        )
        .await
        .expect("200");

        assert_eq!(status.this_node_id, "test-node");
        assert!(status.this_node_is_leader);
        assert!(status.this_node_epoch >= 1);
        assert_eq!(
            status.reassignments_total, 0,
            "no reaper wired → counter is zero"
        );
        assert_eq!(status.nodes_marked_dead_total, 0);
        assert_eq!(status.nodes_gc_removed_total, 0);
        assert_eq!(status.tablet_placements_total, 0);
        assert_eq!(status.tablet_rebalances_total, 0);
        let leader = status.leader.expect("leader row present");
        assert_eq!(leader.node_id, "test-node");
        assert!(leader.is_live);
    }

    #[tokio::test]
    async fn reassignments_total_reflects_reaper_counter_when_present() {
        // Seed meta with a stale tablet so the reaper immediately
        // has work to do on its first on-leader tick.
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&path).await.unwrap());
        agentdb_meta::tablets::get_or_create(meta.pool(), 42, 0)
            .await
            .unwrap();
        agentdb_meta::tablets::assume_ownership(meta.pool(), 42, 0, "dead", 1)
            .await
            .unwrap();

        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let election = LeaderElection::start(meta.clone(), "test-node".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let reaper = StaleTabletReaper::start(
            meta.clone(),
            election.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;

        let Json(status) = get_scheduler(
            Some(Extension(meta.clone())),
            Some(Extension(election.clone())),
            Some(Extension(reaper.clone())),
            None,
            None,
        )
        .await
        .expect("200");
        assert!(
            status.reassignments_total >= 1,
            "reaper counter should reflect the cleared-stale row"
        );
    }

    #[tokio::test]
    async fn node_reaper_counters_reflect_marked_and_gc_totals() {
        // Seed a stale node + a long-dead-already row so the node
        // reaper has one of each on its first on-leader tick.
        use agentdb_core::{NodeReaper, NodeReaperConfig};
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&path).await.unwrap());
        // "stale" is alive but hb is decades-stale (real-time "now"
        // is definitely > 1000 + 1ms dead_after so it'll flip).
        agentdb_meta::nodes::upsert(meta.pool(), "stale", "a", None, "alive", 1_000)
            .await
            .unwrap();
        // Pre-flip "ancient" so GC has a candidate without a wait.
        agentdb_meta::nodes::upsert(meta.pool(), "ancient", "b", None, "alive", 0)
            .await
            .unwrap();
        agentdb_meta::nodes::mark_dead_if_stale(meta.pool(), 50)
            .await
            .unwrap();

        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let election = LeaderElection::start(meta.clone(), "solo".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader());

        let node_reaper = NodeReaper::start(
            meta.clone(),
            election.clone(),
            NodeReaperConfig {
                tick: Duration::from_millis(100),
                dead_after: Duration::from_millis(1),
                // Aggressive GC — "ancient" (hb=0) is millennia
                // old on the wall clock, so it qualifies.
                gc_after: Duration::from_millis(1),
            },
        );
        tokio::time::sleep(Duration::from_millis(500)).await;

        let Json(status) = get_scheduler(
            Some(Extension(meta)),
            Some(Extension(election)),
            None,
            Some(Extension(node_reaper)),
            None,
        )
        .await
        .expect("200");
        assert!(
            status.nodes_marked_dead_total >= 1,
            "stale row should have been flipped to dead at least once, got {}",
            status.nodes_marked_dead_total
        );
        assert!(
            status.nodes_gc_removed_total >= 1,
            "pre-dead row should have been GC'd at least once, got {}",
            status.nodes_gc_removed_total
        );
    }
}
