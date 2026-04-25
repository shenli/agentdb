//! Leader-owned tablet placement.
//!
//! Bridges three pre-existing pieces:
//!
//! 1. The `nodes` table tracks who's in the cluster
//!    (`agentdb_meta::nodes`, plus the `NodeReaper` keeping
//!    `state` honest).
//! 2. The `tablets` table tracks logical units (rows exist
//!    independently of nodes).
//! 3. `LeaseManager`/`Box_` wires a tablet to a process at
//!    runtime.
//!
//! The placement loop binds (1) and (2) together: it ensures
//! every tablet that exists has a candidate `owner_node_id`
//! pointing at an `alive` node. Acquiring the actual lease is
//! the per-node `TabletReconciler`'s job — placement only
//! "promises" the row to a node by writing `owner_node_id`
//! without bumping the epoch (see `tablets::assign_to`). The
//! reconciler on that node observes the assignment, calls
//! `LeaseManager::acquire`, and writes the lease epoch.
//!
//! Why split the work? Two reasons:
//!
//! 1. **Safety.** The lease-bump fence (epoch++ via
//!    `assume_ownership`) must be issued by the node that's
//!    actually going to write — having the leader bump epochs
//!    on behalf of others would race with renewals from the
//!    actual owner.
//! 2. **Liveness.** A leader announcing "node X owns tablet 7"
//!    is fine even if X is briefly slow to catch up; the row
//!    just sits with `lease_expires_at_ms = 0` until X's next
//!    reconcile tick. This is failure-mode-of-last-resort
//!    behaviour: peers correctly see the tablet as
//!    "promised but not yet acquired" and route locally
//!    via the LocationProxy fall-through.
//!
//! Design invariants:
//!
//! 1. **Bootstrap is idempotent and leader-only.** On first
//!    run with an empty `tablets` table the leader seeds
//!    `[0..bootstrap_tablets)` rows. Followers do nothing —
//!    the `is_leader()` check guards the entire loop body.
//!
//! 2. **Placement is conservative.** We only assign tablets
//!    whose `owner_node_id IS NULL`. We do **not** revoke an
//!    `owner_node_id` from a node that's drifting toward `dead`
//!    — that's the `StaleTabletReaper`'s job (it clears
//!    expired leases on `tablets`). Once the reaper has
//!    NULLed an owner, the next placement tick picks the row
//!    up.
//!
//! 3. **Round-robin across alive nodes.** No load-aware logic
//!    yet; just the hash of node-id ordering. Active
//!    rebalancing of healthy-owned tablets is explicit
//!    follow-up work.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdb_meta::{nodes as meta_nodes, tablets as meta_tablets, MetaStore};
use tokio::task::JoinHandle;

use crate::scheduler::LeaderElection;

/// Tunable timings.
#[derive(Debug, Clone, Copy)]
pub struct PlacementConfig {
    /// How often to sweep. Defaults to 5s. Fast enough that a
    /// failover is observable within ~tens of seconds in
    /// production (one reaper tick + one placement tick + one
    /// reconciler tick), slow enough that meta load is a
    /// rounding error.
    pub tick: Duration,
    /// On a fresh cluster (empty `tablets` table) the leader
    /// seeds this many rows. Defaults to the legacy
    /// `AGENTDB_BOXES` value (4) for continuity. Becomes
    /// cluster-wide tablet count post-multi-node.
    pub bootstrap_tablets: u32,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(5),
            bootstrap_tablets: 4,
        }
    }
}

impl PlacementConfig {
    /// Env overrides. `bootstrap_tablets` defaults to
    /// `AGENTDB_BOXES` first (legacy) then 4.
    pub fn from_env() -> Self {
        let d = Self::default();
        let tick = std::env::var("AGENTDB_PLACEMENT_TICK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.tick);
        let bootstrap_tablets = std::env::var("AGENTDB_CLUSTER_TABLETS")
            .or_else(|_| std::env::var("AGENTDB_BOXES"))
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(d.bootstrap_tablets);
        Self {
            tick,
            bootstrap_tablets,
        }
    }
}

/// Background-task owner. Drop cancels the loop.
pub struct TabletPlacement {
    config: PlacementConfig,
    assignments_total: Arc<AtomicU64>,
    bootstraps_total: Arc<AtomicU64>,
    rebalances_total: Arc<AtomicU64>,
    _handle: JoinHandle<()>,
}

impl TabletPlacement {
    pub fn start(
        meta: Arc<MetaStore>,
        election: Arc<LeaderElection>,
        config: PlacementConfig,
    ) -> Arc<Self> {
        let assignments_total = Arc::new(AtomicU64::new(0));
        let bootstraps_total = Arc::new(AtomicU64::new(0));
        let rebalances_total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            meta,
            election,
            config,
            assignments_total.clone(),
            bootstraps_total.clone(),
            rebalances_total.clone(),
        ));
        Arc::new(Self {
            config,
            assignments_total,
            bootstraps_total,
            rebalances_total,
            _handle: handle,
        })
    }

    /// Total placements (`owner_node_id IS NULL → some_node`)
    /// performed by this process while leader. Followers
    /// always report 0.
    pub fn assignments_total(&self) -> u64 {
        self.assignments_total.load(Ordering::Acquire)
    }

    /// How many times this process bootstrapped an empty
    /// `tablets` table while leader. Effectively a small
    /// integer (1 over the lifetime of any normal cluster).
    pub fn bootstraps_total(&self) -> u64 {
        self.bootstraps_total.load(Ordering::Acquire)
    }

    /// Total tablets relinquished by this process during the
    /// rebalance pass (an over-loaded alive node had a tablet
    /// taken back so it could be reassigned to an under-loaded
    /// node). Every relinquish causes a brief data-path
    /// disruption on the previous owner, so this counter
    /// doubles as a "how many forced-evictions has the leader
    /// inflicted" SLO signal.
    pub fn rebalances_total(&self) -> u64 {
        self.rebalances_total.load(Ordering::Acquire)
    }

    pub fn config(&self) -> PlacementConfig {
        self.config
    }
}

async fn run_loop(
    meta: Arc<MetaStore>,
    election: Arc<LeaderElection>,
    config: PlacementConfig,
    assignments_total: Arc<AtomicU64>,
    bootstraps_total: Arc<AtomicU64>,
    rebalances_total: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so the loop aligns with
    // "every N seconds after startup" rather than "on boot"
    // — gives leader-election time to converge before we
    // act on it.
    interval.tick().await;
    loop {
        interval.tick().await;
        if !election.is_leader() {
            continue;
        }
        sweep_once(
            &meta,
            config,
            &assignments_total,
            &bootstraps_total,
            &rebalances_total,
        )
        .await;
    }
}

/// One placement sweep. Public so the server boot path can run
/// it synchronously after leader election converges, without
/// waiting for the periodic loop's first tick.
///
/// Three phases, each idempotent:
///
/// 1. **Bootstrap**. Empty `tablets` table on a brand-new
///    cluster gets `[0..bootstrap_tablets)` rows.
/// 2. **Rebalance**. Compute the per-alive-node tablet count.
///    `target = ceil(total_owned / num_alive)`. Every alive
///    node holding more than `target` relinquishes the excess
///    via `tablets::relinquish` (which clears
///    `owner_node_id` and bumps the epoch — the previous
///    owner's lease becomes stale on its next renewal). The
///    relinquished tablets fall through to phase 3 as
///    "unowned" and get reassigned in the same sweep.
/// 3. **Assign-unowned**. Every `owner_node_id IS NULL`
///    tablet is assigned round-robin to an alive node.
///
/// Rebalance is only triggered when `max_count - min_count > 1`
/// across alive nodes; an already-balanced cluster (or one
/// where the imbalance is the unavoidable +1 from a
/// non-divisible total) is a steady-state no-op so a node
/// joining mid-stream doesn't cause continuous tablet
/// thrashing.
#[tracing::instrument(
    name = "placement.sweep_once",
    skip(meta, assignments_total, bootstraps_total, rebalances_total),
    fields(
        bootstrap_target = config.bootstrap_tablets,
    ),
)]
pub async fn sweep_once(
    meta: &MetaStore,
    config: PlacementConfig,
    assignments_total: &AtomicU64,
    bootstraps_total: &AtomicU64,
    rebalances_total: &AtomicU64,
) {
    // 1. Bootstrap empty cluster.
    match meta_tablets::list_all(meta.pool()).await {
        Ok(rows) if rows.is_empty() => {
            let now = now_ms();
            let mut seeded = 0u32;
            for id in 0..config.bootstrap_tablets {
                if let Err(err) = meta_tablets::get_or_create(meta.pool(), id as i64, now).await {
                    tracing::warn!(%err, tablet_id = id, "bootstrap: get_or_create failed");
                    continue;
                }
                seeded += 1;
            }
            if seeded > 0 {
                bootstraps_total.fetch_add(1, Ordering::Release);
                tracing::info!(
                    seeded,
                    target = config.bootstrap_tablets,
                    "tablet placement: bootstrapped empty cluster"
                );
            }
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(%err, "tablet placement: list_all failed; skipping tick");
            return;
        }
    }

    // 2. Pull current state.
    let tablets = match meta_tablets::list_all(meta.pool()).await {
        Ok(t) => t,
        Err(err) => {
            tracing::warn!(%err, "tablet placement: list_all (post-bootstrap) failed");
            return;
        }
    };
    let nodes = match meta_nodes::list_all(meta.pool()).await {
        Ok(n) => n,
        Err(err) => {
            tracing::warn!(%err, "tablet placement: list_all (nodes) failed");
            return;
        }
    };

    // Only `state == "alive"` nodes are placement targets.
    // Drained / leaving / dead nodes are explicitly excluded —
    // assigning to them would either thrash (leaving) or
    // never actually take effect (dead/draining).
    let alive: Vec<&str> = nodes
        .iter()
        .filter(|n| n.state == "alive")
        .map(|n| n.node_id.as_str())
        .collect();
    if alive.is_empty() {
        // No targets — nothing to do. This is the steady state
        // on a fresh cluster between bootstrap and the first
        // node registering itself in `nodes`.
        return;
    }

    // 3. Rebalance: relinquish from over-loaded alive nodes
    // back to the unowned pool. Phase 4 picks them up.
    let mut tablets = tablets;
    let relinquished = rebalance_pass(meta, &alive, &tablets).await;
    if !relinquished.is_empty() {
        rebalances_total.fetch_add(relinquished.len() as u64, Ordering::Release);
        // Patch the in-memory snapshot so the assign loop sees
        // the just-relinquished rows as unowned, instead of
        // having to re-query meta.
        for t in &mut tablets {
            if relinquished.contains(&t.tablet_id) {
                t.owner_node_id = None;
                t.lease_expires_at_ms = 0;
            }
        }
    }

    // 4. Load-aware assign: every unowned tablet goes to the
    // currently least-loaded alive node, ties broken by
    // node-id for determinism. This makes the rebalance pass
    // self-correcting — freshly-relinquished tablets land on
    // peers, not back on the source. Equivalent to round-robin
    // when starting from a uniformly-empty load profile.
    let mut load: std::collections::HashMap<&str, usize> = alive
        .iter()
        .map(|n| (*n, 0usize))
        .collect();
    for t in &tablets {
        if let Some(owner) = t.owner_node_id.as_deref() {
            if let Some(c) = load.get_mut(owner) {
                *c += 1;
            }
        }
    }

    let mut placed = 0u64;
    for t in &tablets {
        if t.owner_node_id.is_some() {
            continue;
        }
        // Pick the alive node with the lowest current count;
        // tie-break by node-id alphabetical so two leaders on
        // the same input (the inevitable transition after
        // election churn) make the same choice.
        let target = alive
            .iter()
            .map(|n| (*n, *load.get(n).unwrap_or(&0)))
            .min_by(|(an, ac), (bn, bc)| ac.cmp(bc).then_with(|| an.cmp(bn)))
            .map(|(n, _)| n);
        let Some(target) = target else {
            // alive non-empty was checked at the top of the
            // function, so this branch is unreachable in
            // practice. Keep it defensive rather than panicking.
            break;
        };
        match meta_tablets::assign_to(meta.pool(), t.tablet_id, target).await {
            Ok(true) => {
                placed += 1;
                if let Some(c) = load.get_mut(target) {
                    *c += 1;
                }
                tracing::info!(
                    tablet_id = t.tablet_id,
                    node_id = target,
                    "tablet placement: assigned unowned tablet to alive node"
                );
            }
            Ok(false) => {
                // Lost the race with another writer (placement
                // shouldn't run on multiple nodes, but a node
                // might also have raced to call assume_ownership
                // directly). Harmless — observed by `false` and
                // not counted.
            }
            Err(err) => {
                tracing::warn!(
                    %err,
                    tablet_id = t.tablet_id,
                    target,
                    "tablet placement: assign_to failed"
                );
            }
        }
    }
    if placed > 0 {
        assignments_total.fetch_add(placed, Ordering::Release);
    }
}

/// Active-rebalance pass. Returns the set of tablets that were
/// relinquished — the caller updates its in-memory snapshot
/// with that set so phase 4 sees them as unowned without a
/// re-query.
///
/// Algorithm:
/// 1. Count tablets per alive node. Tablets owned by a non-
///    alive node (e.g., dead/leaving) are intentionally NOT
///    counted toward any alive node — the failover path
///    (StaleTabletReaper + assign-unowned) handles those.
/// 2. Skip if `max_count - min_count <= 1`. The +1 is the
///    unavoidable rounding when total isn't divisible by the
///    number of alive nodes; treating it as imbalance would
///    cause perpetual reshuffling between two equally
///    "balanced" states.
/// 3. Compute `target = ceil(total_alive_owned / num_alive)`.
///    Every alive node owning > target relinquishes the excess
///    (smallest tablet ids first, for determinism).
async fn rebalance_pass(
    meta: &MetaStore,
    alive: &[&str],
    tablets: &[agentdb_meta::tablets::Tablet],
) -> Vec<i64> {
    use std::collections::HashMap;

    // Build per-alive-node ownership map. Owned-by-dead/leaving
    // is excluded from balance accounting — those rows still go
    // through the regular reassign path on a follow-up tick.
    let alive_set: std::collections::HashSet<&str> = alive.iter().copied().collect();
    let mut owned_by_alive: HashMap<&str, Vec<i64>> = HashMap::new();
    for n in alive {
        owned_by_alive.insert(*n, Vec::new());
    }
    for t in tablets {
        if let Some(owner) = t.owner_node_id.as_deref() {
            if alive_set.contains(owner) {
                owned_by_alive
                    .entry(owner)
                    .or_default()
                    .push(t.tablet_id);
            }
        }
    }
    let total_owned: usize = owned_by_alive.values().map(|v| v.len()).sum();
    if total_owned == 0 {
        return Vec::new();
    }
    let num_alive = alive.len();
    if num_alive == 0 {
        return Vec::new();
    }
    let max_count = owned_by_alive.values().map(|v| v.len()).max().unwrap_or(0);
    let min_count = owned_by_alive.values().map(|v| v.len()).min().unwrap_or(0);
    if max_count.saturating_sub(min_count) <= 1 {
        // Already balanced (within unavoidable rounding).
        return Vec::new();
    }
    // ceil(total / num_alive)
    let target = total_owned.div_ceil(num_alive);

    let mut to_relinquish: Vec<i64> = Vec::new();
    // Iterate alive nodes in stable order so the choice of
    // which tablets to move is deterministic across runs.
    let mut sorted_nodes: Vec<&&str> = owned_by_alive.keys().collect();
    sorted_nodes.sort();
    for node_id in sorted_nodes {
        let mut owned = owned_by_alive.get(*node_id).cloned().unwrap_or_default();
        if owned.len() <= target {
            continue;
        }
        owned.sort();
        let excess = owned.len() - target;
        // Take the highest tablet ids — by convention bootstrap
        // tablets cluster at the low end and applications tend
        // to map "important" sessions to early shards via FNV.
        // Releasing the high end first minimises hot-cluster
        // disruption.
        let drop_from = owned.len() - excess;
        for tid in &owned[drop_from..] {
            to_relinquish.push(*tid);
        }
    }

    let mut actually_relinquished = Vec::with_capacity(to_relinquish.len());
    for tid in to_relinquish {
        match meta_tablets::relinquish(meta.pool(), tid).await {
            Ok(true) => {
                actually_relinquished.push(tid);
                tracing::info!(
                    tablet_id = tid,
                    "tablet placement: relinquished tablet from over-loaded alive node"
                );
            }
            Ok(false) => {
                // Row was unowned by the time we got there —
                // someone else relinquished or the row was
                // deleted. Skip silently.
            }
            Err(err) => {
                tracing::warn!(
                    %err,
                    tablet_id = tid,
                    "tablet placement: relinquish failed; will retry next tick"
                );
            }
        }
    }
    actually_relinquished
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
    use crate::scheduler::{LeaderElection, LeaderElectionConfig};
    use agentdb_meta::{migrations, MetaDialect};

    async fn fresh_meta() -> Arc<MetaStore> {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        migrations::apply(meta.pool(), MetaDialect::Sqlite)
            .await
            .unwrap();
        Arc::new(meta)
    }

    async fn file_backed_meta() -> (tempfile::NamedTempFile, Arc<MetaStore>) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = MetaStore::open_sqlite(&path).await.unwrap();
        (file, Arc::new(meta))
    }

    fn fast_election() -> LeaderElectionConfig {
        LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn sweep_once_bootstraps_empty_cluster() {
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "n1", "addr", None, "alive", 0)
            .await
            .unwrap();
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        let cfg = PlacementConfig {
            tick: Duration::from_secs(1),
            bootstrap_tablets: 4,
        };
        sweep_once(&meta, cfg, &assigns, &boots, &rebals).await;
        // Bootstrap created 4 rows.
        let all = meta_tablets::list_all(meta.pool()).await.unwrap();
        assert_eq!(all.len(), 4);
        // All 4 got assigned to the only alive node.
        for t in &all {
            assert_eq!(t.owner_node_id.as_deref(), Some("n1"));
        }
        assert_eq!(boots.load(Ordering::Acquire), 1);
        assert_eq!(assigns.load(Ordering::Acquire), 4);
    }

    #[tokio::test]
    async fn sweep_once_distributes_round_robin_across_alive_nodes() {
        let meta = fresh_meta().await;
        // Two alive, one dead. Bootstrap 4 tablets.
        for (id, st) in [("a", "alive"), ("b", "alive"), ("c", "dead")] {
            meta_nodes::upsert(meta.pool(), id, "addr", None, st, 0)
                .await
                .unwrap();
        }
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        let cfg = PlacementConfig {
            tick: Duration::from_secs(1),
            bootstrap_tablets: 4,
        };
        sweep_once(&meta, cfg, &assigns, &boots, &rebals).await;
        let owned_a = meta_tablets::list_owned_by(meta.pool(), "a").await.unwrap();
        let owned_b = meta_tablets::list_owned_by(meta.pool(), "b").await.unwrap();
        let owned_c = meta_tablets::list_owned_by(meta.pool(), "c").await.unwrap();
        assert_eq!(owned_a.len() + owned_b.len(), 4);
        // 4 tablets across 2 alive nodes → 2/2 split.
        assert_eq!(owned_a.len(), 2);
        assert_eq!(owned_b.len(), 2);
        // Dead node never receives placements.
        assert!(owned_c.is_empty());
    }

    #[tokio::test]
    async fn sweep_once_skips_when_no_alive_nodes() {
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "dying", "a", None, "leaving", 0)
            .await
            .unwrap();
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        let cfg = PlacementConfig {
            tick: Duration::from_secs(1),
            bootstrap_tablets: 4,
        };
        sweep_once(&meta, cfg, &assigns, &boots, &rebals).await;
        // Bootstrap still creates the rows; placement just
        // can't assign them anywhere.
        let all = meta_tablets::list_all(meta.pool()).await.unwrap();
        assert_eq!(all.len(), 4);
        for t in &all {
            assert!(
                t.owner_node_id.is_none(),
                "no alive nodes → tablet must stay unowned"
            );
        }
        assert_eq!(assigns.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn sweep_once_picks_up_tablets_orphaned_by_reaper() {
        // Simulates failover: previous owner died, reaper
        // cleared the row to NULL, placement reassigns.
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "alive", "a", None, "alive", 0)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 7, 0)
            .await
            .unwrap();
        // Tablet 7 is unowned; sweep_once must claim it for
        // the only alive node.
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        let cfg = PlacementConfig {
            tick: Duration::from_secs(1),
            bootstrap_tablets: 0,
        };
        sweep_once(&meta, cfg, &assigns, &boots, &rebals).await;
        let t = meta_tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert_eq!(t.owner_node_id.as_deref(), Some("alive"));
        // Epoch and lease must be untouched — the reconciler
        // on "alive" will bump them when it acquires.
        assert_eq!(t.epoch, 0);
        assert_eq!(t.lease_expires_at_ms, 0);
        assert_eq!(boots.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn placement_loop_only_runs_on_leader() {
        // Two election loops on the same meta — the first wins
        // a long lease, the second never becomes leader. The
        // placement loop on the second must therefore never
        // assign anything even though the cluster has unowned
        // tablets and an alive node.
        let (_guard, meta) = file_backed_meta().await;
        meta_nodes::upsert(meta.pool(), "alive", "a", None, "alive", 0)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 0, 0)
            .await
            .unwrap();
        let leader = LeaderElection::start(meta.clone(), "leader".into(), fast_election());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(leader.is_leader());

        let follower = LeaderElection::start(meta.clone(), "follower".into(), fast_election());
        let placement = TabletPlacement::start(
            meta.clone(),
            follower.clone(),
            PlacementConfig {
                tick: Duration::from_millis(100),
                bootstrap_tablets: 0,
            },
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!follower.is_leader());
        assert_eq!(placement.assignments_total(), 0);
        let t = meta_tablets::get(meta.pool(), 0).await.unwrap().unwrap();
        assert!(
            t.owner_node_id.is_none(),
            "follower-side placement must not have assigned anything"
        );
    }

    #[tokio::test]
    async fn placement_on_leader_eventually_assigns_unowned_tablets() {
        let (_guard, meta) = file_backed_meta().await;
        meta_nodes::upsert(meta.pool(), "n1", "a", None, "alive", 0)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 0, 0)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 1, 0)
            .await
            .unwrap();

        let election = LeaderElection::start(meta.clone(), "n1".into(), fast_election());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader());
        let placement = TabletPlacement::start(
            meta.clone(),
            election,
            PlacementConfig {
                tick: Duration::from_millis(100),
                bootstrap_tablets: 0,
            },
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        let owned = meta_tablets::list_owned_by(meta.pool(), "n1")
            .await
            .unwrap();
        assert_eq!(owned.len(), 2);
        assert!(placement.assignments_total() >= 2);
    }

    /// Helper for rebalance tests. Sets up `tablet_count`
    /// tablets, all owned by `seeded_owner`, plus the named
    /// alive nodes registered in the `nodes` table.
    async fn seed_imbalanced(
        meta: &Arc<MetaStore>,
        alive_nodes: &[&str],
        tablet_count: i64,
        seeded_owner: &str,
    ) {
        for node in alive_nodes {
            meta_nodes::upsert(meta.pool(), node, "addr", None, "alive", 0)
                .await
                .unwrap();
        }
        for id in 0..tablet_count {
            meta_tablets::get_or_create(meta.pool(), id, 0).await.unwrap();
            meta_tablets::assume_ownership(meta.pool(), id, 0, seeded_owner, i64::MAX)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn rebalance_redistributes_when_a_new_node_joins_an_already_full_cluster() {
        // Pre-state: node-a owns all 4 tablets, node-b just
        // joined. After one sweep, both nodes should hold 2.
        let meta = fresh_meta().await;
        seed_imbalanced(&meta, &["a", "b"], 4, "a").await;
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        let cfg = PlacementConfig {
            tick: Duration::from_secs(1),
            bootstrap_tablets: 0,
        };
        sweep_once(&meta, cfg, &assigns, &boots, &rebals).await;

        let owned_a = meta_tablets::list_owned_by(meta.pool(), "a").await.unwrap();
        let owned_b = meta_tablets::list_owned_by(meta.pool(), "b").await.unwrap();
        assert_eq!(owned_a.len(), 2, "a should retain 2 of 4 tablets");
        assert_eq!(owned_b.len(), 2, "b should pick up the released 2");
        assert_eq!(rebals.load(Ordering::Acquire), 2, "two relinquishes");
        assert_eq!(assigns.load(Ordering::Acquire), 2, "two reassigns");

        // Epoch on the moved tablets must have advanced.
        // (relinquish bumps once, assign_to writes nothing,
        // and the receiving node's lease.acquire will bump
        // again — but at this point in the test that hasn't
        // happened yet, so we expect epoch=2: 1 from the
        // original assume_ownership, +1 from relinquish.)
        for t in &owned_b {
            assert_eq!(
                t.epoch, 2,
                "moved tablet's epoch should reflect relinquish"
            );
        }
    }

    #[tokio::test]
    async fn rebalance_is_a_noop_on_already_balanced_state() {
        // 4 tablets, 2/2 split. No relinquish should fire.
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "a", "addr", None, "alive", 0)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "b", "addr", None, "alive", 0)
            .await
            .unwrap();
        for (id, owner) in [(0, "a"), (1, "a"), (2, "b"), (3, "b")] {
            meta_tablets::get_or_create(meta.pool(), id, 0).await.unwrap();
            meta_tablets::assume_ownership(meta.pool(), id, 0, owner, i64::MAX)
                .await
                .unwrap();
        }
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        sweep_once(
            &meta,
            PlacementConfig {
                tick: Duration::from_secs(1),
                bootstrap_tablets: 0,
            },
            &assigns,
            &boots,
            &rebals,
        )
        .await;
        assert_eq!(rebals.load(Ordering::Acquire), 0);
        assert_eq!(assigns.load(Ordering::Acquire), 0);
        assert_eq!(
            meta_tablets::list_owned_by(meta.pool(), "a")
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            meta_tablets::list_owned_by(meta.pool(), "b")
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn rebalance_tolerates_unavoidable_plus_one_imbalance() {
        // 5 tablets, 2 nodes — one node holds 3, the other 2.
        // That's the smallest possible imbalance and not worth
        // shuffling for.
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "a", "addr", None, "alive", 0)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "b", "addr", None, "alive", 0)
            .await
            .unwrap();
        for (id, owner) in [(0, "a"), (1, "a"), (2, "a"), (3, "b"), (4, "b")] {
            meta_tablets::get_or_create(meta.pool(), id, 0).await.unwrap();
            meta_tablets::assume_ownership(meta.pool(), id, 0, owner, i64::MAX)
                .await
                .unwrap();
        }
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        sweep_once(
            &meta,
            PlacementConfig {
                tick: Duration::from_secs(1),
                bootstrap_tablets: 0,
            },
            &assigns,
            &boots,
            &rebals,
        )
        .await;
        assert_eq!(
            rebals.load(Ordering::Acquire),
            0,
            "+1 imbalance is steady state, must not trigger relinquish"
        );
    }

    #[tokio::test]
    async fn rebalance_excludes_dead_node_holdings_from_load_calc() {
        // a (alive) owns 1, dead-node owns 3. total_alive_owned
        // is 1, num_alive=1, target=1. No relinquish on a — its
        // count already matches target.
        // The 3 dead-owned rows are NOT mass-relinquished here:
        // that's the StaleTabletReaper's job. Placement only
        // counts alive ownership for load.
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "a", "addr", None, "alive", 0)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "dead", "addr", None, "dead", 0)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 0, 0).await.unwrap();
        meta_tablets::assume_ownership(meta.pool(), 0, 0, "a", i64::MAX)
            .await
            .unwrap();
        for id in [1, 2, 3] {
            meta_tablets::get_or_create(meta.pool(), id, 0).await.unwrap();
            meta_tablets::assume_ownership(meta.pool(), id, 0, "dead", i64::MAX)
                .await
                .unwrap();
        }
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        sweep_once(
            &meta,
            PlacementConfig {
                tick: Duration::from_secs(1),
                bootstrap_tablets: 0,
            },
            &assigns,
            &boots,
            &rebals,
        )
        .await;
        assert_eq!(
            rebals.load(Ordering::Acquire),
            0,
            "dead-owned rows must not influence rebalance"
        );
        // a still owns its single tablet.
        assert_eq!(
            meta_tablets::list_owned_by(meta.pool(), "a")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn rebalance_distributes_extras_evenly_across_three_nodes() {
        // Pre-state: a owns 6, b/c just joined. target =
        // ceil(6/3) = 2. a should release 4 tablets; b and c
        // each pick up 2.
        let meta = fresh_meta().await;
        seed_imbalanced(&meta, &["a", "b", "c"], 6, "a").await;
        let assigns = Arc::new(AtomicU64::new(0));
        let boots = Arc::new(AtomicU64::new(0));
        let rebals = Arc::new(AtomicU64::new(0));
        sweep_once(
            &meta,
            PlacementConfig {
                tick: Duration::from_secs(1),
                bootstrap_tablets: 0,
            },
            &assigns,
            &boots,
            &rebals,
        )
        .await;
        let count_a = meta_tablets::list_owned_by(meta.pool(), "a")
            .await
            .unwrap()
            .len();
        let count_b = meta_tablets::list_owned_by(meta.pool(), "b")
            .await
            .unwrap()
            .len();
        let count_c = meta_tablets::list_owned_by(meta.pool(), "c")
            .await
            .unwrap()
            .len();
        assert_eq!(count_a + count_b + count_c, 6);
        assert_eq!(count_a, 2);
        assert_eq!(count_b, 2);
        assert_eq!(count_c, 2);
        assert_eq!(rebals.load(Ordering::Acquire), 4);
    }
}
