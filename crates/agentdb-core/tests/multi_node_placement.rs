//! Multi-node placement + reconcile integration.
//!
//! Brings up two empty BoxRegistry instances backed by the
//! same on-disk SQLite meta store and exercises the full
//! placement → reconcile → failover loop, the behavior the
//! docker-compose comment used to flag as "tracked, not
//! implemented."
//!
//! Test 1: bootstrap a fresh cluster with both nodes already
//! registered; placement spreads tablets across them; both
//! reconcilers pick up their assignments.
//!
//! Test 2: simulate failover. Node A owns tablets, then dies.
//! `StaleTabletReaper` clears the expired leases. Placement
//! reassigns the orphaned tablets to node B. Node B's
//! reconciler picks them up.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agentdb_core::{
    tablet_placement, tablet_reconciler, BoxRegistry, HlcClock, LeaderElection,
    LeaderElectionConfig, NodeReaper, NodeReaperConfig, PlacementConfig, ReaperConfig,
    StaleTabletReaper,
};
use agentdb_meta::{nodes as meta_nodes, tablets as meta_tablets, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

struct Node {
    registry: Arc<BoxRegistry>,
    _kv_dir: TempDir,
    _obj_dir: TempDir,
}

async fn build_node(meta: Arc<MetaStore>, node_id: &str) -> Node {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(
        BoxRegistry::new_with_meta_empty(
            kv,
            store,
            Arc::new(HlcClock::default()),
            meta,
            node_id.into(),
            64,
        )
        .await
        .unwrap(),
    );
    Node {
        registry,
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
    }
}

async fn fresh_meta() -> (tempfile::NamedTempFile, Arc<MetaStore>) {
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
async fn fresh_cluster_bootstrap_distributes_tablets_across_two_nodes() {
    let (_meta_file, meta) = fresh_meta().await;
    // Both nodes register before placement runs — the leader's
    // first placement tick sees both and round-robins.
    meta_nodes::upsert(meta.pool(), "a", "addr-a", None, "alive", 0)
        .await
        .unwrap();
    meta_nodes::upsert(meta.pool(), "b", "addr-b", None, "alive", 0)
        .await
        .unwrap();
    let node_a = build_node(meta.clone(), "a").await;
    let node_b = build_node(meta.clone(), "b").await;
    assert_eq!(node_a.registry.num_boxes(), 0);
    assert_eq!(node_b.registry.num_boxes(), 0);

    // One leader, one placement sweep, both reconcilers.
    let leader = LeaderElection::start(meta.clone(), "a".into(), fast_election());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(leader.is_leader());

    let assigns = AtomicU64::new(0);
    let boots = AtomicU64::new(0);
    let rebals = AtomicU64::new(0);
    let placement_cfg = PlacementConfig {
        tick: Duration::from_secs(1),
        bootstrap_tablets: 4,
    };
    tablet_placement::sweep_once(&meta, placement_cfg, &assigns, &boots, &rebals).await;

    // Bootstrap created 4 tablets; round-robin gave 2 to each node.
    assert_eq!(boots.load(Ordering::Acquire), 1);
    assert_eq!(assigns.load(Ordering::Acquire), 4);
    let owned_a = meta_tablets::list_owned_by(meta.pool(), "a").await.unwrap();
    let owned_b = meta_tablets::list_owned_by(meta.pool(), "b").await.unwrap();
    assert_eq!(owned_a.len(), 2);
    assert_eq!(owned_b.len(), 2);

    // Both reconcilers turn assignments into live boxes.
    let added_a = AtomicU64::new(0);
    let added_b = AtomicU64::new(0);
    let errs = AtomicU64::new(0);
    tablet_reconciler::reconcile_once(&node_a.registry, &added_a, &errs)
        .await
        .unwrap();
    tablet_reconciler::reconcile_once(&node_b.registry, &added_b, &errs)
        .await
        .unwrap();
    assert_eq!(node_a.registry.num_boxes(), 2);
    assert_eq!(node_b.registry.num_boxes(), 2);
    assert_eq!(errs.load(Ordering::Acquire), 0);

    // Cross-check: no tablet ended up on both registries.
    let ids_a: Vec<_> = node_a
        .registry
        .boxes_snapshot()
        .iter()
        .map(|b| b.id)
        .collect();
    let ids_b: Vec<_> = node_b
        .registry
        .boxes_snapshot()
        .iter()
        .map(|b| b.id)
        .collect();
    for id in &ids_a {
        assert!(
            !ids_b.contains(id),
            "tablet {id} ended up on both nodes — placement isn't isolating"
        );
    }
}

#[tokio::test]
async fn failover_reassigns_dead_node_tablets_to_survivor() {
    let (_meta_file, meta) = fresh_meta().await;
    // Pre-seed: two nodes, two tablets each on A. Lease is
    // already expired in this seed (lease_expires_at_ms in the
    // past) so the reaper can clear immediately.
    meta_nodes::upsert(meta.pool(), "a", "addr-a", None, "alive", 0)
        .await
        .unwrap();
    meta_nodes::upsert(meta.pool(), "b", "addr-b", None, "alive", 0)
        .await
        .unwrap();
    for id in [0, 1, 2, 3] {
        meta_tablets::get_or_create(meta.pool(), id, 0).await.unwrap();
    }
    // Node A held all tablets. Lease expired in the past
    // (the reaper will catch it on the first tick).
    for id in [0, 1, 2, 3] {
        meta_tablets::assume_ownership(meta.pool(), id, 0, "a", -1)
            .await
            .unwrap();
    }
    // Mark A dead so placement won't reassign it back.
    meta_nodes::upsert(meta.pool(), "a", "addr-a", None, "dead", 0)
        .await
        .unwrap();

    let node_b = build_node(meta.clone(), "b").await;
    assert_eq!(node_b.registry.num_boxes(), 0);

    let leader = LeaderElection::start(meta.clone(), "b".into(), fast_election());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(leader.is_leader());

    // Run reaper to clear A's expired leases. Use 0 safety
    // margin so the negative lease_expires_at_ms is caught
    // immediately.
    let reaper = StaleTabletReaper::start(
        meta.clone(),
        leader.clone(),
        ReaperConfig {
            tick: Duration::from_millis(50),
            safety_margin: Duration::from_millis(0),
        },
        None,
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        reaper.reassignments_total() >= 4,
        "reaper should have cleared all 4 expired leases, got {}",
        reaper.reassignments_total()
    );

    // Now placement reassigns the orphaned tablets to b.
    let assigns = AtomicU64::new(0);
    let boots = AtomicU64::new(0);
    let rebals = AtomicU64::new(0);
    tablet_placement::sweep_once(
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
    let owned_b = meta_tablets::list_owned_by(meta.pool(), "b").await.unwrap();
    assert_eq!(owned_b.len(), 4, "b should now own every tablet");
    assert_eq!(assigns.load(Ordering::Acquire), 4);

    // Reconciler picks them up.
    let added = AtomicU64::new(0);
    let errs = AtomicU64::new(0);
    tablet_reconciler::reconcile_once(&node_b.registry, &added, &errs)
        .await
        .unwrap();
    assert_eq!(node_b.registry.num_boxes(), 4);
    assert_eq!(added.load(Ordering::Acquire), 4);
    assert_eq!(errs.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn cooperative_rebalance_handshake_redistributes_to_new_node() {
    // End-to-end exercise of the cooperative-handoff machinery:
    //   1. Pre-state: node-a runs 4 tablets, node-b just joined.
    //   2. Leader's placement sweep signals 2 handoffs on a.
    //      Ownership in meta is unchanged at this point — a is
    //      still serving every tablet.
    //   3. a's TabletReconciler observes the signals, drops its
    //      local boxes, and atomically transfers ownership.
    //   4. b's TabletReconciler picks up the new ownership and
    //      starts serving the moved tablets.
    let (_meta_file, meta) = fresh_meta().await;
    meta_nodes::upsert(meta.pool(), "a", "addr-a", None, "alive", 0)
        .await
        .unwrap();
    meta_nodes::upsert(meta.pool(), "b", "addr-b", None, "alive", 0)
        .await
        .unwrap();
    for id in [0, 1, 2, 3] {
        meta_tablets::get_or_create(meta.pool(), id, 0).await.unwrap();
        meta_tablets::assume_ownership(meta.pool(), id, 0, "a", i64::MAX)
            .await
            .unwrap();
    }

    // Build both nodes' BoxRegistries. node-a's registry needs
    // to have boxes for tablets 0..3 already running so the
    // reconciler has something to remove during handoff. We
    // add them via add_box after constructing an empty
    // registry — same path the real boot uses.
    let node_a = build_node(meta.clone(), "a").await;
    for id in [0u16, 1, 2, 3] {
        node_a.registry.add_box(id).await.unwrap();
    }
    assert_eq!(node_a.registry.num_boxes(), 4);
    let node_b = build_node(meta.clone(), "b").await;
    assert_eq!(node_b.registry.num_boxes(), 0);

    let leader = LeaderElection::start(meta.clone(), "a".into(), fast_election());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(leader.is_leader());

    // Step 2: leader signals.
    let assigns = AtomicU64::new(0);
    let boots = AtomicU64::new(0);
    let rebals = AtomicU64::new(0);
    tablet_placement::sweep_once(
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
        2,
        "two cooperative handoffs should have been signalled"
    );
    // Ownership unchanged — a still has all 4 tablets.
    assert_eq!(
        meta_tablets::list_owned_by(meta.pool(), "a")
            .await
            .unwrap()
            .len(),
        4
    );
    assert_eq!(node_a.registry.num_boxes(), 4);
    assert_eq!(node_b.registry.num_boxes(), 0);

    // Step 3: a's reconciler picks up the signals, drops the
    // boxes, and completes the handoff.
    let added_a = AtomicU64::new(0);
    let errs_a = AtomicU64::new(0);
    tablet_reconciler::reconcile_once(&node_a.registry, &added_a, &errs_a)
        .await
        .unwrap();
    assert_eq!(
        node_a.registry.num_boxes(),
        2,
        "a should have dropped 2 boxes during handoff"
    );
    assert_eq!(
        meta_tablets::list_owned_by(meta.pool(), "b")
            .await
            .unwrap()
            .len(),
        2,
        "ownership should have transferred to b in meta"
    );

    // Step 4: b's reconciler activates the moved tablets.
    let added_b = AtomicU64::new(0);
    let errs_b = AtomicU64::new(0);
    tablet_reconciler::reconcile_once(&node_b.registry, &added_b, &errs_b)
        .await
        .unwrap();
    assert_eq!(node_b.registry.num_boxes(), 2);
    assert_eq!(added_b.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn node_reaper_marks_silent_node_dead_unblocking_placement() {
    // End-to-end: NodeReaper observes a stale heartbeat,
    // flips the row to `dead`, and from then on placement
    // will route around it. Combined coverage of the new
    // membership story (PR #66) and the new placement story
    // (this PR).
    let (_meta_file, meta) = fresh_meta().await;
    let hb = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 60_000;
    meta_nodes::upsert(meta.pool(), "alive", "addr-a", None, "alive", hb + 60_000)
        .await
        .unwrap();
    meta_nodes::upsert(meta.pool(), "silent", "addr-b", None, "alive", hb)
        .await
        .unwrap();

    let leader = LeaderElection::start(meta.clone(), "alive".into(), fast_election());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(leader.is_leader());

    let _node_reaper = NodeReaper::start(
        meta.clone(),
        leader.clone(),
        NodeReaperConfig {
            tick: Duration::from_millis(100),
            // 30s threshold: "alive" (hb=now) stays under, but
            // "silent" (hb=now-60s) is over and gets reaped.
            // An aggressive 1ms threshold would mark *every*
            // non-actively-bumping node dead, including our
            // synthetic "alive".
            dead_after: Duration::from_secs(30),
            gc_after: Duration::from_secs(3_600),
        },
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        meta_nodes::get(meta.pool(), "silent")
            .await
            .unwrap()
            .unwrap()
            .state,
        "dead",
        "node reaper must have flipped the silent node"
    );

    // Bootstrap a tablet and ensure placement picks the alive
    // node, not the dead one. Sanity check that "dead" rows
    // are excluded from the placement candidate set.
    meta_tablets::get_or_create(meta.pool(), 0, 0).await.unwrap();
    let assigns = AtomicU64::new(0);
    let boots = AtomicU64::new(0);
    let rebals = AtomicU64::new(0);
    tablet_placement::sweep_once(
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
    let t = meta_tablets::get(meta.pool(), 0).await.unwrap().unwrap();
    let nodes = meta_nodes::list_all(meta.pool()).await.unwrap();
    assert_eq!(
        t.owner_node_id.as_deref(),
        Some("alive"),
        "placement should have picked the alive node; saw nodes={:?} assigns={}",
        nodes
            .iter()
            .map(|n| (&n.node_id, &n.state))
            .collect::<Vec<_>>(),
        assigns.load(Ordering::Acquire),
    );
}
