use std::sync::Arc;
use std::time::Duration;

use agentdb_core::{
    append_and_fetch, cold_transition, fetch_last_n_merged, reactivate, scan_session_events,
    BoxRegistry, ReadCtx, SessionRegistry, TenantRegistry,
};
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

async fn setup() -> (
    TempDir,
    TempDir,
    Arc<dyn KvStore>,
    Arc<BoxRegistry>,
    Arc<TenantRegistry>,
    Arc<SessionRegistry>,
) {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));
    (kv_dir, obj_dir, kv, reg, tenants, sessions)
}

#[tokio::test]
async fn snapshot_isolates_cold_transition_midread() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    for i in 1..=3u64 {
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"i": i}),
            0,
            i,
        )
        .await
        .unwrap();
    }
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

    let ctx = ReadCtx::open(&box_, &kv).unwrap();
    let at_open = ctx
        .session(&sessions, tenant.internal_id, "s-1")
        .unwrap()
        .unwrap();

    // Mid-read: tier transition to Cold.
    {
        let guard = box_.write_lock.lock().await;
        let mut fresh = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        cold_transition(&kv, reg.object_store(), &sessions, &guard, &mut fresh)
            .await
            .unwrap();
    }

    // Warm events already captured in the snapshot.
    let events = scan_session_events(&ctx, &at_open).unwrap();
    assert_eq!(
        events.len(),
        3,
        "snapshot must see the pre-transition warm events"
    );
}

#[tokio::test]
async fn snapshot_isolates_reactivate_midread() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"x": 1}),
        0,
        1,
    )
    .await
    .unwrap();
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

    // Pre-cold.
    {
        let guard = box_.write_lock.lock().await;
        let mut fresh = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        cold_transition(&kv, reg.object_store(), &sessions, &guard, &mut fresh)
            .await
            .unwrap();
    }

    // Open ReadCtx — sees Cold.
    let ctx = ReadCtx::open(&box_, &kv).unwrap();
    let cold_view = ctx
        .session(&sessions, tenant.internal_id, "s-1")
        .unwrap()
        .unwrap();
    assert_eq!(cold_view.tier, agentdb_core::Tier::Cold);
    let seg_path = cold_view.segment_path.clone().unwrap();

    // Mid-read: reactivate.
    {
        let guard = box_.write_lock.lock().await;
        let mut fresh = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        reactivate(&kv, reg.object_store(), &sessions, &guard, &mut fresh)
            .await
            .unwrap();
    }

    // Segment still readable through the pin.
    assert!(agentdb_core::read_segment(reg.object_store(), &seg_path)
        .await
        .is_ok());
    drop(ctx);
    // After drop: the ReadCtx no longer pins the segment. It is NOT
    // guaranteed to be on disk forever now — a SegmentGc sweep would
    // reap it — but since no GC runs in this test, it still exists.
    assert!(
        agentdb_core::read_segment(reg.object_store(), &seg_path)
            .await
            .is_ok(),
        "no GC in this test so segment remains"
    );
}

#[tokio::test]
async fn fork_and_snapshot_compose() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    // Parent: 5 events.
    for i in 1..=5u64 {
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "parent",
            json!({"i": i}),
            0,
            i * 10,
        )
        .await
        .unwrap();
    }
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
    let parent = sessions
        .lookup(box_.id, tenant.internal_id, "parent")
        .unwrap()
        .unwrap();
    // Fork. Child sees parent with seq < parent.next_seq.
    let fork_ref = agentdb_core::ForkRef {
        parent_session_internal_id: parent.internal_id,
        parent_seq_at_fork: parent.next_seq,
    };
    sessions
        .get_or_mint_with_fork(box_.id, tenant.internal_id, "child", 1000, Some(fork_ref))
        .unwrap();
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "child",
        json!({"c": 1}),
        0,
        100,
    )
    .await
    .unwrap();

    let ctx = ReadCtx::open(&box_, &kv).unwrap();

    // Post-snapshot: writes invisible to `ctx`.
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "child",
        json!({"c": 2}),
        0,
        110,
    )
    .await
    .unwrap();
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "parent",
        json!({"i": 999}),
        0,
        120,
    )
    .await
    .unwrap();

    let child = ctx
        .session(&sessions, tenant.internal_id, "child")
        .unwrap()
        .unwrap();
    let got = fetch_last_n_merged(&ctx, reg.object_store(), &sessions, &child, 100)
        .await
        .unwrap();
    // Expected: 5 parent events (ts 10..50) + 1 child event (ts 100) = 6.
    // Nothing with ts 110 or 120.
    assert_eq!(got.len(), 6);
    assert_eq!(got.iter().filter(|e| e.ts == 120).count(), 0);
    assert_eq!(got.iter().filter(|e| e.ts == 110).count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_writes_do_not_affect_snapshot_scan() {
    let (_k, _o, kv, reg, tenants, sessions) = setup().await;
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"i": 0}),
        0,
        1,
    )
    .await
    .unwrap();
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();

    let ctx = ReadCtx::open(&box_, &kv).unwrap();
    let at_open = ctx
        .session(&sessions, tenant.internal_id, "s-1")
        .unwrap()
        .unwrap();

    // Spawn a writer that adds 50 events concurrently.
    let writer = {
        let reg = reg.clone();
        let tenants = tenants.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            for i in 1..=50u64 {
                append_and_fetch(
                    &reg,
                    &tenants,
                    &sessions,
                    "t-1",
                    "s-1",
                    json!({"i": i}),
                    0,
                    1000 + i,
                )
                .await
                .unwrap();
            }
        })
    };

    // While writer runs, scan the snapshot — result should be just the single event.
    for _ in 0..20 {
        let events = scan_session_events(&ctx, &at_open).unwrap();
        assert_eq!(events.len(), 1, "snapshot must stay frozen at 1 event");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    writer.await.unwrap();
}
