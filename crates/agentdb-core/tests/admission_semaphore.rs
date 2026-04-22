//! M23 integration: prove that the per-box admission semaphore
//! (a) caps in-flight writes at the configured capacity, (b) lets
//! waiters proceed as earlier writes release, and (c) surfaces a
//! non-zero `admission_waits_total` when the cap actually bites.

use std::sync::Arc;

use agentdb_core::{append_and_fetch, BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use futures::future::try_join_all;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

async fn fixture_with_capacity(
    capacity: u32,
) -> (
    TempDir,
    TempDir,
    Arc<BoxRegistry>,
    Arc<TenantRegistry>,
    Arc<SessionRegistry>,
) {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let reg = Arc::new(
        BoxRegistry::new_with_capacity(
            kv.clone(),
            store,
            1,
            Arc::new(HlcClock::default()),
            capacity,
        )
        .unwrap(),
    );
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));
    (kv_dir, obj_dir, reg, tenants, sessions)
}

#[tokio::test]
async fn admission_capacity_is_reported_from_the_box() {
    let (_kv, _obj, reg, _t, _s) = fixture_with_capacity(13).await;
    let box_ = reg.route_tenant("anything");
    assert_eq!(box_.admission_capacity(), 13);
    // Nothing in flight yet.
    assert_eq!(box_.admission_in_flight(), 0);
}

#[tokio::test]
async fn zero_capacity_is_clamped_to_one_so_the_box_still_works() {
    // A capacity of 0 would deadlock every write. The constructor
    // clamps to 1 to make misconfiguration visible but non-fatal.
    let (_kv, _obj, reg, _t, _s) = fixture_with_capacity(0).await;
    let box_ = reg.route_tenant("anything");
    assert_eq!(box_.admission_capacity(), 1);
}

#[tokio::test]
async fn many_concurrent_writes_succeed_under_tight_capacity() {
    // Fire 16 concurrent writes through a box with admission
    // capacity 4 — lots of contention — and confirm every write
    // completes. This is the regression guard against "admission
    // gate deadlocks when waiters greatly exceed capacity." Uses 16
    // distinct tenants so the per-tenant write lock doesn't serialize
    // them; only the admission gate does.
    let (_kv, _obj, reg, tenants, sessions) = fixture_with_capacity(4).await;
    let box_ = reg.route_tenant("t-0");

    let futures = (0..16u32).map(|i| {
        let reg = reg.clone();
        let tenants = tenants.clone();
        let sessions = sessions.clone();
        async move {
            let tenant = format!("t-{i}");
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                &tenant,
                "s-1",
                json!({"i": i}),
                1,
                0,
            )
            .await
        }
    });
    let outs = try_join_all(futures)
        .await
        .expect("all writes must complete");
    assert_eq!(outs.len(), 16);
    for out in outs {
        assert_eq!(out.new_seq, 1);
    }

    // On a good day every write passes through the gate immediately
    // on a fast machine; on a slow machine some will have waited.
    // Don't assert waits > 0 (flaky) — but make sure the counter
    // didn't *underflow* or miscount.
    let snap = box_.metrics.snapshot();
    assert!(
        snap.admission_waits_total <= 16,
        "waits counter cannot exceed total requests"
    );
    // By the time every future has completed the semaphore must be
    // back to zero in-flight — permits all released on drop.
    assert_eq!(box_.admission_in_flight(), 0);
}

#[tokio::test]
async fn admission_waits_total_bumps_when_capacity_is_exceeded_deterministically() {
    // Deterministically force admission waits: spawn two writers
    // that hold the semaphore full (capacity 1) while a third tries
    // to enter. The third must have to wait.
    let (_kv, _obj, reg, tenants, sessions) = fixture_with_capacity(1).await;
    let box_ = reg.route_tenant("t-0");

    // Holder: acquire the only permit and keep it until we say so.
    let permit_box = box_.clone();
    let holder = tokio::spawn(async move {
        let _permit = permit_box.acquire_admission().await;
        // Yield for long enough that the waiter below is guaranteed
        // to land in the slow path.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Drop releases.
    });

    // Briefly let the holder actually acquire the permit.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let before = box_.metrics.snapshot().admission_waits_total;

    // Waiter: tries to write — must block on the semaphore until
    // the holder releases. Use a distinct tenant so the per-tenant
    // lock doesn't also serialize.
    let waiter = {
        let reg = reg.clone();
        let tenants = tenants.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                "t-waiter",
                "s-1",
                json!({"x": 1}),
                1,
                0,
            )
            .await
        })
    };

    holder.await.unwrap();
    let out = waiter.await.unwrap().unwrap();
    assert_eq!(out.new_seq, 1);

    let after = box_.metrics.snapshot().admission_waits_total;
    assert!(
        after > before,
        "admission_waits_total must advance: before={before} after={after}"
    );
}
