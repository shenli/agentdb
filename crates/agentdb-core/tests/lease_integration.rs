//! M21 integration: prove that a registry built with meta enforces
//! the lease check on the write path, and that a box without a
//! configured lease (the common case) still behaves exactly as it did
//! pre-M21.

use std::sync::Arc;

use agentdb_core::{
    append_and_fetch, BoxRegistry, Error, HlcClock, SessionRegistry, TenantRegistry,
};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

struct TestEnv {
    _kv_dir: TempDir,
    _obj_dir: TempDir,
    kv: Arc<dyn KvStore>,
    store: Arc<dyn ObjectStore>,
    _meta: Arc<MetaStore>,
}

async fn setup() -> TestEnv {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
    TestEnv {
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
        kv,
        store,
        _meta: meta,
    }
}

async fn registry_with_meta(env: &TestEnv) -> BoxRegistry {
    BoxRegistry::new_with_meta(
        env.kv.clone(),
        env.store.clone(),
        4,
        Arc::new(HlcClock::default()),
        env._meta.clone(),
        "node-a".into(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn meta_backed_registry_allows_appends_while_lease_is_live() {
    let env = setup().await;
    let reg = registry_with_meta(&env).await;
    let tenants = TenantRegistry::new(env.kv.clone());
    let sessions = SessionRegistry::new(env.kv.clone());

    let out = append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"k": 1}),
        0,
        0,
    )
    .await
    .unwrap();
    assert_eq!(out.new_seq, 1);

    let box_ = reg.route_tenant("t-1");
    let lease = box_.lease().expect("meta-backed registry wires a lease");
    assert!(lease.is_live());
    let snap = box_.metrics.snapshot();
    assert!(
        snap.lease_renewals >= 1,
        "initial acquire counts as a renewal"
    );
    assert!(snap.lease_epoch >= 1);
}

#[tokio::test]
async fn expired_lease_causes_append_to_fail_with_lease_lost() {
    let env = setup().await;
    let reg = registry_with_meta(&env).await;
    let tenants = TenantRegistry::new(env.kv.clone());
    let sessions = SessionRegistry::new(env.kv.clone());

    // Pre-condition: appends succeed while the lease is live.
    append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"k": 1}),
        0,
        0,
    )
    .await
    .unwrap();

    // Force the routed box's lease to look expired.
    let box_ = reg.route_tenant("t-1");
    let lease = box_.lease().expect("meta-backed registry wires a lease");
    let epoch_before_expiry = lease.current_epoch();
    lease.expire_for_test();
    assert!(!lease.is_live(), "forced expiry flips is_live to false");

    // Next append on the same tenant must fail with LeaseLost, carrying
    // box id + the epoch observed at the moment of refusal.
    let err = append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"k": 2}),
        0,
        0,
    )
    .await
    .unwrap_err();
    match err {
        Error::LeaseLost { box_id, epoch } => {
            assert_eq!(box_id, box_.id);
            assert_eq!(epoch, epoch_before_expiry);
        }
        other => panic!("expected LeaseLost, got: {other:?}"),
    }
    assert!(
        box_.metrics.snapshot().lease_lost >= 1,
        "lease_lost metric records the refusal"
    );
}

#[tokio::test]
async fn legacy_registry_without_meta_has_no_lease_and_appends_freely() {
    // Pre-M21 behavior: BoxRegistry::new builds boxes with
    // `lease = None`, the write path skips the check, and clients see
    // no behavior change.
    let env = setup().await;
    let reg = BoxRegistry::new(env.kv.clone(), env.store.clone(), 4).unwrap();
    let tenants = TenantRegistry::new(env.kv.clone());
    let sessions = SessionRegistry::new(env.kv.clone());

    let out = append_and_fetch(
        &reg,
        &tenants,
        &sessions,
        "t-1",
        "s-1",
        json!({"k": 1}),
        0,
        0,
    )
    .await
    .unwrap();
    assert_eq!(out.new_seq, 1);

    let box_ = reg.route_tenant("t-1");
    assert!(box_.lease().is_none(), "legacy registry has no lease");
}
