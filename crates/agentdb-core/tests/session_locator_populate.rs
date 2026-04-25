//! M26 Phase 1 integration coverage: every session creation
//! path on a meta-backed registry should leave a row in
//! `session_locators` keyed by the external session id, with
//! the right tenant_internal_id, tablet_id, and shard slot.
//!
//! The locator isn't yet consulted on the routing path —
//! that's a follow-up — but the populate-on-mint contract is
//! load-bearing for everything that comes after, and an
//! integration test pins it down before the consumers land.

use std::sync::Arc;

use agentdb_core::{
    append_and_fetch_with_actor, fork::fork_session, BoxRegistry, HlcClock, SessionRegistry,
    TenantRegistry,
};
use agentdb_meta::{session_locators, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

struct Env {
    _kv_dir: TempDir,
    _obj_dir: TempDir,
    _meta_file: tempfile::NamedTempFile,
    kv: Arc<dyn KvStore>,
    meta: Arc<MetaStore>,
    registry: Arc<BoxRegistry>,
    tenants: Arc<TenantRegistry>,
    sessions: Arc<SessionRegistry>,
}

async fn setup() -> Env {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "node-a".into(),
        )
        .await
        .unwrap(),
    );
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));
    Env {
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
        _meta_file: meta_file,
        kv,
        meta,
        registry,
        tenants,
        sessions,
    }
}

#[tokio::test]
async fn first_append_populates_session_locator() {
    let env = setup().await;
    append_and_fetch_with_actor(
        &env.registry,
        &env.tenants,
        &env.sessions,
        "tenant-a",
        "s-mint",
        json!({"k": 1}),
        None,
        0,
        1_000,
    )
    .await
    .unwrap();

    let locator = session_locators::get(env.meta.pool(), "s-mint")
        .await
        .unwrap()
        .expect("locator row written on first append");
    assert_eq!(locator.session_id, "s-mint");
    assert!(locator.tablet_id.is_some());
    // M26 Phase 1b: external tenant id is the authoritative
    // identity recorded on the locator.
    assert_eq!(locator.tenant_external_id.as_deref(), Some("tenant-a"));
    // Shard id should be FNV("s-mint") % SHARD_COUNT — recompute
    // to confirm the helper used the same hash.
    assert_eq!(
        locator.shard_id,
        agentdb_core::shard::shard_id("s-mint") as i32
    );
    assert_eq!(locator.created_at_ms, 1_000);
}

#[tokio::test]
async fn locator_is_idempotent_across_repeat_appends() {
    let env = setup().await;
    for i in 0..3 {
        append_and_fetch_with_actor(
            &env.registry,
            &env.tenants,
            &env.sessions,
            "tenant-a",
            "s-mint",
            json!({"k": i}),
            None,
            0,
            (i + 1) as u64 * 100,
        )
        .await
        .unwrap();
    }
    let l = session_locators::get(env.meta.pool(), "s-mint")
        .await
        .unwrap()
        .unwrap();
    // The first call's timestamp wins — repeat appends don't
    // overwrite the locator.
    assert_eq!(l.created_at_ms, 100);
}

#[tokio::test]
async fn fork_then_explicit_populate_writes_child_locator() {
    // `fork_session` itself is a sync primitive in agentdb-core
    // and deliberately does no meta I/O — that keeps the
    // primitive testable from sync contexts and avoids
    // blocking inside the per-tenant write lock. The REST
    // fork handler in `agentdb-api` is responsible for the
    // locator write, via `populate_session_locator`. This
    // test exercises that exact pattern: fork, then
    // explicitly populate, then verify.
    let env = setup().await;
    append_and_fetch_with_actor(
        &env.registry,
        &env.tenants,
        &env.sessions,
        "tenant-a",
        "s-parent",
        json!({"k": 1}),
        None,
        0,
        100,
    )
    .await
    .unwrap();

    let box_ = env.registry.route_session("tenant-a", "s-parent");
    let guard = box_.tenant_write_lock("tenant-a").lock_owned().await;
    let child = fork_session(
        &env.registry,
        &env.tenants,
        &env.sessions,
        &guard,
        "tenant-a",
        "s-parent",
        "s-child",
        200,
    )
    .unwrap();
    drop(guard);

    // Pre-populate state: child has no locator because the
    // sync primitive didn't write one.
    assert!(session_locators::get(env.meta.pool(), "s-child")
        .await
        .unwrap()
        .is_none());

    // Mirror what the REST fork handler does next.
    let tenant = env
        .tenants
        .lookup(box_.id, "tenant-a")
        .unwrap()
        .unwrap();
    agentdb_core::primitives::populate_session_locator(
        env.meta.as_ref(),
        &child.external_id,
        tenant.internal_id,
        "tenant-a",
        box_.id,
        200,
    )
    .await;
    let child_loc = session_locators::get(env.meta.pool(), "s-child")
        .await
        .unwrap()
        .expect("child locator after explicit populate");
    assert_eq!(child_loc.session_id, "s-child");
    assert!(child_loc.tablet_id.is_some());
    assert_eq!(child_loc.tenant_internal_id, tenant.internal_id as i64);
}

#[tokio::test]
async fn distinct_sessions_under_one_tenant_each_get_their_own_locator() {
    let env = setup().await;
    for sid in ["s-a", "s-b", "s-c"] {
        append_and_fetch_with_actor(
            &env.registry,
            &env.tenants,
            &env.sessions,
            "tenant-a",
            sid,
            json!({}),
            None,
            0,
            42,
        )
        .await
        .unwrap();
    }
    // tenant_internal_id is 1 for the first tenant minted.
    let mine = session_locators::list_by_tenant(env.meta.pool(), 1, None, 100)
        .await
        .unwrap();
    let ids: Vec<_> = mine.iter().map(|l| l.session_id.as_str()).collect();
    assert_eq!(ids, vec!["s-a", "s-b", "s-c"]);
}

// Reference the kv binding so clippy doesn't warn about the
// field; the registry already owns its own clone but we keep
// the env's handle for tests that want direct KV access in the
// future.
#[allow(dead_code)]
fn _touch_kv(env: &Env) {
    let _ = env.kv.clone();
}
