//! M16 Phase E — end-to-end snapshot → restore → replay flow.
//!
//! Simulates a restart by:
//! 1. Appending events, letting the live index capture them
//! 2. Writing a snapshot to a shared object store
//! 3. Constructing a fresh `BoxRegistry` pointed at the same KV + store
//!    and triggering `indexes_built`, which should:
//!    - discover the snapshot
//!    - restore the indexed state
//!    - replay any events whose HLC exceeds the snapshot's cutoff
//!
//! Accepts a degraded fallback path too: if a snapshot is malformed or
//! missing, `indexes_built` must still return a populated index via the
//! pre-M16 full-scan path.

use std::sync::Arc;

use agentdb_core::{append_and_fetch, BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

// Tiny wrapper because we want to reuse the KV + object-store dirs
// across two `BoxRegistry` instances (the "before restart" and "after
// restart" phases). Live Box_ only exists inside BoxRegistry, so we
// rebuild the registry and let `indexes_built` do the recovery.
async fn setup_with_paths() -> (
    TempDir,
    TempDir,
    Arc<dyn KvStore>,
    Arc<dyn ObjectStore>,
) {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    (kv_dir, obj_dir, kv, store)
}

fn registry_from(kv: &Arc<dyn KvStore>, store: &Arc<dyn ObjectStore>) -> Arc<BoxRegistry> {
    // `BoxRegistry::new` creates a fresh HLC per call, which is fine —
    // recovery doesn't depend on the HLC of the second process sharing
    // any state with the first; it only uses the HLCs stored in
    // events and snapshots.
    Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap())
}

#[tokio::test]
async fn restart_with_snapshot_restores_indexed_events_without_full_rebuild() {
    let (_kd, _od, kv, store) = setup_with_paths().await;

    // --- Phase 1: original process ---
    let reg1 = registry_from(&kv, &store);
    let tenants1 = TenantRegistry::new(kv.clone());
    let sessions1 = SessionRegistry::new(kv.clone());

    // Append events under two sessions.
    for i in 1..=5u64 {
        append_and_fetch(
            &reg1,
            &tenants1,
            &sessions1,
            "t-1",
            "s-a",
            json!({"text": format!("alpha {i}")}),
            0,
            i * 10,
        )
        .await
        .unwrap();
    }
    for i in 1..=3u64 {
        append_and_fetch(
            &reg1,
            &tenants1,
            &sessions1,
            "t-1",
            "s-b",
            json!({"text": format!("beta {i}")}),
            0,
            (i + 100) * 10,
        )
        .await
        .unwrap();
    }

    // Snapshot at "now" — capture HLC cutoff and write.
    let snapshot_hlc = reg1.hlc().current();
    let box_ = reg1.route_tenant("t-1");
    // First trigger the initial index build so snapshot has something
    // to serialize (the initial build runs lazily on the first query
    // or, as we'll do here, on the first snapshot trigger).
    // Guard is held only for side effect (triggering lazy initial build).
    drop(
        box_.indexes_built(&kv, &store, &tenants1, &sessions1)
            .await
            .unwrap(),
    );
    let written = box_
        .snapshot_indexes_to_store(&store, snapshot_hlc)
        .await
        .unwrap();
    assert!(written.is_some(), "snapshot must upload successfully");

    // --- Phase 2: simulated restart ---
    // Drop reg1 entirely; build a fresh BoxRegistry against the same KV.
    drop(reg1);
    let reg2 = registry_from(&kv, &store);
    let tenants2 = TenantRegistry::new(kv.clone());
    let sessions2 = SessionRegistry::new(kv.clone());
    let box_2 = reg2.route_tenant("t-1");

    // Trigger the first index access on the new registry — recovery.
    let guard = box_2
        .indexes_built(&kv, &store, &tenants2, &sessions2)
        .await
        .unwrap();
    let idx = guard.as_ref().expect("indexes built via recovery");

    // All 8 pre-restart events must be searchable.
    use agentdb_index::text::TextIndex;
    let alpha_hits = idx.text.search("payload.text", "alpha", 10);
    let beta_hits = idx.text.search("payload.text", "beta", 10);
    assert_eq!(alpha_hits.len(), 5, "5 alpha events recovered from snapshot");
    assert_eq!(beta_hits.len(), 3, "3 beta events recovered from snapshot");
}

#[tokio::test]
async fn restart_with_snapshot_replays_post_cutoff_events() {
    let (_kd, _od, kv, store) = setup_with_paths().await;

    let reg1 = registry_from(&kv, &store);
    let tenants1 = TenantRegistry::new(kv.clone());
    let sessions1 = SessionRegistry::new(kv.clone());

    // Append 3 events.
    for i in 1..=3u64 {
        append_and_fetch(
            &reg1,
            &tenants1,
            &sessions1,
            "t-r",
            "s-r",
            json!({"text": format!("pre {i}")}),
            0,
            i * 10,
        )
        .await
        .unwrap();
    }

    // Snapshot — cutoff captured here.
    let snapshot_hlc = reg1.hlc().current();
    let box_ = reg1.route_tenant("t-r");
    // Guard is held only for side effect (triggering lazy initial build).
    drop(
        box_.indexes_built(&kv, &store, &tenants1, &sessions1)
            .await
            .unwrap(),
    );
    box_.snapshot_indexes_to_store(&store, snapshot_hlc)
        .await
        .unwrap();

    // Append two more events AFTER the snapshot cutoff.
    for i in 1..=2u64 {
        append_and_fetch(
            &reg1,
            &tenants1,
            &sessions1,
            "t-r",
            "s-r",
            json!({"text": format!("post {i}")}),
            0,
            (i + 100) * 10,
        )
        .await
        .unwrap();
    }

    // Simulated restart.
    drop(reg1);
    let reg2 = registry_from(&kv, &store);
    let tenants2 = TenantRegistry::new(kv.clone());
    let sessions2 = SessionRegistry::new(kv.clone());
    let box_2 = reg2.route_tenant("t-r");

    let guard = box_2
        .indexes_built(&kv, &store, &tenants2, &sessions2)
        .await
        .unwrap();
    let idx = guard.as_ref().expect("indexes built");

    use agentdb_index::text::TextIndex;
    // Pre-cutoff events from snapshot
    assert_eq!(idx.text.search("payload.text", "pre", 10).len(), 3);
    // Post-cutoff events from WAL replay
    assert_eq!(idx.text.search("payload.text", "post", 10).len(), 2);
}

#[tokio::test]
async fn restart_without_snapshot_falls_back_to_full_rebuild() {
    let (_kd, _od, kv, store) = setup_with_paths().await;

    let reg1 = registry_from(&kv, &store);
    let tenants1 = TenantRegistry::new(kv.clone());
    let sessions1 = SessionRegistry::new(kv.clone());

    // Append, don't snapshot.
    for i in 1..=4u64 {
        append_and_fetch(
            &reg1,
            &tenants1,
            &sessions1,
            "t-nofull",
            "s-nofull",
            json!({"text": format!("lorem {i}")}),
            0,
            i * 10,
        )
        .await
        .unwrap();
    }

    drop(reg1);
    let reg2 = registry_from(&kv, &store);
    let tenants2 = TenantRegistry::new(kv.clone());
    let sessions2 = SessionRegistry::new(kv.clone());
    let box_2 = reg2.route_tenant("t-nofull");

    let guard = box_2
        .indexes_built(&kv, &store, &tenants2, &sessions2)
        .await
        .unwrap();
    let idx = guard.as_ref().expect("indexes built");

    use agentdb_index::text::TextIndex;
    // All 4 events available via full-rebuild fallback.
    assert_eq!(idx.text.search("payload.text", "lorem", 10).len(), 4);
}
