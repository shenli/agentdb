//! Integration coverage for eager SST-based whole-tablet movement (hardening
//! §7). These tests toggle the `AGENTDB_SST_MOVEMENT` process env, so they live
//! in their own test binary (a separate process) where that env cannot leak
//! into other test files, and are additionally `#[serial]` so they never race
//! each other.

use std::collections::BTreeMap;
use std::sync::Arc;

use agentdb_core::tenant::NewTenant;
use agentdb_core::{is_sst_snapshot_payload, Id, Runtime, RUNTIME_PRIMARY_TABLET_ID};
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, ReplicatedLog, RocksKvStore,
    SingleNodeReplicatedLog,
};
use serial_test::serial;
use tempfile::TempDir;

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

async fn source_runtime_with_tenants(dir: &TempDir, tenants: usize) -> (Arc<Runtime>, Vec<Id>) {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let runtime = Runtime::with_kv_and_replicated_logs(
        kv,
        content(),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, primary_log), (7, tablet_log)]),
    );

    let mut ids = Vec::new();
    for i in 0..tenants {
        let seed = runtime.clone();
        let tenant = runtime
            .with_tablet_context(7, async move {
                seed.create_tenant(NewTenant {
                    external_key: format!("sst-move-tenant-{i:04}"),
                    policy: serde_json::json!({ "note": "padding to clear the SST size threshold" }),
                })
                .await
            })
            .await
            .unwrap();
        ids.push(tenant.tenant_id);
    }
    (runtime, ids)
}

#[tokio::test]
#[serial]
async fn sst_movement_round_trips_a_whole_tablet_when_enabled() {
    std::env::set_var("AGENTDB_SST_MOVEMENT", "1");
    std::env::set_var("AGENTDB_SST_MOVEMENT_MIN_BYTES", "1");

    let source_dir = TempDir::new().unwrap();
    let (source_runtime, ids) = source_runtime_with_tenants(&source_dir, 200).await;

    // Export takes the SST fast path: the payload is an `ADSS` frame.
    let payload = source_runtime.export_tablet_snapshot_payload(7).unwrap();

    // Read the flag only around export; clear it before install so the value
    // cannot influence unrelated later work in this process.
    std::env::remove_var("AGENTDB_SST_MOVEMENT");
    std::env::remove_var("AGENTDB_SST_MOVEMENT_MIN_BYTES");

    assert!(
        is_sst_snapshot_payload(&payload),
        "expected an SST movement payload with the flag enabled and threshold at 1 byte"
    );

    // Install into a fresh target runtime (whole-tablet, same tablet id).
    let target_dir = TempDir::new().unwrap();
    let target_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
    let target_primary: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_tablet: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_logs = BTreeMap::from([
        (RUNTIME_PRIMARY_TABLET_ID, target_primary),
        (7, target_tablet),
    ]);
    let target_runtime =
        Runtime::with_kv_and_replicated_logs(target_kv.clone(), content(), target_logs.clone());

    assert!(target_runtime
        .install_tablet_snapshot_payload_and_rehydrate(7, &payload)
        .await
        .unwrap());

    // Every seeded tenant is readable on the target after the SST install.
    for tenant_id in [ids.first().unwrap(), ids.last().unwrap()] {
        let read_runtime = target_runtime.clone();
        let want = tenant_id.clone();
        let hydrated = target_runtime
            .with_tablet_context(7, async move { read_runtime.get_tenant(&want).await })
            .await
            .unwrap();
        assert_eq!(&hydrated.tenant_id, tenant_id);
    }

    // Survives a restart from the same store (durable, not just in-memory).
    let restarted = Runtime::with_kv_and_replicated_logs(target_kv, content(), target_logs);
    let read_runtime = restarted.clone();
    let want = ids.last().unwrap().clone();
    let hydrated = restarted
        .with_tablet_context(7, async move { read_runtime.get_tenant(&want).await })
        .await
        .unwrap();
    assert_eq!(&hydrated.tenant_id, ids.last().unwrap());
}

#[tokio::test]
#[serial]
async fn install_refuses_when_disk_reservation_would_be_breached() {
    std::env::set_var("AGENTDB_SST_MOVEMENT", "1");
    std::env::set_var("AGENTDB_SST_MOVEMENT_MIN_BYTES", "1");

    let source_dir = TempDir::new().unwrap();
    let (source_runtime, _ids) = source_runtime_with_tenants(&source_dir, 200).await;
    let payload = source_runtime.export_tablet_snapshot_payload(7).unwrap();
    std::env::remove_var("AGENTDB_SST_MOVEMENT");
    std::env::remove_var("AGENTDB_SST_MOVEMENT_MIN_BYTES");
    assert!(is_sst_snapshot_payload(&payload));

    // An impossibly large reservation forces the install to fail fast rather
    // than consume the disk down into the reserve (the doom-loop guard).
    std::env::set_var("AGENTDB_MOVEMENT_DISK_RESERVE_BYTES", &u64::MAX.to_string());

    let target_dir = TempDir::new().unwrap();
    let target_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
    let target_primary: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_tablet: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_runtime = Runtime::with_kv_and_replicated_logs(
        target_kv,
        content(),
        BTreeMap::from([
            (RUNTIME_PRIMARY_TABLET_ID, target_primary),
            (7, target_tablet),
        ]),
    );

    let result = target_runtime
        .install_tablet_snapshot_payload_and_rehydrate(7, &payload)
        .await;
    std::env::remove_var("AGENTDB_MOVEMENT_DISK_RESERVE_BYTES");

    let err = result.expect_err("install must refuse under an unsatisfiable disk reservation");
    assert!(
        err.message.contains("reserve") || err.message.contains("add disk"),
        "unexpected error: {}",
        err.message
    );
}
