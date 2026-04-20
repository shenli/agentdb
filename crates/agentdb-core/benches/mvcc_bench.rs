use std::sync::Arc;

use agentdb_core::{
    append_and_fetch, scan_session_events, BoxRegistry, ReadCtx, SessionRegistry, TenantRegistry,
};
use agentdb_storage::{KvStore, RocksKvStore};
use criterion::{criterion_group, criterion_main, Criterion};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn bench_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (_kv_dir, _obj_dir, _kv, reg, tenants, sessions) = rt.block_on(async {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        for i in 1..=1000u64 {
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({ "i": i }),
                0,
                i,
            )
            .await
            .unwrap();
        }
        (kv_dir, obj_dir, kv, reg, tenants, sessions)
    });
    let box_ = reg.route_tenant("t-1");
    let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
    let session = sessions
        .lookup(box_.id, tenant.internal_id, "s-1")
        .unwrap()
        .unwrap();

    c.bench_function("scan_1000_events_via_ctx", |b| {
        b.iter(|| {
            let ctx = ReadCtx::open(&box_, reg.kv()).unwrap();
            let v = scan_session_events(&ctx, &session).unwrap();
            assert_eq!(v.len(), 1000);
        });
    });
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
