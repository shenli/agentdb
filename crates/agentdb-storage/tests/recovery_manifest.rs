//! End-to-end recovery-root exercise on the real backends: export the full
//! KV state from RocksDB, ship it through a filesystem-backed object store,
//! and rebuild a fresh RocksDB directory from the bucket alone.

use std::sync::Arc;

use agentdb_storage::{
    export_recovery_manifest, import_recovery_manifest, read_recovery_manifest_header,
    KvStore, RecoveryObjectStore, RocksKvStore,
};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

#[tokio::test]
async fn rocksdb_roundtrip_through_object_store() {
    let src_dir = TempDir::new().unwrap();
    let src: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(src_dir.path()).unwrap());
    for i in 0..2_500u32 {
        src.put(
            format!("event/ten_1/ses_1/{i:06}").as_bytes(),
            format!("{{\"payload\":{i}}}").as_bytes(),
        )
        .unwrap();
    }
    src.put(b"tenant/ten_1", b"{\"name\":\"acme\"}").unwrap();

    let bytes =
        export_recovery_manifest(src.clone(), Some("node-a"), 1_000, u64::MAX).unwrap();
    let header = read_recovery_manifest_header(&bytes).unwrap();
    assert_eq!(header.entry_count, 2_501);

    let bucket_dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(bucket_dir.path()).unwrap());
    let recovery = RecoveryObjectStore::new(store, "agentdb");
    recovery.put_latest(&bytes, 1_000).await.unwrap();

    // Rebuild from the bucket alone.
    let fetched = recovery.get_latest().await.unwrap();
    let dst_dir = TempDir::new().unwrap();
    let dst = RocksKvStore::open(dst_dir.path()).unwrap();
    let stats = import_recovery_manifest(&dst, &fetched).unwrap();
    assert_eq!(stats.entries, 2_501);

    let src_rows: Vec<_> = src.scan_prefix(b"").unwrap().map(|r| r.unwrap()).collect();
    let dst_rows: Vec<_> = dst.scan_prefix(b"").unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(src_rows.len(), 2_501);
    assert_eq!(src_rows, dst_rows);
}
