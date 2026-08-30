//! End-to-end recovery-root exercise on the real backends: export the full
//! KV state from RocksDB, ship it through a filesystem-backed object store,
//! and rebuild a fresh RocksDB directory from the bucket alone.

use std::sync::Arc;

use agentdb_storage::{
    export_recovery_manifest, import_recovery_manifest, read_recovery_manifest_header,
    InMemoryKvStore, KvStore, RecoveryObjectStore, RocksKvStore,
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

mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Any set of binary KV pairs — arbitrary bytes in keys and values,
        /// including empty values and non-UTF-8 keys — survives the
        /// export → import roundtrip exactly.
        #[test]
        fn manifest_roundtrip_preserves_arbitrary_binary_pairs(
            pairs in proptest::collection::btree_map(
                proptest::collection::vec(any::<u8>(), 1..64),
                proptest::collection::vec(any::<u8>(), 0..256),
                0..40,
            )
        ) {
            let src = InMemoryKvStore::default();
            for (k, v) in &pairs {
                src.put(k, v).unwrap();
            }
            let src: Arc<dyn KvStore + Send + Sync> = Arc::new(src);
            let bytes = export_recovery_manifest(src, None, 7, u64::MAX).unwrap();
            let header = read_recovery_manifest_header(&bytes).unwrap();
            prop_assert_eq!(header.entry_count, pairs.len() as u64);

            let dst = InMemoryKvStore::default();
            import_recovery_manifest(&dst, &bytes).unwrap();
            let restored: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = dst
                .scan_prefix(b"")
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            prop_assert_eq!(restored, pairs);
        }

        /// Cutting the manifest at any byte offset must never import
        /// successfully with fewer entries than the header declares —
        /// truncation is an error, not silent partial recovery.
        #[test]
        fn truncated_manifest_never_imports_partially(
            cut_fraction in 0.0f64..1.0f64,
        ) {
            let src = InMemoryKvStore::default();
            for i in 0..20u32 {
                src.put(format!("key-{i:04}").as_bytes(), &i.to_be_bytes()).unwrap();
            }
            let src: Arc<dyn KvStore + Send + Sync> = Arc::new(src);
            let bytes = export_recovery_manifest(src, None, 7, u64::MAX).unwrap();
            let cut = ((bytes.len() as f64) * cut_fraction) as usize;
            let truncated = &bytes[..cut];

            let dst = InMemoryKvStore::default();
            match import_recovery_manifest(&dst, truncated) {
                Ok(stats) => prop_assert_eq!(stats.entries, 20, "full import only"),
                Err(_) => {
                    // Refused: acceptable, and the target must not be left
                    // half-imported *silently* — a partial import that
                    // errored is visible to the caller, which is the
                    // contract (restore into a fresh dir, retry on error).
                }
            }
        }
    }
}
