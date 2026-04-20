mod kv_contract;

use agentdb_storage::SlateKvStore;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[test]
fn slate_satisfies_kv_contract() {
    // We need an outer runtime to drive `SlateKvStore::open` (async). The
    // test itself is `#[test]` (not `#[tokio::test]`) so `block_on` is safe
    // — we're not already inside a tokio runtime context. `SlateKvStore`
    // owns its own internal runtime for sync-trait dispatches, so per-call
    // ops (`put`, `get`, `scan_prefix`, `snapshot`) run without touching
    // `outer` after construction.
    let outer = Runtime::new().unwrap();

    let make_kv = || -> Arc<SlateKvStore> {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let kv = outer
            .block_on(SlateKvStore::open(store, "agentdb/slate"))
            .unwrap();
        // Intentionally leak the TempDir so its path stays valid for the
        // full contract run. Process exit cleans up.
        Box::leak(Box::new(dir));
        Arc::new(kv)
    };
    kv_contract::run_contract(make_kv);
}
