mod kv_contract;

use agentdb_storage::{KvStore, RocksKvStore};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn rocks_satisfies_kv_contract() {
    // Each `make_kv` call gets a fresh tempdir + fresh store so contract
    // assertions are order-independent. TempDir is leaked intentionally to
    // keep paths live for the whole contract run; the process-exit cleanup
    // handles removal.
    let make_kv = || -> Arc<RocksKvStore> {
        let dir = TempDir::new().unwrap();
        let store = RocksKvStore::open(dir.path()).unwrap();
        Box::leak(Box::new(dir));
        Arc::new(store)
    };
    kv_contract::run_contract(make_kv);
}

#[test]
fn rocks_reopens_and_reads_previously_written_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let key = b"event/1/2/3/0/00000000000000000001".to_vec();
    {
        let store = RocksKvStore::open(&path).expect("open 1");
        store.put(&key, b"hello").unwrap();
    }
    let store = RocksKvStore::open(&path).expect("reopen");
    let got = store.get(&key).unwrap();
    assert_eq!(got.as_deref(), Some(b"hello".as_ref()));
}
