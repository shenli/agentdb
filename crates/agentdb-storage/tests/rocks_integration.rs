use agentdb_storage::{encode_key, BoxId, KvStore, Namespace, RocksKvStore, TenantId};
use tempfile::TempDir;

fn tmp() -> TempDir {
    TempDir::new().expect("tempdir")
}

#[test]
fn reopens_and_reads_previously_written_data() {
    let dir = tmp();
    let path = dir.path().to_path_buf();

    {
        let store = RocksKvStore::open(&path).expect("open 1");
        let key = encode_key(Namespace::Events, 1 as BoxId, 2 as TenantId, 3, 0, 1);
        store.put(&key, b"hello").unwrap();
    } // drop

    let store = RocksKvStore::open(&path).expect("reopen");
    let key = encode_key(Namespace::Events, 1, 2, 3, 0, 1);
    let got = store.get(&key).unwrap();
    assert_eq!(got.as_deref(), Some(b"hello".as_ref()));
}

#[test]
fn delete_removes_a_key() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let k = encode_key(Namespace::Events, 1, 2, 3, 0, 1);
    store.put(&k, b"v").unwrap();
    assert_eq!(store.get(&k).unwrap().as_deref(), Some(b"v".as_ref()));
    store.delete(&k).unwrap();
    assert_eq!(store.get(&k).unwrap(), None);
}

#[test]
fn delete_of_missing_key_is_ok() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let k = encode_key(Namespace::Events, 1, 2, 3, 0, 1);
    store.delete(&k).unwrap(); // must not error
}
