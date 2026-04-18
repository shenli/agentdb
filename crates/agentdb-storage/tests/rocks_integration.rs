use agentdb_storage::{encode_key, BoxId, KvStore, Namespace, RocksKvStore, TenantId};
use agentdb_storage::{prefix_for_session, prefix_for_tenant, WriteOp};
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

#[test]
fn scan_prefix_returns_only_session_keys_in_order() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    // Three sessions for the same tenant; session 2 is the one we query.
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 1, 0, seq);
        store.put(&k, format!("s1-{seq}").as_bytes()).unwrap();
    }
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 2, 0, seq);
        store.put(&k, format!("s2-{seq}").as_bytes()).unwrap();
    }
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 3, 0, seq);
        store.put(&k, format!("s3-{seq}").as_bytes()).unwrap();
    }

    let prefix = prefix_for_session(Namespace::Events, 1, 10, 2);
    let mut iter = store.scan_prefix(&prefix).unwrap();
    let got: Vec<_> = iter.by_ref().map(|r| r.unwrap().1).collect();
    assert_eq!(
        got,
        vec![b"s2-1".to_vec(), b"s2-2".to_vec(), b"s2-3".to_vec()],
    );
}

#[test]
fn scan_prefix_does_not_leak_across_namespaces() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    let k_event = encode_key(Namespace::Events, 1, 10, 1, 0, 1);
    let k_state = encode_key(Namespace::State, 1, 10, 1, 0, 1);
    store.put(&k_event, b"event").unwrap();
    store.put(&k_state, b"state").unwrap();

    let prefix = prefix_for_tenant(Namespace::Events, 1, 10);
    let got: Vec<_> = store
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(got, vec![b"event".to_vec()]);
}

#[test]
fn scan_prefix_on_empty_store_returns_no_rows() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let prefix = prefix_for_tenant(Namespace::Events, 1, 10);
    assert_eq!(store.scan_prefix(&prefix).unwrap().count(), 0);
}

#[test]
fn write_batch_applies_mixed_put_and_delete_atomically() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();

    let k_keep = encode_key(Namespace::Events, 1, 10, 1, 0, 1);
    let k_drop = encode_key(Namespace::Events, 1, 10, 1, 0, 2);
    store.put(&k_keep, b"pre-keep").unwrap();
    store.put(&k_drop, b"pre-drop").unwrap();

    let k_new = encode_key(Namespace::Events, 1, 10, 1, 0, 3);
    store
        .write_batch(vec![
            WriteOp::Put {
                key: k_keep.clone(),
                value: b"post-keep".to_vec(),
            },
            WriteOp::Delete {
                key: k_drop.clone(),
            },
            WriteOp::Put {
                key: k_new.clone(),
                value: b"new".to_vec(),
            },
        ])
        .unwrap();

    assert_eq!(
        store.get(&k_keep).unwrap().as_deref(),
        Some(b"post-keep".as_ref())
    );
    assert_eq!(store.get(&k_drop).unwrap(), None);
    assert_eq!(store.get(&k_new).unwrap().as_deref(), Some(b"new".as_ref()));
}

#[test]
fn empty_write_batch_is_a_noop() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    store.write_batch(Vec::new()).unwrap();
}

#[test]
fn snapshot_does_not_observe_writes_that_happened_after_it() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    let k = encode_key(Namespace::Events, 1, 10, 1, 0, 1);
    store.put(&k, b"v1").unwrap();

    let snap = store.snapshot().unwrap();
    store.put(&k, b"v2").unwrap();

    assert_eq!(snap.get(&k).unwrap().as_deref(), Some(b"v1".as_ref()));
    assert_eq!(store.get(&k).unwrap().as_deref(), Some(b"v2".as_ref()));
}

#[test]
fn snapshot_scan_prefix_is_consistent() {
    let dir = tmp();
    let store = RocksKvStore::open(dir.path()).unwrap();
    for seq in 1..=3 {
        let k = encode_key(Namespace::Events, 1, 10, 1, 0, seq);
        store.put(&k, format!("v{seq}").as_bytes()).unwrap();
    }

    let snap = store.snapshot().unwrap();
    // write a new one after snapshot
    let k4 = encode_key(Namespace::Events, 1, 10, 1, 0, 4);
    store.put(&k4, b"v4").unwrap();

    let prefix = prefix_for_session(Namespace::Events, 1, 10, 1);
    let snap_values: Vec<_> = snap
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(
        snap_values,
        vec![b"v1".to_vec(), b"v2".to_vec(), b"v3".to_vec()],
        "snapshot should not see the key written after it"
    );
}
