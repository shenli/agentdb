//! Backend-agnostic `KvStore` contract tests. Used by rocks_integration.rs
//! and slate_integration.rs to exercise identical behavior. Tests that rely
//! on backend-specific semantics (e.g. RocksDB's reopen-and-read) stay in
//! their dedicated integration file.
//!
//! Keys are constructed inline as plain byte strings. The contract tests
//! exercise `KvStore` behavior (put/get/delete/scan_prefix/write_batch/
//! snapshot) which only cares that keys are `&[u8]` — not what the encoding
//! is. Format here:
//!
//! ```text
//! "<ns>/<box>/<tenant>/<session>/<branch>/<seq>"
//! ```
//!
//! with `<seq>` zero-padded to 20 digits so lexicographic order matches
//! numeric order, mirroring the production `keys::event_key` shape.

use agentdb_storage::{KvStore, WriteOp};
use std::sync::Arc;

fn k(ns: &str, box_id: u16, tenant: u64, session: u64, branch: u32, seq: u64) -> Vec<u8> {
    format!("{ns}/{box_id}/{tenant}/{session}/{branch}/{seq:020}").into_bytes()
}

fn prefix_box(ns: &str, box_id: u16) -> Vec<u8> {
    format!("{ns}/{box_id}/").into_bytes()
}

fn prefix_tenant(ns: &str, box_id: u16, tenant: u64) -> Vec<u8> {
    format!("{ns}/{box_id}/{tenant}/").into_bytes()
}

fn prefix_session(ns: &str, box_id: u16, tenant: u64, session: u64) -> Vec<u8> {
    format!("{ns}/{box_id}/{tenant}/{session}/").into_bytes()
}

/// Run every KV contract assertion against a fresh store produced by `make_kv`.
/// `make_kv` is invoked once per assertion so tests are order-independent.
pub fn run_contract<F, K>(make_kv: F)
where
    F: Fn() -> Arc<K>,
    K: KvStore + 'static,
{
    delete_removes_a_key(&make_kv());
    delete_of_missing_key_is_ok(&make_kv());
    scan_prefix_returns_only_session_keys_in_order(&make_kv());
    scan_prefix_does_not_leak_across_namespaces(&make_kv());
    scan_prefix_on_empty_store_returns_no_rows(&make_kv());
    write_batch_applies_mixed_put_and_delete_atomically(&make_kv());
    empty_write_batch_is_a_noop(&make_kv());
    snapshot_does_not_observe_writes_that_happened_after_it(&make_kv());
    snapshot_scan_prefix_is_consistent(&make_kv());
    end_to_end_locality_slices_line_up(&make_kv());
}

fn delete_removes_a_key<K: KvStore>(store: &Arc<K>) {
    let key = k("event", 1, 2, 3, 0, 1);
    store.put(&key, b"v").unwrap();
    assert_eq!(store.get(&key).unwrap().as_deref(), Some(b"v".as_ref()));
    store.delete(&key).unwrap();
    assert_eq!(store.get(&key).unwrap(), None);
}

fn delete_of_missing_key_is_ok<K: KvStore>(store: &Arc<K>) {
    let key = k("event", 1, 2, 3, 0, 1);
    store.delete(&key).unwrap();
}

fn scan_prefix_returns_only_session_keys_in_order<K: KvStore>(store: &Arc<K>) {
    for seq in 1..=3 {
        let key = k("event", 1, 10, 1, 0, seq);
        store.put(&key, format!("s1-{seq}").as_bytes()).unwrap();
    }
    for seq in 1..=3 {
        let key = k("event", 1, 10, 2, 0, seq);
        store.put(&key, format!("s2-{seq}").as_bytes()).unwrap();
    }
    for seq in 1..=3 {
        let key = k("event", 1, 10, 3, 0, seq);
        store.put(&key, format!("s3-{seq}").as_bytes()).unwrap();
    }
    let prefix = prefix_session("event", 1, 10, 2);
    let got: Vec<_> = store
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(
        got,
        vec![b"s2-1".to_vec(), b"s2-2".to_vec(), b"s2-3".to_vec()]
    );
}

fn scan_prefix_does_not_leak_across_namespaces<K: KvStore>(store: &Arc<K>) {
    let k_event = k("event", 1, 10, 1, 0, 1);
    let k_state = k("state", 1, 10, 1, 0, 1);
    store.put(&k_event, b"event").unwrap();
    store.put(&k_state, b"state").unwrap();
    let prefix = prefix_tenant("event", 1, 10);
    let got: Vec<_> = store
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(got, vec![b"event".to_vec()]);
}

fn scan_prefix_on_empty_store_returns_no_rows<K: KvStore>(store: &Arc<K>) {
    let prefix = prefix_tenant("event", 1, 10);
    assert_eq!(store.scan_prefix(&prefix).unwrap().count(), 0);
}

fn write_batch_applies_mixed_put_and_delete_atomically<K: KvStore>(store: &Arc<K>) {
    let k_keep = k("event", 1, 10, 1, 0, 1);
    let k_drop = k("event", 1, 10, 1, 0, 2);
    store.put(&k_keep, b"pre-keep").unwrap();
    store.put(&k_drop, b"pre-drop").unwrap();
    let k_new = k("event", 1, 10, 1, 0, 3);
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

fn empty_write_batch_is_a_noop<K: KvStore>(store: &Arc<K>) {
    store.write_batch(Vec::new()).unwrap();
}

fn snapshot_does_not_observe_writes_that_happened_after_it<K: KvStore>(store: &Arc<K>) {
    let key = k("event", 1, 10, 1, 0, 1);
    store.put(&key, b"v1").unwrap();
    let snap = store.snapshot().unwrap();
    store.put(&key, b"v2").unwrap();
    assert_eq!(snap.get(&key).unwrap().as_deref(), Some(b"v1".as_ref()));
    assert_eq!(store.get(&key).unwrap().as_deref(), Some(b"v2".as_ref()));
}

fn snapshot_scan_prefix_is_consistent<K: KvStore>(store: &Arc<K>) {
    for seq in 1..=3 {
        let key = k("event", 1, 10, 1, 0, seq);
        store.put(&key, format!("v{seq}").as_bytes()).unwrap();
    }
    let snap = store.snapshot().unwrap();
    let k4 = k("event", 1, 10, 1, 0, 4);
    store.put(&k4, b"v4").unwrap();
    let prefix = prefix_session("event", 1, 10, 1);
    let snap_values: Vec<_> = snap
        .scan_prefix(&prefix)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(
        snap_values,
        vec![b"v1".to_vec(), b"v2".to_vec(), b"v3".to_vec()]
    );
}

fn end_to_end_locality_slices_line_up<K: KvStore>(store: &Arc<K>) {
    let box_id: u16 = 7;
    let tenant_a: u64 = 100;
    let tenant_b: u64 = 200;
    let plan = [(tenant_a, 1u64), (tenant_a, 2u64), (tenant_b, 1u64)];
    for (tenant, session) in plan {
        for seq in 1..=10u64 {
            let key = k("event", box_id, tenant, session, 0, seq);
            store
                .put(&key, format!("t{tenant}-s{session}-e{seq}").as_bytes())
                .unwrap();
        }
        let sk = k("state", box_id, tenant, session, 0, 0);
        store
            .put(&sk, format!("state-t{tenant}-s{session}").as_bytes())
            .unwrap();
    }
    let p = prefix_session("event", box_id, tenant_a, 1);
    let rows: Vec<_> = store
        .scan_prefix(&p)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0], b"t100-s1-e1".to_vec());
    assert_eq!(rows[9], b"t100-s1-e10".to_vec());
    let p = prefix_tenant("event", box_id, tenant_a);
    assert_eq!(store.scan_prefix(&p).unwrap().count(), 20);
    let p = prefix_box("event", box_id);
    assert_eq!(store.scan_prefix(&p).unwrap().count(), 30);
    let p = prefix_box("state", box_id);
    let rows: Vec<_> = store
        .scan_prefix(&p)
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert!(std::str::from_utf8(row).unwrap().starts_with("state-"));
    }
}
