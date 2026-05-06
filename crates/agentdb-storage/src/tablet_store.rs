//! Tablet-local applied-state namespace over a `KvStore`.
//!
//! This keeps the current KV abstraction but stops new tablet-state code from
//! writing into one global keyspace. Each tablet gets an independent logical
//! prefix, which can later map to a separate RocksDB instance, column family, or
//! another tablet-local LSM implementation.

use std::sync::Arc;

use crate::{Error, KvIter, KvStore, Result, WriteOp};

const TABLET_STATE_PREFIX: &[u8] = b"tablet_state/";

#[derive(Clone)]
pub struct TabletKvStore {
    kv: Arc<dyn KvStore + Send + Sync>,
    tablet_id: i64,
    prefix: Vec<u8>,
}

impl TabletKvStore {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>, tablet_id: i64) -> Result<Self> {
        if tablet_id < 0 {
            return Err(Error::Other("tablet_id must be non-negative".into()));
        }
        Ok(Self {
            kv,
            tablet_id,
            prefix: tablet_prefix(tablet_id),
        })
    }

    pub fn tablet_id(&self) -> i64 {
        self.tablet_id
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get(&self.key(key))
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.kv.put(&self.key(key), value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.kv.delete(&self.key(key))
    }

    pub fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        let scoped = ops
            .into_iter()
            .map(|op| match op {
                WriteOp::Put { key, value } => WriteOp::Put {
                    key: self.key(&key),
                    value,
                },
                WriteOp::Delete { key } => WriteOp::Delete {
                    key: self.key(&key),
                },
            })
            .collect();
        self.kv.write_batch(scoped)
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let scoped_prefix = self.key(prefix);
        let tablet_prefix = self.prefix.clone();
        let iter = self.kv.scan_prefix(&scoped_prefix)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| {
                let logical = key
                    .strip_prefix(tablet_prefix.as_slice())
                    .map_or_else(|| key.clone(), |suffix| suffix.to_vec());
                (logical, value)
            })
        })))
    }

    fn key(&self, key: &[u8]) -> Vec<u8> {
        let mut scoped = self.prefix.clone();
        scoped.extend_from_slice(key);
        scoped
    }
}

fn tablet_prefix(tablet_id: i64) -> Vec<u8> {
    format!(
        "{}{:020}/",
        String::from_utf8_lossy(TABLET_STATE_PREFIX),
        tablet_id
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksKvStore;

    #[test]
    fn tablet_store_scopes_get_put_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let tablet_a = TabletKvStore::new(kv.clone(), 7).unwrap();
        let tablet_b = TabletKvStore::new(kv, 8).unwrap();

        tablet_a.put(b"session/s1", b"a").unwrap();
        tablet_b.put(b"session/s1", b"b").unwrap();

        assert_eq!(
            tablet_a.get(b"session/s1").unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(
            tablet_b.get(b"session/s1").unwrap().as_deref(),
            Some(&b"b"[..])
        );

        tablet_a.delete(b"session/s1").unwrap();
        assert!(tablet_a.get(b"session/s1").unwrap().is_none());
        assert_eq!(
            tablet_b.get(b"session/s1").unwrap().as_deref(),
            Some(&b"b"[..])
        );
    }

    #[test]
    fn tablet_store_batch_and_scan_are_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let tablet = TabletKvStore::new(kv, 7).unwrap();

        tablet
            .write_batch(vec![
                WriteOp::Put {
                    key: b"record/a".to_vec(),
                    value: b"a".to_vec(),
                },
                WriteOp::Put {
                    key: b"record/b".to_vec(),
                    value: b"b".to_vec(),
                },
            ])
            .unwrap();

        let rows: Vec<_> = tablet
            .scan_prefix(b"record/")
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (b"record/a".to_vec(), b"a".to_vec()),
                (b"record/b".to_vec(), b"b".to_vec())
            ]
        );
    }
}
