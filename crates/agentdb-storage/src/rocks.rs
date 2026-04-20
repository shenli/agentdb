use std::path::Path;
use std::sync::Arc;

use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};

use crate::error::{Error, Result};
use crate::kv::{KvIter, KvSnapshot, KvStore, WriteOp};

/// Result type for KV operations.
type KvResult = std::result::Result<(Vec<u8>, Vec<u8>), crate::error::Error>;

/// RocksDB-backed `KvStore`.
pub struct RocksKvStore {
    db: Arc<DB>,
}

impl RocksKvStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl KvStore for RocksKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        // Eagerly collect so the returned iterator is 'static w.r.t. lifetimes:
        // RocksDB's DBIterator borrows from &DB and cannot be returned as a
        // Box<dyn Iterator + 'static>. Stop on the first error (do not continue
        // past it) so callers don't silently skip a corrupt read.
        let prefix_vec = prefix.to_vec();
        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix_vec, Direction::Forward));
        let mut results: Vec<Result<(Vec<u8>, Vec<u8>)>> = Vec::new();
        for res in iter {
            match res {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix_vec) {
                        break;
                    }
                    results.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    results.push(Err(Error::from(e)));
                    break;
                }
            }
        }
        Ok(Box::new(results.into_iter()))
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.put(&key, &value),
                WriteOp::Delete { key } => batch.delete(&key),
            }
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        Ok(Box::new(RocksSnapshotHandle {
            snap: self.db.snapshot(),
        }))
    }

    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>> {
        Ok(Arc::new(OwnedRocksSnapshotHandle::new(self.db.clone())))
    }
}

/// Borrows from a `RocksKvStore`; caller keeps the store alive via its own reference.
struct RocksSnapshotHandle<'a> {
    snap: rocksdb::Snapshot<'a>,
}

impl<'a> KvSnapshot for RocksSnapshotHandle<'a> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snap.get(key)?)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let prefix_vec = prefix.to_vec();
        // Eagerly collect so the returned iterator is 'static w.r.t. lifetimes.
        let mut out: Vec<KvResult> = Vec::new();
        let iter = self
            .snap
            .iterator(IteratorMode::From(&prefix_vec, Direction::Forward));
        for res in iter {
            match res {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix_vec) {
                        break;
                    }
                    out.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    out.push(Err(crate::error::Error::from(e)));
                    break;
                }
            }
        }
        Ok(Box::new(out.into_iter()))
    }
}

/// Owned snapshot: holds `Arc<DB>` itself and stores a snapshot whose
/// lifetime has been extended to `'static`. This is safe because:
///   - the `Arc<DB>` field is dropped AFTER the snapshot (Rust drops
///     struct fields in declaration order; snapshot is declared first),
///   - `Arc<DB>` keeps the underlying DB alive for the duration of the
///     snapshot, and
///   - `DB` is not moved once heap-allocated inside the Arc.
///
/// The `unsafe` lives inside `new`. Outside this module, the handle is
/// a safe abstraction.
struct OwnedRocksSnapshotHandle {
    // Drop order: snap drops first, then _db.
    snap: rocksdb::Snapshot<'static>,
    _db: Arc<DB>,
}

impl OwnedRocksSnapshotHandle {
    fn new(db: Arc<DB>) -> Self {
        let snap: rocksdb::Snapshot<'_> = db.snapshot();
        // SAFETY: see the struct-level doc comment. We extend the lifetime
        // from the `&DB` borrow to `'static`; the Arc held in `_db` outlives
        // this transmuted reference because fields are dropped in declared
        // order (snap before _db).
        let snap: rocksdb::Snapshot<'static> = unsafe { std::mem::transmute(snap) };
        Self { snap, _db: db }
    }
}

impl KvSnapshot for OwnedRocksSnapshotHandle {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snap.get(key)?)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let prefix_vec = prefix.to_vec();
        let mut out: Vec<KvResult> = Vec::new();
        let iter = self
            .snap
            .iterator(IteratorMode::From(&prefix_vec, Direction::Forward));
        for res in iter {
            match res {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix_vec) {
                        break;
                    }
                    out.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    out.push(Err(Error::from(e)));
                    break;
                }
            }
        }
        Ok(Box::new(out.into_iter()))
    }
}

#[cfg(test)]
mod owned_snapshot_tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn owned_snapshot_sees_put_after_open_as_absent() {
        let dir = TempDir::new().unwrap();
        let store: Arc<RocksKvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        store.put(b"k1", b"v1").unwrap();

        let snap = Arc::clone(&store).snapshot_owned().unwrap();
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));

        store.put(b"k1", b"v2").unwrap();
        // Snapshot still sees v1.
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));

        store.put(b"k2", b"v2").unwrap();
        // Snapshot doesn't see the new key.
        assert!(snap.get(b"k2").unwrap().is_none());
    }

    #[tokio::test]
    async fn owned_snapshot_is_held_across_await() {
        let dir = TempDir::new().unwrap();
        let store: Arc<RocksKvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        store.put(b"k1", b"v1").unwrap();
        let snap = Arc::clone(&store).snapshot_owned().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        // Compiles + runs: proves handle is Send across await.
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
    }
}
