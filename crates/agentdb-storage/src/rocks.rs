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
