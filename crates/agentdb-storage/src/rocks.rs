use std::path::Path;
use std::sync::Arc;

use rocksdb::{Direction, IteratorMode, Options, DB};

use crate::error::{Error, Result};
use crate::kv::{KvIter, KvSnapshot, KvStore, WriteOp};

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
        let prefix_vec = prefix.to_vec();
        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix_vec, Direction::Forward));

        let results: Vec<Result<(Vec<u8>, Vec<u8>)>> = iter
            .take_while(|res| match res {
                Ok((k, _)) => k.starts_with(&prefix_vec),
                Err(_) => true,
            })
            .map(|res| {
                res.map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .map_err(Error::from)
            })
            .collect();

        Ok(Box::new(results.into_iter()))
    }

    fn write_batch(&self, _ops: Vec<WriteOp>) -> Result<()> {
        unimplemented!("write_batch — Task 11")
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        unimplemented!("snapshot — Task 12")
    }
}
