use std::path::Path;
use std::sync::Arc;

use rocksdb::{Options, DB};

use crate::error::Result;
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

    fn delete(&self, _key: &[u8]) -> Result<()> {
        unimplemented!("delete — Task 9")
    }

    fn scan_prefix(&self, _prefix: &[u8]) -> Result<KvIter> {
        unimplemented!("scan_prefix — Task 10")
    }

    fn write_batch(&self, _ops: Vec<WriteOp>) -> Result<()> {
        unimplemented!("write_batch — Task 11")
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        unimplemented!("snapshot — Task 12")
    }
}
