use crate::error::Result;

/// A single operation in a write batch.
#[derive(Debug, Clone)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Iterator yielded by `scan_prefix`.
///
/// Items are owned `(key, value)` pairs so callers do not have to
/// reason about borrow lifetimes against the backing store.
pub type KvIter = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

/// A point-in-time read view over a `KvStore`.
pub trait KvSnapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter>;
}

/// Minimum surface the rest of AgentDB depends on.
///
/// Additional backends (e.g. SlateDB) can implement this trait to be
/// drop-in swappable. The key-shape contract (see `key.rs`) is assumed.
pub trait KvStore: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter>;
    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()>;
    /// Snapshot borrows from the store, so callers keep the store alive
    /// (e.g. `Arc<dyn KvStore>`) for the snapshot's lifetime.
    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>>;
}
