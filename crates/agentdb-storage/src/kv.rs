use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

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
    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let start = start.to_vec();
        let iter = self.scan_prefix(prefix)?;
        Ok(Box::new(iter.filter(move |item| {
            item.as_ref()
                .map(|(key, _)| key.as_slice() >= start.as_slice())
                .unwrap_or(true)
        })))
    }
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
    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let start = start.to_vec();
        let iter = self.scan_prefix(prefix)?;
        Ok(Box::new(iter.filter(move |item| {
            item.as_ref()
                .map(|(key, _)| key.as_slice() >= start.as_slice())
                .unwrap_or(true)
        })))
    }
    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()>;
    /// Snapshot borrows from the store, so callers keep the store alive
    /// (e.g. `Arc<dyn KvStore>`) for the snapshot's lifetime.
    ///
    /// The returned handle is **not safe to hold across an async `.await`
    /// point**: its lifetime borrows from `&self`, so await-yielding while
    /// the handle is live will fail the borrow checker. Inside async code,
    /// resolve the snapshot into owned data (e.g. collect a `scan_prefix`
    /// into a `Vec`) before yielding.
    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>>;

    /// Like `snapshot`, but the returned handle owns its DB reference and
    /// is therefore `'static`, so it is safe to hold across an `.await`
    /// point. Intended for `ReadCtx` and other cross-await readers.
    ///
    /// Takes `Arc<Self>` to ensure the store outlives the snapshot.
    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>>;

    /// Write a point-in-time snapshot of the entire KV state to
    /// `out_dir`. `out_dir` MUST NOT exist — backends create it,
    /// so accidentally overwriting an existing path is an error,
    /// not silent corruption.
    ///
    /// Semantics per backend:
    ///   - RocksDB: uses the native `Checkpoint` API, which
    ///     hard-links SSTs where possible (instant, near-zero IO)
    ///     and copies WAL/MANIFEST. Safe to run concurrent with
    ///     live writes.
    ///   - Others: return an error unless explicitly supported.
    ///
    /// The returned `Ok` shape carries nothing — callers can
    /// `fs::metadata(out_dir)` to measure on-disk size if they
    /// care.
    fn checkpoint(&self, out_dir: &Path) -> Result<()> {
        let _ = out_dir;
        Err(crate::error::Error::Other(
            "checkpoint not supported by this KvStore backend".into(),
        ))
    }
}

#[derive(Default)]
pub struct InMemoryKvStore {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl InMemoryKvStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl KvStore for InMemoryKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .rows
            .lock()
            .expect("in-memory kv lock poisoned")
            .get(key)
            .cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.rows
            .lock()
            .expect("in-memory kv lock poisoned")
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.rows
            .lock()
            .expect("in-memory kv lock poisoned")
            .remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let rows = self
            .rows
            .lock()
            .expect("in-memory kv lock poisoned")
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let rows = self
            .rows
            .lock()
            .expect("in-memory kv lock poisoned")
            .iter()
            .filter(|(key, _)| key.starts_with(prefix) && key.as_slice() >= start)
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        let mut rows = self.rows.lock().expect("in-memory kv lock poisoned");
        for op in ops {
            match op {
                WriteOp::Put { key, value } => {
                    rows.insert(key, value);
                }
                WriteOp::Delete { key } => {
                    rows.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        Ok(Box::new(InMemoryKvSnapshot {
            rows: self
                .rows
                .lock()
                .expect("in-memory kv lock poisoned")
                .clone(),
        }))
    }

    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>> {
        Ok(Arc::new(InMemoryKvSnapshot {
            rows: self
                .rows
                .lock()
                .expect("in-memory kv lock poisoned")
                .clone(),
        }))
    }
}

struct InMemoryKvSnapshot {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvSnapshot for InMemoryKvSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.rows.get(key).cloned())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let rows = self
            .rows
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let rows = self
            .rows
            .iter()
            .filter(|(key, _)| key.starts_with(prefix) && key.as_slice() >= start)
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }
}
