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

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KvStoreMetrics {
    pub live_data_size_bytes: Option<u64>,
    pub total_sst_file_size_bytes: Option<u64>,
    pub active_memtable_bytes: Option<u64>,
    pub all_memtables_bytes: Option<u64>,
    pub immutable_memtables: Option<u64>,
    pub level0_files: Option<u64>,
    pub pending_compaction_bytes: Option<u64>,
    pub compaction_pending: Option<bool>,
    pub memtable_flush_pending: Option<bool>,
    pub running_compactions: Option<u64>,
    pub running_flushes: Option<u64>,
    pub background_errors: Option<u64>,
    pub estimated_keys: Option<u64>,
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
/// Additional backends can implement this trait to be
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

    /// Return the largest (key, value) whose key starts with `prefix`, or
    /// `None` if the prefix is empty. Used for O(1) "tail" lookups (e.g. the
    /// max event seq in a session). The default forward-scans and keeps the
    /// last item (O(n)); backends should override with a reverse seek.
    fn last_kv_with_prefix(&self, prefix: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut last = None;
        for item in self.scan_prefix(prefix)? {
            last = Some(item?);
        }
        Ok(last)
    }

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

    /// Best-effort storage-engine metrics. Backends that do not expose native
    /// properties return `Ok(None)`.
    fn metrics(&self) -> Result<Option<KvStoreMetrics>> {
        Ok(None)
    }

    /// Best-effort manual compaction hook. Backends without a native compaction
    /// primitive may treat it as a no-op.
    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        let _ = (start, end);
        Ok(())
    }

    fn compact_all(&self) -> Result<()> {
        self.compact_range(None, None)
    }

    /// Export every KV pair under `prefix` to one self-contained SST file at
    /// `out_path`, for bulk-loading into another store via
    /// [`replace_prefix_from_sst`](KvStore::replace_prefix_from_sst). This is
    /// the fast path for moving a whole tablet's key range — the file lands at
    /// the target's LSM bottom with no per-key memtable/compaction work.
    ///
    /// Returns `Some(bytes)` with the SST size, `Some(0)` when the range is
    /// empty (no file produced), or `None` when the backend has no SST support
    /// (the caller then falls back to the logical KV path).
    fn export_prefix_sst(&self, prefix: &[u8], out_path: &Path) -> Result<Option<u64>> {
        let _ = (prefix, out_path);
        Ok(None)
    }

    /// Replace the entire `prefix` range with the contents of the SST at
    /// `sst_path`: range-delete the prefix, then ingest the SST *above* that
    /// tombstone so its keys win even if the target already held keys in the
    /// range (any stale key absent from the SST stays deleted). This makes the
    /// install idempotent and safe for a re-move to a non-empty target, not
    /// only a fresh replica.
    ///
    /// Returns `true` on success, or `false` when the backend has no SST
    /// support (the caller then falls back to the logical KV path).
    fn replace_prefix_from_sst(&self, prefix: &[u8], sst_path: &Path) -> Result<bool> {
        let _ = (prefix, sst_path);
        Ok(false)
    }

    /// On-disk directory backing this store, when it has one. Used to place
    /// movement scratch (SST) files on the *same* filesystem as the data — so a
    /// disk-space reservation check measures the disk that actually matters —
    /// and to let ingest avoid a cross-filesystem copy. `None` for stores with
    /// no filesystem home (e.g. in-memory).
    fn data_dir(&self) -> Option<std::path::PathBuf> {
        None
    }
}

/// Bytes currently available to an unprivileged process on the filesystem that
/// holds `path` (the directory must exist). `None` if the query fails. Used by
/// the movement path to keep a disk-space reservation: a snapshot generate/apply
/// that fills the disk otherwise wedges the node into a doom loop, since even
/// compaction and recovery need scratch space (a lesson from TiKV's store
/// `reserve-space`).
pub fn available_disk_bytes(path: &Path) -> Option<u64> {
    fs2::available_space(path).ok()
}

/// Smallest key strictly greater than every key that has `prefix` as a prefix,
/// i.e. the exclusive upper bound of the prefix range. Increments the last
/// byte below `0xFF`, dropping trailing `0xFF` bytes. Returns `None` when the
/// prefix is empty or all `0xFF` (the range is unbounded above — the whole
/// remaining keyspace).
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last().copied() {
        if last < 0xFF {
            *end.last_mut().unwrap() = last + 1;
            return Some(end);
        }
        end.pop();
    }
    None
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
