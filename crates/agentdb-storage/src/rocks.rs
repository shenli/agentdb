use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    checkpoint::Checkpoint, DBCompressionType, Direction, IteratorMode, Options, WriteBatch,
    WriteOptions, DB,
};

use crate::error::{Error, Result};
use crate::kv::{KvIter, KvSnapshot, KvStore, WriteOp};

/// Result type for KV operations.
type KvResult = std::result::Result<(Vec<u8>, Vec<u8>), crate::error::Error>;

/// RocksDB-backed `KvStore`.
pub struct RocksKvStore {
    db: Arc<DB>,
    sync_writes: bool,
}

#[derive(Debug, Clone)]
pub struct RocksKvStoreConfig {
    pub parallelism: i32,
    pub max_background_jobs: i32,
    pub write_buffer_size_bytes: usize,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,
    pub level_zero_file_num_compaction_trigger: i32,
    pub level_zero_slowdown_writes_trigger: i32,
    pub level_zero_stop_writes_trigger: i32,
    pub max_open_files: i32,
}

impl Default for RocksKvStoreConfig {
    fn default() -> Self {
        Self {
            parallelism: 2,
            max_background_jobs: 2,
            write_buffer_size_bytes: 128 * 1024 * 1024,
            max_write_buffer_number: 4,
            min_write_buffer_number_to_merge: 1,
            level_zero_file_num_compaction_trigger: 8,
            level_zero_slowdown_writes_trigger: 24,
            level_zero_stop_writes_trigger: 36,
            max_open_files: 512,
        }
    }
}

impl RocksKvStoreConfig {
    pub fn tablet_domain() -> Self {
        Self {
            parallelism: 1,
            max_background_jobs: 1,
            write_buffer_size_bytes: 16 * 1024 * 1024,
            max_write_buffer_number: 2,
            min_write_buffer_number_to_merge: 1,
            level_zero_file_num_compaction_trigger: 4,
            level_zero_slowdown_writes_trigger: 12,
            level_zero_stop_writes_trigger: 20,
            max_open_files: 128,
        }
    }
}

impl RocksKvStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, &RocksKvStoreConfig::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: &RocksKvStoreConfig) -> Result<Self> {
        let opts = default_rocks_options(config);
        let db = DB::open(&opts, path)?;
        Ok(Self {
            db: Arc::new(db),
            sync_writes: env_flag_enabled("AGENTDB_ROCKSDB_SYNC_WRITES"),
        })
    }

    fn write_options(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(self.sync_writes);
        opts
    }
}

fn default_rocks_options(config: &RocksKvStoreConfig) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    let parallelism = std::env::var("AGENTDB_ROCKSDB_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(config.parallelism);
    let max_background_jobs = std::env::var("AGENTDB_ROCKSDB_MAX_BACKGROUND_JOBS")
        .ok()
        .and_then(|raw| raw.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(config.max_background_jobs);
    opts.increase_parallelism(parallelism);
    opts.set_max_background_jobs(max_background_jobs);

    // AgentDB's local developer path is append-heavy with small metadata rows.
    // Larger memtables reduce flush pressure and keep the out-of-box RocksDB
    // profile from behaving like a tiny embedded default.
    opts.set_write_buffer_size(config.write_buffer_size_bytes);
    opts.set_max_write_buffer_number(config.max_write_buffer_number);
    opts.set_min_write_buffer_number_to_merge(config.min_write_buffer_number_to_merge);
    opts.set_level_zero_file_num_compaction_trigger(config.level_zero_file_num_compaction_trigger);
    opts.set_level_zero_slowdown_writes_trigger(config.level_zero_slowdown_writes_trigger);
    opts.set_level_zero_stop_writes_trigger(config.level_zero_stop_writes_trigger);
    opts.set_bytes_per_sync(1024 * 1024);
    let compression = match std::env::var("AGENTDB_ROCKSDB_COMPRESSION")
        .unwrap_or_else(|_| "lz4".to_string())
        .as_str()
    {
        "none" => DBCompressionType::None,
        "lz4" => DBCompressionType::Lz4,
        "lz4hc" => DBCompressionType::Lz4hc,
        "snappy" => DBCompressionType::Snappy,
        other => {
            eprintln!("unknown AGENTDB_ROCKSDB_COMPRESSION={other}; falling back to lz4");
            DBCompressionType::Lz4
        }
    };
    opts.set_compression_type(compression);
    opts.set_bottommost_compression_type(compression);
    let max_open_files = std::env::var("AGENTDB_ROCKSDB_MAX_OPEN_FILES")
        .ok()
        .and_then(|raw| raw.parse::<i32>().ok())
        .unwrap_or(config.max_open_files);
    opts.set_max_open_files(max_open_files);

    opts
}

impl KvStore for RocksKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put_opt(key, value, &self.write_options())?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete_opt(key, &self.write_options())?;
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

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        collect_prefix_from(&self.db, prefix, start)
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.put(&key, &value),
                WriteOp::Delete { key } => batch.delete(&key),
            }
        }
        self.db.write_opt(batch, &self.write_options())?;
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

    fn checkpoint(&self, out_dir: &Path) -> Result<()> {
        // `Checkpoint::create_checkpoint` refuses to overwrite,
        // so the contract matches our trait ("MUST NOT exist").
        let cp = Checkpoint::new(&self.db)
            .map_err(|e| Error::Other(format!("rocksdb checkpoint init: {e}")))?;
        cp.create_checkpoint(out_dir)
            .map_err(|e| Error::Other(format!("rocksdb create_checkpoint: {e}")))?;
        Ok(())
    }
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
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

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        collect_snapshot_prefix_from(&self.snap, prefix, start)
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
    // !!! DO NOT REORDER THESE FIELDS !!!
    // The `unsafe transmute` in `new()` is sound only because Rust drops fields in
    // declaration order: `snap` (which holds a transmuted `'static` borrow into the
    // DB) MUST drop before `_db` (the Arc that keeps the DB alive). Swapping these
    // two fields turns the transmute into use-after-free UB.
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

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        collect_snapshot_prefix_from(&self.snap, prefix, start)
    }
}

fn collect_prefix_from(db: &DB, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
    let prefix_vec = prefix.to_vec();
    let start_vec = start.to_vec();
    let iter = db.iterator(IteratorMode::From(&start_vec, Direction::Forward));
    let mut out: Vec<KvResult> = Vec::new();
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

fn collect_snapshot_prefix_from(
    snap: &rocksdb::Snapshot<'_>,
    prefix: &[u8],
    start: &[u8],
) -> Result<KvIter> {
    let prefix_vec = prefix.to_vec();
    let start_vec = start.to_vec();
    let iter = snap.iterator(IteratorMode::From(&start_vec, Direction::Forward));
    let mut out: Vec<KvResult> = Vec::new();
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

    #[test]
    fn checkpoint_produces_a_readable_clone() {
        let src = TempDir::new().unwrap();
        let store = RocksKvStore::open(src.path()).unwrap();
        store.put(b"alpha", b"a-val").unwrap();
        store.put(b"beta", b"b-val").unwrap();

        // Checkpoint into a sibling dir that must not exist.
        let parent = TempDir::new().unwrap();
        let dest = parent.path().join("ckpt");
        store.checkpoint(&dest).unwrap();
        assert!(dest.exists(), "checkpoint dir should have been created");

        // Open the checkpoint as an independent store and read back.
        let restored = RocksKvStore::open(&dest).unwrap();
        assert_eq!(
            restored.get(b"alpha").unwrap().as_deref(),
            Some(b"a-val".as_ref())
        );
        assert_eq!(
            restored.get(b"beta").unwrap().as_deref(),
            Some(b"b-val".as_ref())
        );
    }

    #[test]
    fn scan_prefix_from_seeks_to_lower_bound_within_prefix() {
        let dir = TempDir::new().unwrap();
        let store = RocksKvStore::open(dir.path()).unwrap();
        store.put(b"event/t/s/00000000000000000001", b"e1").unwrap();
        store.put(b"event/t/s/00000000000000000002", b"e2").unwrap();
        store.put(b"event/t/s/00000000000000000003", b"e3").unwrap();
        store
            .put(b"event/t/other/00000000000000000003", b"other")
            .unwrap();

        let rows = store
            .scan_prefix_from(b"event/t/s/", b"event/t/s/00000000000000000002")
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let keys: Vec<Vec<u8>> = rows.into_iter().map(|(key, _)| key).collect();

        assert_eq!(
            keys,
            vec![
                b"event/t/s/00000000000000000002".to_vec(),
                b"event/t/s/00000000000000000003".to_vec(),
            ]
        );
    }

    #[test]
    fn checkpoint_refuses_to_overwrite_existing_dir() {
        let src = TempDir::new().unwrap();
        let store = RocksKvStore::open(src.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let dest_dir = TempDir::new().unwrap();
        let err = store.checkpoint(dest_dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("exist")
                || msg.to_lowercase().contains("invalid")
                || msg.to_lowercase().contains("create_checkpoint"),
            "expected 'already exists' style failure; got: {msg}",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_snapshot_is_send_across_await() {
        let dir = TempDir::new().unwrap();
        let store: Arc<RocksKvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        store.put(b"k1", b"v1").unwrap();
        let snap = Arc::clone(&store).snapshot_owned().unwrap();
        // `tokio::spawn` requires the future to be `Send + 'static`. If `snap` were
        // not `Send`, this would fail to compile — which is the whole point of
        // keeping the snapshot across an `.await` in an async handler.
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            snap.get(b"k1").unwrap()
        });
        assert_eq!(handle.await.unwrap().as_deref(), Some(b"v1".as_ref()));
    }
}
