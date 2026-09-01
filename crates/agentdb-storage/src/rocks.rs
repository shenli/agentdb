use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    checkpoint::Checkpoint, DBCompressionType, Direction, IngestExternalFileOptions, IteratorMode,
    Options, WriteBatch, WriteOptions, DB,
};

use crate::error::{Error, Result};
use crate::kv::{prefix_upper_bound, KvIter, KvSnapshot, KvStore, KvStoreMetrics, WriteOp};

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
            sync_writes: rocksdb_sync_writes_enabled(),
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

impl RocksKvStore {
    /// Write every KV pair under `prefix` to a single self-contained SST
    /// file at `sst_path` (RocksDB `SstFileWriter`). The file can be
    /// bulk-loaded into another `RocksKvStore` via `ingest_ssts`, bypassing
    /// the memtable / WAL / compaction on the target — the fast path for
    /// moving a whole tablet's key range. Returns the SST size in bytes, or
    /// `Ok(0)` when the range is empty (no SST is produced).
    ///
    /// `SstFileWriter` requires keys in ascending order; the prefix scan
    /// yields RocksDB bytewise order, matching the default comparator the
    /// SST and the target DB share.
    pub fn write_range_to_sst(&self, prefix: &[u8], sst_path: &Path) -> Result<u64> {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        let mut writer = rocksdb::SstFileWriter::create(&opts);
        writer
            .open(sst_path)
            .map_err(|e| Error::Other(format!("sst open: {e}")))?;
        let mut count = 0u64;
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix, Direction::Forward));
        for item in iter {
            let (key, value) = item.map_err(|e| Error::Other(format!("sst scan: {e}")))?;
            if !key.starts_with(prefix) {
                break;
            }
            writer
                .put(&key, &value)
                .map_err(|e| Error::Other(format!("sst put: {e}")))?;
            count += 1;
        }
        if count == 0 {
            // SstFileWriter::finish errors on an empty file; produce none.
            let _ = std::fs::remove_file(sst_path);
            return Ok(0);
        }
        writer
            .finish()
            .map_err(|e| Error::Other(format!("sst finish: {e}")))?;
        Ok(std::fs::metadata(sst_path).map(|m| m.len()).unwrap_or(0))
    }

    /// Bulk-load external SST files (produced by `write_range_to_sst`) into
    /// this store via RocksDB `IngestExternalFile`. The ingested key ranges
    /// must not overlap existing keys — a movement target holds a fresh
    /// tablet range — so the SSTs land at the LSM bottom without a rewrite.
    pub fn ingest_ssts(&self, paths: &[std::path::PathBuf]) -> Result<()> {
        self.db
            .ingest_external_file(paths.to_vec())
            .map_err(|e| Error::Other(format!("rocksdb ingest_external_file: {e}")))
    }
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
        stream_prefix_from(self.db.clone(), prefix, prefix)
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        stream_prefix_from(self.db.clone(), prefix, start)
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

    fn export_prefix_sst(&self, prefix: &[u8], out_path: &Path) -> Result<Option<u64>> {
        Ok(Some(self.write_range_to_sst(prefix, out_path)?))
    }

    fn replace_prefix_from_sst(&self, prefix: &[u8], sst_path: &Path) -> Result<bool> {
        // 1. Range-delete the whole prefix so any stale key on the target that
        //    the SST does not carry does not survive the move. A range
        //    tombstone is O(1) to write regardless of how many keys it covers.
        let mut batch = WriteBatch::default();
        match prefix_upper_bound(prefix) {
            Some(end) => batch.delete_range(prefix, &end),
            None => {
                // Empty / all-0xFF prefix: the range is unbounded above, so
                // there is no single delete_range end key. Movement always
                // passes a concrete tablet prefix; this branch is defensive —
                // clear the remaining keyspace point-wise.
                for item in self
                    .db
                    .iterator(IteratorMode::From(prefix, Direction::Forward))
                {
                    let (key, _) =
                        item.map_err(|e| Error::Other(format!("sst replace scan: {e}")))?;
                    if !key.starts_with(prefix) {
                        break;
                    }
                    batch.delete(&key);
                }
            }
        }
        self.db.write_opt(batch, &self.write_options())?;

        // 2. Ingest the SST above the tombstone. `allow_global_seqno` lets
        //    RocksDB stamp the file with a sequence number newer than the
        //    delete_range, so its keys win; `allow_blocking_flush` flushes the
        //    memtable (which holds that tombstone) first so the ordering is
        //    correct. Result: an idempotent whole-range replace, safe even when
        //    the target already held keys in the range.
        let mut opts = IngestExternalFileOptions::default();
        opts.set_allow_global_seqno(true);
        opts.set_allow_blocking_flush(true);
        self.db
            .ingest_external_file_opts(&opts, vec![sst_path])
            .map_err(|e| Error::Other(format!("rocksdb ingest_external_file_opts: {e}")))?;
        Ok(true)
    }

    fn data_dir(&self) -> Option<std::path::PathBuf> {
        Some(self.db.path().to_path_buf())
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

    fn metrics(&self) -> Result<Option<KvStoreMetrics>> {
        Ok(Some(KvStoreMetrics {
            live_data_size_bytes: rocks_property_u64(&self.db, "rocksdb.estimate-live-data-size")?,
            total_sst_file_size_bytes: rocks_property_u64(
                &self.db,
                "rocksdb.total-sst-files-size",
            )?,
            active_memtable_bytes: rocks_property_u64(
                &self.db,
                "rocksdb.cur-size-active-mem-table",
            )?,
            all_memtables_bytes: rocks_property_u64(&self.db, "rocksdb.cur-size-all-mem-tables")?,
            immutable_memtables: rocks_property_u64(&self.db, "rocksdb.num-immutable-mem-table")?,
            level0_files: rocks_property_u64(&self.db, "rocksdb.num-files-at-level0")?,
            pending_compaction_bytes: rocks_property_u64(
                &self.db,
                "rocksdb.estimate-pending-compaction-bytes",
            )?,
            compaction_pending: rocks_property_bool(&self.db, "rocksdb.compaction-pending")?,
            memtable_flush_pending: rocks_property_bool(
                &self.db,
                "rocksdb.mem-table-flush-pending",
            )?,
            running_compactions: rocks_property_u64(&self.db, "rocksdb.num-running-compactions")?,
            running_flushes: rocks_property_u64(&self.db, "rocksdb.num-running-flushes")?,
            background_errors: rocks_property_u64(&self.db, "rocksdb.background-errors")?,
            estimated_keys: rocks_property_u64(&self.db, "rocksdb.estimate-num-keys")?,
        }))
    }

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.db.compact_range::<&[u8], &[u8]>(start, end);
        Ok(())
    }
}

fn rocks_property_u64(db: &DB, name: &str) -> Result<Option<u64>> {
    db.property_int_value(name).map_err(Error::from)
}

fn rocks_property_bool(db: &DB, name: &str) -> Result<Option<bool>> {
    Ok(rocks_property_u64(db, name)?.map(|value| value != 0))
}

/// Whether acknowledged RocksDB writes fsync the WAL before returning.
///
/// ON by default: AgentDB's core promise is that an acknowledged append
/// survives a crash, so an ack must mean "WAL on disk", not "in the
/// memtable". Set `AGENTDB_ROCKSDB_SYNC_WRITES=0` (or false/off/no/disabled)
/// to opt out for throwaway environments and benchmarks that measure the
/// non-durable ceiling.
pub fn rocksdb_sync_writes_enabled() -> bool {
    match std::env::var("AGENTDB_ROCKSDB_SYNC_WRITES") {
        Ok(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no" | "disabled"
        ),
        Err(_) => true,
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

fn stream_prefix_from(db: Arc<DB>, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
    let prefix_vec = prefix.to_vec();
    let start_vec = start.to_vec();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<KvResult>(32);
    std::thread::Builder::new()
        .name("agentdb-rocks-scan".into())
        .spawn(move || {
            let iter = db.iterator(IteratorMode::From(&start_vec, Direction::Forward));
            for result in iter {
                let item = match result {
                    Ok((key, value)) => {
                        if !key.starts_with(&prefix_vec) {
                            break;
                        }
                        Ok((key.to_vec(), value.to_vec()))
                    }
                    Err(error) => Err(Error::from(error)),
                };
                let failed = item.is_err();
                if sender.send(item).is_err() || failed {
                    break;
                }
            }
        })
        .map_err(|error| Error::Other(format!("spawn RocksDB prefix scan: {error}")))?;
    Ok(Box::new(receiver.into_iter()))
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
    fn sst_export_then_ingest_moves_a_prefix_range() {
        let src_dir = TempDir::new().unwrap();
        let src = RocksKvStore::open(src_dir.path()).unwrap();
        // Two tablets' ranges; we move only tablet/1/.
        src.put(b"tablet/1/a", b"1a").unwrap();
        src.put(b"tablet/1/b", b"1b").unwrap();
        src.put(b"tablet/2/a", b"2a").unwrap();

        let sst_dir = TempDir::new().unwrap();
        let sst = sst_dir.path().join("t1.sst");
        let bytes = src.write_range_to_sst(b"tablet/1/", &sst).unwrap();
        assert!(bytes > 0);

        let dst_dir = TempDir::new().unwrap();
        let dst = RocksKvStore::open(dst_dir.path()).unwrap();
        dst.ingest_ssts(&[sst]).unwrap();

        // The moved range is present; the other tablet's range is not.
        assert_eq!(dst.get(b"tablet/1/a").unwrap().as_deref(), Some(&b"1a"[..]));
        assert_eq!(dst.get(b"tablet/1/b").unwrap().as_deref(), Some(&b"1b"[..]));
        assert!(dst.get(b"tablet/2/a").unwrap().is_none());
    }

    #[test]
    fn replace_prefix_from_sst_overwrites_a_non_empty_target() {
        // The full movement path must be safe when the target already holds a
        // (stale) copy of the tablet range — not only a fresh replica. Seed the
        // source with the new state, and the target with a divergent old state
        // (an extra key that must disappear, and a changed value), then replace.
        let src_dir = TempDir::new().unwrap();
        let src = RocksKvStore::open(src_dir.path()).unwrap();
        src.put(b"tablet/1/a", b"new-a").unwrap();
        src.put(b"tablet/1/b", b"new-b").unwrap();

        let sst_dir = TempDir::new().unwrap();
        let sst = sst_dir.path().join("t1.sst");
        assert!(src.write_range_to_sst(b"tablet/1/", &sst).unwrap() > 0);

        let dst_dir = TempDir::new().unwrap();
        let dst = RocksKvStore::open(dst_dir.path()).unwrap();
        dst.put(b"tablet/1/a", b"old-a").unwrap(); // will be overwritten
        dst.put(b"tablet/1/stale", b"gone").unwrap(); // absent from SST → must vanish
        dst.put(b"tablet/2/keep", b"other").unwrap(); // outside prefix → must survive

        assert!(dst.replace_prefix_from_sst(b"tablet/1/", &sst).unwrap());

        assert_eq!(
            dst.get(b"tablet/1/a").unwrap().as_deref(),
            Some(&b"new-a"[..])
        );
        assert_eq!(
            dst.get(b"tablet/1/b").unwrap().as_deref(),
            Some(&b"new-b"[..])
        );
        assert!(dst.get(b"tablet/1/stale").unwrap().is_none());
        assert_eq!(
            dst.get(b"tablet/2/keep").unwrap().as_deref(),
            Some(&b"other"[..])
        );
        // Exactly the source range remains under the moved prefix.
        assert_eq!(dst.scan_prefix(b"tablet/1/").unwrap().count(), 2);
    }

    #[test]
    fn sst_export_of_empty_range_produces_no_file() {
        let dir = TempDir::new().unwrap();
        let store = RocksKvStore::open(dir.path()).unwrap();
        store.put(b"other/x", b"v").unwrap();
        let sst_dir = TempDir::new().unwrap();
        let sst = sst_dir.path().join("empty.sst");
        assert_eq!(store.write_range_to_sst(b"tablet/1/", &sst).unwrap(), 0);
        assert!(!sst.exists());
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
