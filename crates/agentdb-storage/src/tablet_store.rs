//! Tablet-local applied-state namespace over a `KvStore`.
//!
//! This keeps the current KV abstraction but stops new tablet-state code from
//! writing into one global keyspace. Each tablet gets an independent logical
//! prefix, which can later map to a separate RocksDB instance, column family, or
//! another tablet-local LSM implementation.

use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use sha2::{Digest, Sha256};

use crate::{
    Error, KvIter, KvSnapshot, KvStore, Result, RocksKvStore, RocksKvStoreConfig, WriteOp,
};

const TABLET_STATE_PREFIX: &[u8] = b"tablet_state/";

#[derive(Clone)]
pub struct TabletKvStore {
    kv: Arc<dyn KvStore + Send + Sync>,
    tablet_id: i64,
    scope: TabletKvScope,
}

#[derive(Clone)]
enum TabletKvScope {
    SharedPrefix(Vec<u8>),
    Physical,
}

impl TabletKvStore {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>, tablet_id: i64) -> Result<Self> {
        if tablet_id < 0 {
            return Err(Error::Other("tablet_id must be non-negative".into()));
        }
        Ok(Self {
            kv,
            tablet_id,
            scope: TabletKvScope::SharedPrefix(tablet_prefix(tablet_id)),
        })
    }

    pub fn physical(kv: Arc<dyn KvStore + Send + Sync>, tablet_id: i64) -> Result<Self> {
        if tablet_id < 0 {
            return Err(Error::Other("tablet_id must be non-negative".into()));
        }
        Ok(Self {
            kv,
            tablet_id,
            scope: TabletKvScope::Physical,
        })
    }

    pub fn tablet_id(&self) -> i64 {
        self.tablet_id
    }

    pub fn is_physical(&self) -> bool {
        matches!(self.scope, TabletKvScope::Physical)
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
        let scope = self.scope.clone();
        let iter = self.kv.scan_prefix(&scoped_prefix)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| (unscoped_key(&scope, key), value))
        })))
    }

    fn key(&self, key: &[u8]) -> Vec<u8> {
        scoped_key(&self.scope, key)
    }
}

impl KvStore for TabletKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Self::get(self, key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Self::put(self, key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        Self::delete(self, key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        Self::scan_prefix(self, prefix)
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let scoped_prefix = self.key(prefix);
        let scoped_start = self.key(start);
        let scope = self.scope.clone();
        let iter = self.kv.scan_prefix_from(&scoped_prefix, &scoped_start)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| (unscoped_key(&scope, key), value))
        })))
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        Self::write_batch(self, ops)
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        let snapshot = self.kv.snapshot()?;
        Ok(Box::new(TabletKvSnapshot {
            inner: snapshot,
            scope: self.scope.clone(),
        }))
    }

    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>> {
        let inner = self.kv.clone().snapshot_owned()?;
        Ok(Arc::new(TabletKvOwnedSnapshot {
            inner,
            scope: self.scope.clone(),
        }))
    }

    fn checkpoint(&self, out_dir: &Path) -> Result<()> {
        match self.scope {
            TabletKvScope::Physical => self.kv.checkpoint(out_dir),
            TabletKvScope::SharedPrefix(_) => Err(Error::Other(
                "tablet-scoped checkpoint requires a physical tablet store".into(),
            )),
        }
    }
}

struct TabletKvSnapshot<'a> {
    inner: Box<dyn KvSnapshot + 'a>,
    scope: TabletKvScope,
}

impl KvSnapshot for TabletKvSnapshot<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(&scoped_key(&self.scope, key))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let scoped_prefix = scoped_key(&self.scope, prefix);
        let scope = self.scope.clone();
        let iter = self.inner.scan_prefix(&scoped_prefix)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| (unscoped_key(&scope, key), value))
        })))
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let scoped_prefix = scoped_key(&self.scope, prefix);
        let scoped_start = scoped_key(&self.scope, start);
        let scope = self.scope.clone();
        let iter = self.inner.scan_prefix_from(&scoped_prefix, &scoped_start)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| (unscoped_key(&scope, key), value))
        })))
    }
}

struct TabletKvOwnedSnapshot {
    inner: Arc<dyn KvSnapshot>,
    scope: TabletKvScope,
}

impl KvSnapshot for TabletKvOwnedSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(&scoped_key(&self.scope, key))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let scoped_prefix = scoped_key(&self.scope, prefix);
        let scope = self.scope.clone();
        let iter = self.inner.scan_prefix(&scoped_prefix)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| (unscoped_key(&scope, key), value))
        })))
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> Result<KvIter> {
        let scoped_prefix = scoped_key(&self.scope, prefix);
        let scoped_start = scoped_key(&self.scope, start);
        let scope = self.scope.clone();
        let iter = self.inner.scan_prefix_from(&scoped_prefix, &scoped_start)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|(key, value)| (unscoped_key(&scope, key), value))
        })))
    }
}

pub trait TabletStoreLifecycle: Send + Sync {
    fn layout_name(&self) -> &'static str;
    fn open_tablet_store(&self, tablet_id: i64) -> Result<TabletKvStore>;
    fn restore_tablet_checkpoint(
        &self,
        tablet_id: i64,
        checkpoint_dir: &Path,
    ) -> Result<TabletStoreManifest> {
        let _ = (tablet_id, checkpoint_dir);
        Err(Error::Other(format!(
            "{} tablet stores do not support checkpoint restore",
            self.layout_name()
        )))
    }

    fn checkpoint_tablet(&self, tablet_id: i64, out_dir: &Path) -> Result<TabletStoreManifest> {
        let store = self.open_tablet_store(tablet_id)?;
        store.checkpoint(out_dir)?;
        let summary = checkpoint_summary(out_dir)?;
        Ok(TabletStoreManifest {
            tablet_id,
            layout: self.layout_name().to_string(),
            engine: "unknown".into(),
            storage_uri: out_dir.display().to_string(),
            checksum_sha256: Some(summary.checksum_sha256),
            size_bytes: Some(summary.size_bytes),
            ..TabletStoreManifest::default_for(tablet_id)
        })
    }
}

pub struct PrefixScopedTabletStore {
    kv: Arc<dyn KvStore + Send + Sync>,
}

impl PrefixScopedTabletStore {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self { kv }
    }
}

impl TabletStoreLifecycle for PrefixScopedTabletStore {
    fn layout_name(&self) -> &'static str {
        "prefix_scoped"
    }

    fn open_tablet_store(&self, tablet_id: i64) -> Result<TabletKvStore> {
        TabletKvStore::new(self.kv.clone(), tablet_id)
    }

    fn checkpoint_tablet(&self, tablet_id: i64, out_dir: &Path) -> Result<TabletStoreManifest> {
        let _ = (tablet_id, out_dir);
        Err(Error::Other(
            "prefix-scoped tablet stores do not support tablet-local checkpoints".into(),
        ))
    }
}

pub struct GroupedRocksDbTabletStore {
    root_dir: PathBuf,
    tablets_per_domain: i64,
    max_open_domains: usize,
    rocks_config: RocksKvStoreConfig,
    cache: Mutex<GroupedRocksDbCache>,
}

#[derive(Default)]
struct GroupedRocksDbCache {
    next_use: u64,
    domains: BTreeMap<i64, CachedRocksDbDomain>,
}

struct CachedRocksDbDomain {
    kv: Arc<dyn KvStore + Send + Sync>,
    last_used: u64,
}

impl GroupedRocksDbTabletStore {
    pub fn new(
        root_dir: impl Into<PathBuf>,
        tablets_per_domain: i64,
        max_open_domains: usize,
    ) -> Self {
        Self::new_with_rocks_config(
            root_dir,
            tablets_per_domain,
            max_open_domains,
            RocksKvStoreConfig::tablet_domain(),
        )
    }

    pub fn new_with_rocks_config(
        root_dir: impl Into<PathBuf>,
        tablets_per_domain: i64,
        max_open_domains: usize,
        rocks_config: RocksKvStoreConfig,
    ) -> Self {
        Self {
            root_dir: root_dir.into(),
            tablets_per_domain: tablets_per_domain.max(1),
            max_open_domains: max_open_domains.max(1),
            rocks_config,
            cache: Mutex::new(GroupedRocksDbCache::default()),
        }
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn tablets_per_domain(&self) -> i64 {
        self.tablets_per_domain
    }

    pub fn max_open_domains(&self) -> usize {
        self.max_open_domains
    }

    pub fn rocks_config(&self) -> &RocksKvStoreConfig {
        &self.rocks_config
    }

    pub fn domain_id(&self, tablet_id: i64) -> Result<i64> {
        if tablet_id < 0 {
            return Err(Error::Other("tablet_id must be non-negative".into()));
        }
        Ok(tablet_id / self.tablets_per_domain)
    }

    pub fn domain_path(&self, domain_id: i64) -> PathBuf {
        self.root_dir.join(format!("domain-{domain_id:020}"))
    }

    fn open_domain(&self, domain_id: i64) -> Result<Arc<dyn KvStore + Send + Sync>> {
        let mut cache = self
            .cache
            .lock()
            .expect("grouped rocksdb tablet store cache poisoned");
        cache.next_use = cache.next_use.saturating_add(1);
        let use_order = cache.next_use;
        if let Some(domain) = cache.domains.get_mut(&domain_id) {
            domain.last_used = use_order;
            return Ok(domain.kv.clone());
        }

        let domain_path = self.domain_path(domain_id);
        fs::create_dir_all(&domain_path)?;
        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open_with_config(
            &domain_path,
            &self.rocks_config,
        )?);
        cache.domains.insert(
            domain_id,
            CachedRocksDbDomain {
                kv: kv.clone(),
                last_used: use_order,
            },
        );
        while cache.domains.len() > self.max_open_domains {
            let Some(evict_id) = cache
                .domains
                .iter()
                .filter(|(_, domain)| Arc::strong_count(&domain.kv) == 1)
                .min_by_key(|(_, domain)| domain.last_used)
                .map(|(domain_id, _)| *domain_id)
            else {
                break;
            };
            cache.domains.remove(&evict_id);
        }
        Ok(kv)
    }

    fn ensure_domain_not_open(&self, domain_id: i64) -> Result<()> {
        let cache = self
            .cache
            .lock()
            .expect("grouped rocksdb tablet store cache poisoned");
        if cache.domains.contains_key(&domain_id) {
            return Err(Error::Other(format!(
                "cannot restore domain {domain_id}; RocksDB domain is already open"
            )));
        }
        Ok(())
    }
}

impl TabletStoreLifecycle for GroupedRocksDbTabletStore {
    fn layout_name(&self) -> &'static str {
        "grouped_rocksdb"
    }

    fn open_tablet_store(&self, tablet_id: i64) -> Result<TabletKvStore> {
        let domain_id = self.domain_id(tablet_id)?;
        let kv = self.open_domain(domain_id)?;
        TabletKvStore::new(kv, tablet_id)
    }

    fn restore_tablet_checkpoint(
        &self,
        tablet_id: i64,
        checkpoint_dir: &Path,
    ) -> Result<TabletStoreManifest> {
        if self.tablets_per_domain != 1 {
            return Err(Error::Other(
                "tablet restore requires grouped_rocksdb tablets_per_domain=1".into(),
            ));
        }
        if !checkpoint_dir.is_dir() {
            return Err(Error::Other(format!(
                "checkpoint directory {} does not exist",
                checkpoint_dir.display()
            )));
        }
        let domain_id = self.domain_id(tablet_id)?;
        self.ensure_domain_not_open(domain_id)?;
        let domain_path = self.domain_path(domain_id);
        if domain_path.exists() {
            let mut entries = fs::read_dir(&domain_path)?;
            if entries.next().is_some() {
                return Err(Error::Other(format!(
                    "cannot restore tablet {tablet_id}; destination {} already exists",
                    domain_path.display()
                )));
            }
            fs::remove_dir_all(&domain_path)?;
        }
        copy_dir_all(checkpoint_dir, &domain_path)?;
        let summary = checkpoint_summary(&domain_path)?;
        Ok(TabletStoreManifest {
            tablet_id,
            layout: self.layout_name().into(),
            engine: "rocksdb".into(),
            storage_uri: domain_path.display().to_string(),
            checksum_sha256: Some(summary.checksum_sha256),
            size_bytes: Some(summary.size_bytes),
            ..TabletStoreManifest::default_for(tablet_id)
        })
    }

    fn checkpoint_tablet(&self, tablet_id: i64, out_dir: &Path) -> Result<TabletStoreManifest> {
        if self.tablets_per_domain != 1 {
            return Err(Error::Other(
                "tablet checkpoint requires grouped_rocksdb tablets_per_domain=1".into(),
            ));
        }
        let domain_id = self.domain_id(tablet_id)?;
        let kv = self.open_domain(domain_id)?;
        kv.checkpoint(out_dir)?;
        let summary = checkpoint_summary(out_dir)?;
        Ok(TabletStoreManifest {
            tablet_id,
            layout: self.layout_name().into(),
            engine: "rocksdb".into(),
            storage_uri: out_dir.display().to_string(),
            checksum_sha256: Some(summary.checksum_sha256),
            size_bytes: Some(summary.size_bytes),
            ..TabletStoreManifest::default_for(tablet_id)
        })
    }
}

fn copy_dir_all(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            copy_dir_all(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TabletStoreManifest {
    pub tablet_id: i64,
    pub cell_id: Option<String>,
    pub placement_scope_id: Option<String>,
    pub tablet_epoch: Option<i64>,
    pub latest_applied_index: u64,
    pub latest_snapshot_index: Option<u64>,
    pub layout: String,
    pub engine: String,
    pub storage_uri: String,
    pub checksum_sha256: Option<String>,
    pub size_bytes: Option<u64>,
}

impl TabletStoreManifest {
    pub fn default_for(tablet_id: i64) -> Self {
        Self {
            tablet_id,
            cell_id: None,
            placement_scope_id: None,
            tablet_epoch: None,
            latest_applied_index: 0,
            latest_snapshot_index: None,
            layout: "unknown".into(),
            engine: "unknown".into(),
            storage_uri: String::new(),
            checksum_sha256: None,
            size_bytes: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointSummary {
    checksum_sha256: String,
    size_bytes: u64,
}

fn checkpoint_summary(root: &Path) -> Result<CheckpointSummary> {
    let mut files = Vec::new();
    collect_checkpoint_files(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));

    let mut hasher = Sha256::new();
    let mut size_bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    for (relative, path) in files {
        hasher.update(relative.as_os_str().as_encoded_bytes());
        hasher.update([0]);
        let mut file = fs::File::open(&path)?;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            size_bytes = size_bytes.saturating_add(read as u64);
            hasher.update(&buffer[..read]);
        }
        hasher.update([0]);
    }

    Ok(CheckpointSummary {
        checksum_sha256: format!("{:x}", hasher.finalize()),
        size_bytes,
    })
}

fn collect_checkpoint_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_checkpoint_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path.strip_prefix(root).map_err(|err| {
                Error::Other(format!(
                    "checkpoint path {} is outside root {}: {err}",
                    path.display(),
                    root.display()
                ))
            })?;
            files.push((relative.to_path_buf(), path));
        }
    }
    Ok(())
}

fn scoped_key(scope: &TabletKvScope, key: &[u8]) -> Vec<u8> {
    match scope {
        TabletKvScope::SharedPrefix(prefix) => {
            let mut scoped = prefix.clone();
            scoped.extend_from_slice(key);
            scoped
        }
        TabletKvScope::Physical => key.to_vec(),
    }
}

fn unscoped_key(scope: &TabletKvScope, key: Vec<u8>) -> Vec<u8> {
    match scope {
        TabletKvScope::SharedPrefix(prefix) => key
            .strip_prefix(prefix.as_slice())
            .map_or_else(|| key.clone(), |suffix| suffix.to_vec()),
        TabletKvScope::Physical => key,
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

    #[test]
    fn physical_tablet_store_does_not_prefix_keys() {
        let dir = tempfile::tempdir().unwrap();
        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let tablet = TabletKvStore::physical(kv.clone(), 7).unwrap();

        tablet.put(b"record/a", b"a").unwrap();

        assert!(tablet.is_physical());
        assert_eq!(kv.get(b"record/a").unwrap().as_deref(), Some(&b"a"[..]));
        assert!(kv
            .get(b"tablet_state/00000000000000000007/record/a")
            .unwrap()
            .is_none());
    }

    #[test]
    fn grouped_rocksdb_store_isolates_tablets_inside_bounded_domains() {
        let dir = tempfile::tempdir().unwrap();
        let lifecycle = GroupedRocksDbTabletStore::new(dir.path(), 2, 8);
        let tablet_a = lifecycle.open_tablet_store(7).unwrap();
        let tablet_b = lifecycle.open_tablet_store(8).unwrap();

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
        let domain_a = lifecycle.domain_id(7).unwrap();
        let domain_b = lifecycle.domain_id(8).unwrap();
        assert!(lifecycle.domain_path(domain_a).exists());
        assert!(lifecycle.domain_path(domain_b).exists());
        assert_ne!(
            lifecycle.domain_path(domain_a),
            lifecycle.domain_path(domain_b)
        );
    }

    #[test]
    fn grouped_rocksdb_checkpoint_requires_single_tablet_domain() {
        let dir = tempfile::tempdir().unwrap();
        let lifecycle = GroupedRocksDbTabletStore::new(dir.path().join("tablets"), 4, 8);
        let tablet = lifecycle.open_tablet_store(7).unwrap();
        tablet.put(b"record/a", b"a").unwrap();

        let err = lifecycle
            .checkpoint_tablet(7, &dir.path().join("checkpoint"))
            .unwrap_err();
        assert!(err.to_string().contains("tablets_per_domain=1"));
    }

    #[test]
    fn grouped_rocksdb_checkpoint_can_capture_single_tablet_domain() {
        let dir = tempfile::tempdir().unwrap();
        let lifecycle = GroupedRocksDbTabletStore::new(dir.path().join("tablets"), 1, 8);
        let tablet = lifecycle.open_tablet_store(7).unwrap();
        tablet.put(b"record/a", b"a").unwrap();
        drop(tablet);

        let checkpoint_dir = dir.path().join("checkpoint");
        let manifest = lifecycle.checkpoint_tablet(7, &checkpoint_dir).unwrap();
        let restored = RocksKvStore::open(&checkpoint_dir).unwrap();

        assert_eq!(manifest.tablet_id, 7);
        assert_eq!(manifest.layout, "grouped_rocksdb");
        assert_eq!(manifest.engine, "rocksdb");
        assert!(manifest.size_bytes.unwrap_or_default() > 0);
        assert_eq!(manifest.checksum_sha256.as_deref().unwrap().len(), 64);
        assert_eq!(
            restored
                .get(b"tablet_state/00000000000000000007/record/a")
                .unwrap()
                .as_deref(),
            Some(&b"a"[..])
        );
    }

    #[test]
    fn grouped_rocksdb_restore_can_seed_single_tablet_domain() {
        let dir = tempfile::tempdir().unwrap();
        let source = GroupedRocksDbTabletStore::new(dir.path().join("source"), 1, 8);
        let tablet = source.open_tablet_store(7).unwrap();
        tablet.put(b"record/a", b"a").unwrap();
        drop(tablet);

        let checkpoint_dir = dir.path().join("checkpoint");
        let checkpoint_manifest = source.checkpoint_tablet(7, &checkpoint_dir).unwrap();

        let target = GroupedRocksDbTabletStore::new(dir.path().join("target"), 1, 8);
        let restore_manifest = target
            .restore_tablet_checkpoint(7, &checkpoint_dir)
            .unwrap();
        let restored = target.open_tablet_store(7).unwrap();

        assert_eq!(restore_manifest.tablet_id, 7);
        assert_eq!(restore_manifest.layout, "grouped_rocksdb");
        assert_eq!(restore_manifest.engine, "rocksdb");
        assert_eq!(restore_manifest.size_bytes, checkpoint_manifest.size_bytes);
        assert_eq!(
            restored.get(b"record/a").unwrap().as_deref(),
            Some(&b"a"[..])
        );
    }

    #[test]
    fn grouped_rocksdb_restore_refuses_open_destination_domain() {
        let dir = tempfile::tempdir().unwrap();
        let source = GroupedRocksDbTabletStore::new(dir.path().join("source"), 1, 8);
        let tablet = source.open_tablet_store(7).unwrap();
        tablet.put(b"record/a", b"a").unwrap();
        drop(tablet);
        let checkpoint_dir = dir.path().join("checkpoint");
        source.checkpoint_tablet(7, &checkpoint_dir).unwrap();

        let target = GroupedRocksDbTabletStore::new(dir.path().join("target"), 1, 8);
        let open_target = target.open_tablet_store(7).unwrap();
        let err = target
            .restore_tablet_checkpoint(7, &checkpoint_dir)
            .unwrap_err();

        assert!(err.to_string().contains("already open"));
        drop(open_target);
    }
}
