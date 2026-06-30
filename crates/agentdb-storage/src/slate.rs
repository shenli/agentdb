//! SlateDB-backed `KvStore`.
//!
//! See `docs/design/storage-engine.md` for SlateDB's current role as an
//! optional object-store-backed evaluation backend.

use std::sync::Arc;

use object_store::ObjectStore;
use tokio::runtime::Runtime;

use crate::error::{Error, Result};

/// A `KvStore` backed by SlateDB on object storage.
///
/// Owns a dedicated tokio `Runtime` to bridge the sync `KvStore` trait
/// to SlateDB's async API. The runtime runs on one extra OS thread;
/// this was the chosen tradeoff over calling `block_on` on a shared
/// runtime handle (which deadlocks from inside a live tokio task).
pub struct SlateKvStore {
    pub(crate) inner: Arc<slatedb::Db>,
    /// Option so `Drop` can `take()` the `Arc<Runtime>` and hand it to a
    /// plain OS thread for shutdown. `Runtime::drop` is a blocking join on
    /// worker threads and panics if called from inside another async
    /// context (e.g. a `#[tokio::test]`). Always `Some` while the store
    /// is alive.
    pub(crate) runtime: Option<Arc<Runtime>>,
}

impl SlateKvStore {
    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("runtime present while SlateKvStore is alive")
    }

    /// Clone of the owned runtime `Arc` — used when constructing a
    /// `SlateSnapshot`, which needs its own owned handle so it can outlive
    /// any particular borrow of `self`.
    fn runtime_arc(&self) -> Arc<Runtime> {
        self.runtime
            .as_ref()
            .expect("runtime present while SlateKvStore is alive")
            .clone()
    }
}

impl SlateKvStore {
    /// Open (or create) a SlateDB database rooted at `path` inside `object_store`.
    /// `path` is an object-store prefix like `"agentdb/slate"` (no leading slash).
    ///
    /// Builds the owned runtime FIRST, then spawns `Db::open` on it so all of
    /// SlateDB's internal background tasks (flush, compaction) register on
    /// our runtime rather than the caller's. This is load-bearing: if
    /// `Db::open` runs on the caller's runtime and the caller then blocks
    /// (e.g. via `KvStore::get`'s sync bridge), SlateDB's background tasks
    /// starve and foreground ops hang.
    pub async fn open(object_store: Arc<dyn ObjectStore>, path: &str) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                // 2 workers: 1 for foreground, 1 for SlateDB compaction/flush.
                // worker_threads(1) was insufficient and caused background
                // task starvation during get-on-empty-db tests.
                .worker_threads(2)
                .enable_all()
                .thread_name("slatedb")
                .build()
                .map_err(|e| Error::Other(format!("slatedb runtime: {e}")))?,
        );

        // `rt.spawn(...).await` returns a JoinHandle<T>; JoinHandles can be
        // awaited from any runtime (they signal via a waker). Critically,
        // the inner async block runs on OUR runtime, so every `tokio::spawn`
        // inside `slatedb::Db::open` goes to OUR worker threads.
        let path = path.to_string();
        let db = runtime
            .spawn(async move { slatedb::Db::open(path, object_store).await })
            .await
            .map_err(|e| Error::Other(format!("slatedb open join: {e}")))?
            .map_err(|e| Error::Other(format!("slatedb open: {e}")))?;

        Ok(Self {
            inner: Arc::new(db),
            runtime: Some(runtime),
        })
    }
}

impl SlateKvStore {
    /// Run `fut` on the owned SlateDB runtime and block the caller until it
    /// completes, returning the future's output.
    ///
    /// Uses `Handle::spawn` + `std::sync::mpsc` rather than `Runtime::block_on`
    /// so the caller may itself be inside a tokio runtime (e.g. a
    /// `#[tokio::test]` function). `Runtime::block_on` panics in that case
    /// ("Cannot start a runtime from within a runtime"); `Handle::spawn`
    /// does not, and the std channel `recv` is a plain synchronous park.
    fn bridge<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel::<T>(1);
        self.runtime().handle().spawn(async move {
            let out = fut.await;
            let _ = tx.send(out);
        });
        rx.recv()
            .expect("slatedb bridge task dropped before sending")
    }
}

impl Drop for SlateKvStore {
    fn drop(&mut self) {
        // No inline `db.close().await`: SlateDB's WAL + SSTs on object
        // storage already preserve committed writes, so close is only
        // best-effort (flushing the final in-memory batch). Blocking the
        // caller thread on it would stall tokio workers during normal
        // async-context drops; the win is not worth the cost.
        //
        // The `Arc<Db>` also goes away when the last reference drops — if
        // that happens on the OS thread below (via the owned runtime's
        // internal task cleanup), background flushes have a chance to
        // complete. Anything in flight at process exit relies on SlateDB's
        // own durability story, not ours.
        //
        // If we own the last reference to the runtime, its `Drop` would
        // block waiting for worker threads to join — which panics when
        // `drop` runs inside an async context:
        //   "Cannot drop a runtime in a context where blocking is not allowed."
        // Move the `Arc<Runtime>` onto a plain OS thread so the blocking
        // shutdown happens off the async context. If this `Arc` is not the
        // last reference, the drop on the other thread is cheap; if it is,
        // the join happens there instead of here.
        if let Some(runtime) = self.runtime.take() {
            std::thread::spawn(move || drop(runtime));
        }
    }
}

use crate::kv::{KvIter, KvSnapshot, KvStore, WriteOp};

impl KvStore for SlateKvStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let db = self.inner.clone();
        let key = key.to_vec();
        self.bridge(async move { db.get(&key).await })
            .map_err(|e| Error::Other(format!("slatedb get: {e}")))
            .map(|opt| opt.map(|bytes| bytes.to_vec()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let db = self.inner.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        self.bridge(async move { db.put(&key, &value).await })
            .map_err(|e| Error::Other(format!("slatedb put: {e}")))
            .map(|_handle| ())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let db = self.inner.clone();
        let key = key.to_vec();
        self.bridge(async move { db.delete(&key).await })
            .map_err(|e| Error::Other(format!("slatedb delete: {e}")))
            .map(|_handle| ())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let db = self.inner.clone();
        let prefix = prefix.to_vec();
        // Eagerly collect into an owned Vec so the returned KvIter is 'static
        // and doesn't borrow from self or the runtime.
        let collected = self
            .bridge(async move {
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                let mut it = db.scan_prefix(&prefix[..]).await?;
                while let Some(kv) = it.next().await? {
                    out.push((kv.key.to_vec(), kv.value.to_vec()));
                }
                Ok::<_, slatedb::Error>(out)
            })
            .map_err(|e| Error::Other(format!("slatedb scan_prefix: {e}")))?;
        Ok(Box::new(collected.into_iter().map(Ok)))
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let db = self.inner.clone();
        self.bridge(async move {
            let mut batch = slatedb::WriteBatch::new();
            for op in ops {
                match op {
                    WriteOp::Put { key, value } => batch.put(&key, &value),
                    WriteOp::Delete { key } => batch.delete(&key),
                }
            }
            db.write(batch).await
        })
        .map_err(|e| Error::Other(format!("slatedb write_batch: {e}")))
        .map(|_handle| ())
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn KvSnapshot + 'a>> {
        let db = self.inner.clone();
        let snap = self
            .bridge(async move { db.snapshot().await })
            .map_err(|e| Error::Other(format!("slatedb snapshot: {e}")))?;
        Ok(Box::new(SlateSnapshot {
            snap,
            runtime: Some(self.runtime_arc()),
        }))
    }

    fn snapshot_owned(self: Arc<Self>) -> Result<Arc<dyn KvSnapshot>> {
        let db = self.inner.clone();
        let snap = self
            .bridge(async move { db.snapshot().await })
            .map_err(|e| Error::Other(format!("slatedb snapshot_owned: {e}")))?;
        Ok(Arc::new(SlateSnapshot {
            snap,
            runtime: Some(self.runtime_arc()),
        }))
    }
}

/// `KvSnapshot` backed by a SlateDB `Arc<DbSnapshot>`. Since `DbSnapshot`
/// itself is already an Arc-shaped owned handle from SlateDB, one type
/// serves both `KvStore::snapshot` and `KvStore::snapshot_owned` — we
/// don't need separate borrowed / owned variants.
struct SlateSnapshot {
    snap: Arc<slatedb::DbSnapshot>,
    runtime: Option<Arc<Runtime>>,
}

impl Drop for SlateSnapshot {
    fn drop(&mut self) {
        // Mirror `SlateKvStore::Drop`: `snapshot_owned` contracts that the
        // snapshot is `'static` and may outlive the store, so this drop can
        // land inside an async context with the last `Arc<Runtime>` in hand.
        // Pushing the `Arc` to a plain OS thread avoids the "Cannot drop a
        // runtime in a context where blocking is not allowed" panic.
        if let Some(runtime) = self.runtime.take() {
            std::thread::spawn(move || drop(runtime));
        }
    }
}

impl SlateSnapshot {
    /// Mirror of `SlateKvStore::bridge` — spawns a future on the owned
    /// runtime and blocks the caller via an std channel. See
    /// `SlateKvStore::bridge` for the rationale (avoids the
    /// `Runtime::block_on` panic when called from inside another tokio
    /// runtime, e.g. a `#[tokio::test]` function).
    fn bridge<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let runtime = self
            .runtime
            .as_ref()
            .expect("slate snapshot runtime accessed after Drop took it");
        let (tx, rx) = std::sync::mpsc::sync_channel::<T>(1);
        runtime.handle().spawn(async move {
            let out = fut.await;
            let _ = tx.send(out);
        });
        rx.recv()
            .expect("slatedb snapshot bridge task dropped before sending")
    }
}

impl KvSnapshot for SlateSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snap = self.snap.clone();
        let key = key.to_vec();
        self.bridge(async move { snap.get(&key).await })
            .map_err(|e| Error::Other(format!("slatedb snapshot get: {e}")))
            .map(|opt| opt.map(|bytes| bytes.to_vec()))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        let snap = self.snap.clone();
        let prefix = prefix.to_vec();
        let collected = self
            .bridge(async move {
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                let mut it = snap.scan_prefix(&prefix[..]).await?;
                while let Some(kv) = it.next().await? {
                    out.push((kv.key.to_vec(), kv.value.to_vec()));
                }
                Ok::<_, slatedb::Error>(out)
            })
            .map_err(|e| Error::Other(format!("slatedb snapshot scan_prefix: {e}")))?;
        Ok(Box::new(collected.into_iter().map(Ok)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::KvStore;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn open_test() -> (TempDir, SlateKvStore) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let kv = SlateKvStore::open(store, "agentdb/slate").await.unwrap();
        (dir, kv)
    }

    #[tokio::test]
    async fn put_then_get_roundtrip() {
        let (_dir, kv) = open_test().await;
        kv.put(b"k1", b"v1").unwrap();
        assert_eq!(kv.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let (_dir, kv) = open_test().await;
        kv.put(b"k1", b"v1").unwrap();
        kv.delete(b"k1").unwrap();
        assert!(kv.get(b"k1").unwrap().is_none());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (_dir, kv) = open_test().await;
        assert!(kv.get(b"nope").unwrap().is_none());
    }

    #[tokio::test]
    async fn scan_prefix_returns_only_matching_keys_in_order() {
        let (_dir, kv) = open_test().await;
        kv.put(b"pre1/a", b"v1a").unwrap();
        kv.put(b"pre1/b", b"v1b").unwrap();
        kv.put(b"pre1/c", b"v1c").unwrap();
        kv.put(b"pre2/a", b"v2a").unwrap();
        kv.put(b"pre2/b", b"v2b").unwrap();

        let got: Vec<Vec<u8>> = kv
            .scan_prefix(b"pre1/")
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(
            got,
            vec![b"pre1/a".to_vec(), b"pre1/b".to_vec(), b"pre1/c".to_vec()],
        );
    }

    #[tokio::test]
    async fn scan_prefix_on_empty_store_returns_no_rows() {
        let (_dir, kv) = open_test().await;
        assert_eq!(kv.scan_prefix(b"whatever/").unwrap().count(), 0);
    }

    #[tokio::test]
    async fn scan_prefix_with_no_matches_returns_no_rows() {
        let (_dir, kv) = open_test().await;
        kv.put(b"other/x", b"x").unwrap();
        assert_eq!(kv.scan_prefix(b"missing/").unwrap().count(), 0);
    }

    #[tokio::test]
    async fn write_batch_applies_mixed_put_and_delete() {
        let (_dir, kv) = open_test().await;
        kv.put(b"keep", b"pre-keep").unwrap();
        kv.put(b"drop", b"pre-drop").unwrap();

        kv.write_batch(vec![
            WriteOp::Put {
                key: b"keep".to_vec(),
                value: b"post-keep".to_vec(),
            },
            WriteOp::Delete {
                key: b"drop".to_vec(),
            },
            WriteOp::Put {
                key: b"new".to_vec(),
                value: b"new".to_vec(),
            },
        ])
        .unwrap();

        assert_eq!(
            kv.get(b"keep").unwrap().as_deref(),
            Some(b"post-keep".as_ref())
        );
        assert_eq!(kv.get(b"drop").unwrap(), None);
        assert_eq!(kv.get(b"new").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[tokio::test]
    async fn empty_write_batch_is_noop() {
        let (_dir, kv) = open_test().await;
        kv.write_batch(Vec::new()).unwrap();
    }

    #[tokio::test]
    async fn snapshot_does_not_see_writes_after_open() {
        let (_dir, kv) = open_test().await;
        kv.put(b"k1", b"v1").unwrap();
        let snap = kv.snapshot().unwrap();

        kv.put(b"k1", b"v2").unwrap();
        kv.put(b"k2", b"v2").unwrap();

        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert!(snap.get(b"k2").unwrap().is_none());
    }

    #[tokio::test]
    async fn snapshot_scan_prefix_is_consistent() {
        let (_dir, kv) = open_test().await;
        kv.put(b"pre/a", b"v1").unwrap();
        kv.put(b"pre/b", b"v2").unwrap();
        kv.put(b"pre/c", b"v3").unwrap();

        let snap = kv.snapshot().unwrap();
        // Write a 4th AFTER snapshot opens.
        kv.put(b"pre/d", b"v4").unwrap();

        let got: Vec<Vec<u8>> = snap
            .scan_prefix(b"pre/")
            .unwrap()
            .map(|r| r.unwrap().1)
            .collect();
        assert_eq!(
            got,
            vec![b"v1".to_vec(), b"v2".to_vec(), b"v3".to_vec()],
            "snapshot must not see the post-open write"
        );
    }

    #[tokio::test]
    async fn snapshot_owned_crosses_await() {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let kv: Arc<SlateKvStore> =
            Arc::new(SlateKvStore::open(store, "agentdb/slate").await.unwrap());
        kv.put(b"k1", b"v1").unwrap();

        let snap = Arc::clone(&kv).snapshot_owned().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        assert_eq!(snap.get(b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
    }
}
