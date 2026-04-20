//! SlateDB-backed `KvStore`. See
//! docs/superpowers/specs/2026-04-19-m11-slatedb-s3-design.md
//! and docs/superpowers/notes/2026-04-19-m11-slatedb-spike.md.

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
    #[allow(dead_code)]
    pub(crate) inner: Arc<slatedb::Db>,
    #[allow(dead_code)]
    pub(crate) runtime: Arc<Runtime>,
}

impl SlateKvStore {
    /// Open (or create) a SlateDB database rooted at `path` inside `object_store`.
    /// `path` is an object-store prefix like `"agentdb/slate"` (no leading slash).
    pub async fn open(object_store: Arc<dyn ObjectStore>, path: &str) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| Error::Other(format!("slatedb open: {e}")))?;

        // Dedicated multi-thread runtime. One worker is enough for the
        // sync-over-async bridge plus SlateDB's background compaction.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("slatedb")
            .build()
            .map_err(|e| Error::Other(format!("slatedb runtime: {e}")))?;

        Ok(Self {
            inner: Arc::new(db),
            runtime: Arc::new(runtime),
        })
    }
}

impl Drop for SlateKvStore {
    fn drop(&mut self) {
        // Owned runtime => no task of ours is live here, so block_on is safe.
        // Best-effort close: SlateDB's WAL+SSTs on object storage already
        // preserve committed writes, so a failed close only loses the final
        // in-memory batch.
        let db = self.inner.clone();
        let rt = self.runtime.clone();
        if let Err(err) = rt.block_on(async move { db.close().await }) {
            tracing::warn!(?err, "slatedb close failed during Drop");
        }
    }
}
