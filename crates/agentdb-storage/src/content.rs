//! Artifact content backend — abstraction over where artifact bytes live.
//!
//! The runtime metadata/event store (`KvStore`) holds artifact metadata; the
//! `ContentBackend` holds the bytes. V2.1 ships two impls:
//!
//! - [`LocalFsContentBackend`]: writes blobs to a directory rooted on the
//!   local filesystem under `artifact_content/{tenant_id}/{artifact_id}/blob`.
//!   Used by the persistent runtime and exercised by the restart tests.
//! - [`InMemoryContentBackend`]: holds blobs in a process-local map. Used by
//!   `Runtime::in_memory()` and unit tests.
//!
//! A future S3-compatible backend (V2.4+) implements the same trait and
//! becomes a swap for `LocalFsContentBackend` without touching the runtime.
//!
//! ## Atomicity
//!
//! `LocalFsContentBackend::put` writes to a tmp path under the same parent
//! directory and renames into place. The rename is atomic on POSIX, so a
//! reader either sees the old (or absent) blob or the new one — never a
//! partial write. The tmp file is fsynced before the rename so a crash
//! between rename and the next write doesn't leave an unflushed blob.
//!
//! Parent-directory fsync (which would survive a crash that destroys the
//! directory entry) is intentionally deferred to V2.4+; it costs an extra
//! syscall per put and the V2.1 prototype doesn't need it.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

/// Result of a successful put: the byte count written and the sha256 we
/// computed while writing. Callers compare `sha256_hex` against any
/// caller-provided expectation; the metadata layer stores it on the
/// `Artifact` record so future reads can verify integrity.
#[derive(Debug, Clone)]
pub struct PutResult {
    pub bytes: u64,
    pub sha256_hex: String,
}

/// Abstraction over where artifact bytes live.
///
/// All methods are tenant- and artifact-scoped. The trait does not surface a
/// concept of "current tenant" — every call passes both ids — so the runtime
/// can multiplex many tenants over one backend without per-tenant state.
#[async_trait]
pub trait ContentBackend: Send + Sync {
    /// Atomically write `bytes` at the logical path `tenant_id/artifact_id`,
    /// returning the byte count and the sha256 hex digest computed while
    /// writing. Implementations MUST guarantee that on a successful return
    /// the bytes are durable (or the in-memory equivalent) and a concurrent
    /// reader sees either the new content or no content at that path.
    async fn put(
        &self,
        tenant_id: &str,
        artifact_id: &str,
        bytes: &[u8],
    ) -> io::Result<PutResult>;

    /// Read the entire blob into memory. V2.1 does not stream; large blobs
    /// are memory-bound. TODO(V2.4): expose a streaming reader.
    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>>;

    /// Existence check. Returns `Ok(false)` if the blob is absent;
    /// propagates other I/O errors.
    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool>;

    /// Best-effort delete. Removing a missing blob is NOT an error — the
    /// V2.4 GC reaper relies on idempotent delete to clean up orphans.
    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()>;
}

// ----- LocalFsContentBackend --------------------------------------------------

/// Filesystem-backed content store. Writes blobs under
/// `{root}/artifact_content/{tenant_id}/{artifact_id}/blob`.
pub struct LocalFsContentBackend {
    root: PathBuf,
}

impl LocalFsContentBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn dir_for(&self, tenant_id: &str, artifact_id: &str) -> PathBuf {
        self.root
            .join("artifact_content")
            .join(tenant_id)
            .join(artifact_id)
    }

    fn path_for(&self, tenant_id: &str, artifact_id: &str) -> PathBuf {
        self.dir_for(tenant_id, artifact_id).join("blob")
    }

    fn tmp_path_for(&self, tenant_id: &str, artifact_id: &str) -> PathBuf {
        // Random suffix so concurrent puts of the same artifact_id (which
        // shouldn't normally happen — artifact_ids are UUIDv7 and unique
        // per call — but defensive) don't collide on the tmp path. We use a
        // monotonic-ish nanosecond timestamp combined with a counter
        // synthesized from the timestamp's low bits; that's good enough to
        // avoid same-process collisions.
        use std::time::{SystemTime, UNIX_EPOCH};
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let suffix = format!(".tmp.{ns}");
        self.dir_for(tenant_id, artifact_id)
            .join(format!("blob{suffix}"))
    }
}

#[async_trait]
impl ContentBackend for LocalFsContentBackend {
    async fn put(
        &self,
        tenant_id: &str,
        artifact_id: &str,
        bytes: &[u8],
    ) -> io::Result<PutResult> {
        let dir = self.dir_for(tenant_id, artifact_id);
        tokio::fs::create_dir_all(&dir).await?;

        let final_path = self.path_for(tenant_id, artifact_id);
        let tmp_path = self.tmp_path_for(tenant_id, artifact_id);

        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let sha256_hex = hex_encode(&hasher.finalize());

        // Write tmp + fsync + rename. tokio::fs::rename is atomic on POSIX.
        // TODO(V2.4): fsync the parent directory to make the rename durable
        // across power loss; not needed for the V2.1 prototype.
        {
            let mut f = tokio::fs::File::create(&tmp_path).await?;
            f.write_all(bytes).await?;
            f.sync_data().await?;
        }
        tokio::fs::rename(&tmp_path, &final_path).await?;

        Ok(PutResult {
            bytes: bytes.len() as u64,
            sha256_hex,
        })
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        let p = self.path_for(tenant_id, artifact_id);
        tokio::fs::read(&p).await
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        let p = self.path_for(tenant_id, artifact_id);
        match tokio::fs::metadata(&p).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let p = self.path_for(tenant_id, artifact_id);
        match tokio::fs::remove_file(&p).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // Best-effort cleanup of the empty parent dir. Ignore errors —
        // a non-empty dir (e.g. concurrent put left a tmp behind) just
        // means future GC will collect it.
        let _ = tokio::fs::remove_dir(self.dir_for(tenant_id, artifact_id)).await;
        Ok(())
    }
}

// ----- InMemoryContentBackend -------------------------------------------------

/// Process-local in-memory blob store. Used by `Runtime::in_memory()` and by
/// tests that don't exercise filesystem persistence. Goes alongside
/// `LocalFsContentBackend` so callers that want a content backend but no
/// disk I/O don't have to roll their own.
#[derive(Default)]
pub struct InMemoryContentBackend {
    blobs: Mutex<HashMap<(String, String), Vec<u8>>>,
}

#[async_trait]
impl ContentBackend for InMemoryContentBackend {
    async fn put(
        &self,
        tenant_id: &str,
        artifact_id: &str,
        bytes: &[u8],
    ) -> io::Result<PutResult> {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let sha256_hex = hex_encode(&hasher.finalize());

        let mut g = self.blobs.lock().expect("blobs mutex poisoned");
        g.insert(
            (tenant_id.to_string(), artifact_id.to_string()),
            bytes.to_vec(),
        );
        Ok(PutResult {
            bytes: bytes.len() as u64,
            sha256_hex,
        })
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        let g = self.blobs.lock().expect("blobs mutex poisoned");
        g.get(&(tenant_id.to_string(), artifact_id.to_string()))
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "artifact blob not found"))
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        let g = self.blobs.lock().expect("blobs mutex poisoned");
        Ok(g.contains_key(&(tenant_id.to_string(), artifact_id.to_string())))
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let mut g = self.blobs.lock().expect("blobs mutex poisoned");
        g.remove(&(tenant_id.to_string(), artifact_id.to_string()));
        Ok(())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
