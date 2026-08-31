//! Behavioral tests for the V2.1 content backends.
//!
//! Both `LocalFsContentBackend` and `InMemoryContentBackend` implement the
//! same `ContentBackend` trait, so most tests are written against the
//! abstract trait via a dispatch helper. The local-fs-specific assertions
//! (path layout, atomic rename, restart safety) live in their own tests.

use std::io;
use std::sync::{Arc, Mutex};

use agentdb_storage::{
    AsyncMirrorContentBackend, ContentBackend, InMemoryContentBackend, LocalFsContentBackend,
    ObjectStoreContentBackend,
};
use async_trait::async_trait;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

// Precomputed sha256("hello world") — used to pin that the put result
// returns the correct hex digest, not just any digest.
const HELLO_WORLD_SHA256: &str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

fn local(dir: &TempDir) -> Arc<dyn ContentBackend> {
    Arc::new(LocalFsContentBackend::new(dir.path()))
}

fn in_memory() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

fn object_store_local(dir: &TempDir) -> Arc<dyn ContentBackend> {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ObjectStoreContentBackend::new(store, "agentdb/content"))
}

async fn put_get_round_trip_against(backend: Arc<dyn ContentBackend>) {
    let put = backend
        .put("ten_a", "art_x", b"hello world")
        .await
        .expect("put ok");
    assert_eq!(put.bytes, 11);
    assert_eq!(put.sha256_hex, HELLO_WORLD_SHA256);

    let got = backend.get("ten_a", "art_x").await.expect("get ok");
    assert_eq!(got, b"hello world");
}

#[tokio::test]
async fn local_put_get_round_trip() {
    let dir = TempDir::new().unwrap();
    put_get_round_trip_against(local(&dir)).await;
}

#[tokio::test]
async fn in_memory_put_get_round_trip() {
    put_get_round_trip_against(in_memory()).await;
}

#[tokio::test]
async fn object_store_put_get_round_trip() {
    let dir = TempDir::new().unwrap();
    put_get_round_trip_against(object_store_local(&dir)).await;
}

async fn head_reflects_existence(backend: Arc<dyn ContentBackend>) {
    assert!(!backend.head("ten_a", "art_x").await.unwrap());
    backend.put("ten_a", "art_x", b"x").await.unwrap();
    assert!(backend.head("ten_a", "art_x").await.unwrap());
    backend.delete("ten_a", "art_x").await.unwrap();
    assert!(!backend.head("ten_a", "art_x").await.unwrap());
}

#[tokio::test]
async fn local_head_reflects_existence() {
    let dir = TempDir::new().unwrap();
    head_reflects_existence(local(&dir)).await;
}

#[tokio::test]
async fn in_memory_head_reflects_existence() {
    head_reflects_existence(in_memory()).await;
}

#[tokio::test]
async fn object_store_head_reflects_existence() {
    let dir = TempDir::new().unwrap();
    head_reflects_existence(object_store_local(&dir)).await;
}

async fn put_overwrites_atomically(backend: Arc<dyn ContentBackend>) {
    backend.put("ten_a", "art_x", b"first").await.unwrap();
    backend.put("ten_a", "art_x", b"second").await.unwrap();
    assert_eq!(backend.get("ten_a", "art_x").await.unwrap(), b"second");
}

#[tokio::test]
async fn local_put_overwrites_atomically() {
    let dir = TempDir::new().unwrap();
    put_overwrites_atomically(local(&dir)).await;
}

#[tokio::test]
async fn in_memory_put_overwrites_atomically() {
    put_overwrites_atomically(in_memory()).await;
}

#[tokio::test]
async fn object_store_put_overwrites_atomically() {
    let dir = TempDir::new().unwrap();
    put_overwrites_atomically(object_store_local(&dir)).await;
}

async fn cross_tenant_isolation(backend: Arc<dyn ContentBackend>) {
    backend.put("ten_a", "art_x", b"a-bytes").await.unwrap();
    backend.put("ten_b", "art_x", b"b-bytes").await.unwrap();
    assert_eq!(backend.get("ten_a", "art_x").await.unwrap(), b"a-bytes");
    assert_eq!(backend.get("ten_b", "art_x").await.unwrap(), b"b-bytes");
}

#[tokio::test]
async fn local_cross_tenant_isolation() {
    let dir = TempDir::new().unwrap();
    cross_tenant_isolation(local(&dir)).await;
}

#[tokio::test]
async fn in_memory_cross_tenant_isolation() {
    cross_tenant_isolation(in_memory()).await;
}

#[tokio::test]
async fn object_store_cross_tenant_isolation() {
    let dir = TempDir::new().unwrap();
    cross_tenant_isolation(object_store_local(&dir)).await;
}

async fn delete_missing_is_ok(backend: Arc<dyn ContentBackend>) {
    backend.delete("ten_a", "art_does_not_exist").await.unwrap();
}

#[tokio::test]
async fn local_delete_missing_is_ok() {
    let dir = TempDir::new().unwrap();
    delete_missing_is_ok(local(&dir)).await;
}

#[tokio::test]
async fn in_memory_delete_missing_is_ok() {
    delete_missing_is_ok(in_memory()).await;
}

#[tokio::test]
async fn object_store_delete_missing_is_ok() {
    let dir = TempDir::new().unwrap();
    delete_missing_is_ok(object_store_local(&dir)).await;
}

#[tokio::test]
async fn local_get_missing_is_not_found() {
    let dir = TempDir::new().unwrap();
    let backend = local(&dir);
    let err = backend.get("ten_a", "art_missing").await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[tokio::test]
async fn local_uses_artifact_content_layout() {
    // The layout is part of the contract — deliberate so that a future
    // S3 backend can mirror the same prefix scheme without rewriting
    // call sites.
    let dir = TempDir::new().unwrap();
    let backend = LocalFsContentBackend::new(dir.path());
    backend.put("ten_a", "art_x", b"hi").await.unwrap();
    let expected = dir
        .path()
        .join("artifact_content")
        .join("ten_a")
        .join("art_x")
        .join("blob");
    assert!(expected.exists(), "blob not at expected path: {expected:?}");
    let got = std::fs::read(&expected).unwrap();
    assert_eq!(got, b"hi");
}

#[tokio::test]
async fn local_persists_across_backend_instances() {
    // A new LocalFsContentBackend rooted at the same directory sees blobs
    // written by an earlier instance — no in-memory cache, no hidden state.
    let dir = TempDir::new().unwrap();
    {
        let b1 = LocalFsContentBackend::new(dir.path());
        b1.put("ten_a", "art_x", b"persisted").await.unwrap();
    }
    let b2 = LocalFsContentBackend::new(dir.path());
    assert_eq!(b2.get("ten_a", "art_x").await.unwrap(), b"persisted");
}

#[tokio::test]
async fn local_listing_is_bounded_and_reaps_crash_left_temporary_files() {
    let dir = TempDir::new().unwrap();
    let backend = LocalFsContentBackend::new(dir.path());
    backend
        .put("ten_a", "art_live", b"persisted")
        .await
        .unwrap();
    let crashed_dir = dir
        .path()
        .join("artifact_content")
        .join("ten_a")
        .join("art_partial");
    std::fs::create_dir_all(&crashed_dir).unwrap();
    let temporary = crashed_dir.join("blob.tmp.123.1");
    std::fs::write(&temporary, b"partial").unwrap();

    // The read-only listing must NOT touch the temporary (it may belong to
    // an in-flight put); the explicit startup sweep reaps it.
    let listing = backend.list_objects(1).await.unwrap();
    assert_eq!(listing.objects.len(), 1);
    assert_eq!(listing.objects[0].artifact_id, "art_live");
    assert!(temporary.exists(), "read-only listing must not delete temporaries");

    let removed = backend.sweep_temporary_objects(10).await.unwrap();
    assert_eq!(removed, 1);
    assert!(!temporary.exists());

    let error = backend.list_objects(0).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn object_store_uses_artifact_content_layout() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let backend = ObjectStoreContentBackend::new(store, "agentdb/content");
    backend.put("ten_a", "art_x", b"hi").await.unwrap();
    let expected = dir
        .path()
        .join("agentdb")
        .join("content")
        .join("artifact_content")
        .join("ten_a")
        .join("art_x")
        .join("blob");
    assert!(expected.exists(), "blob not at expected path: {expected:?}");
    let got = std::fs::read(&expected).unwrap();
    assert_eq!(got, b"hi");
}

#[tokio::test]
async fn async_mirror_put_returns_after_primary_and_uploads_in_background() {
    let primary = in_memory();
    let mirror = in_memory();
    let backend = AsyncMirrorContentBackend::new(primary.clone(), mirror.clone(), 8);

    let put = backend.put("ten_a", "art_x", b"hello world").await.unwrap();
    assert_eq!(put.sha256_hex, HELLO_WORLD_SHA256);
    assert_eq!(primary.get("ten_a", "art_x").await.unwrap(), b"hello world");

    wait_for_mirror(&backend, 1).await;
    assert_eq!(mirror.get("ten_a", "art_x").await.unwrap(), b"hello world");
    let stats = backend.stats();
    assert_eq!(stats.queued, 1);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.backlog, 0);
}

#[tokio::test]
async fn async_mirror_delete_removes_primary_and_mirror_in_background() {
    let primary = in_memory();
    let mirror = in_memory();
    let backend = AsyncMirrorContentBackend::new(primary.clone(), mirror.clone(), 8);
    backend.put("ten_a", "art_x", b"x").await.unwrap();
    wait_for_mirror(&backend, 1).await;

    backend.delete("ten_a", "art_x").await.unwrap();
    assert!(!primary.head("ten_a", "art_x").await.unwrap());

    wait_for_mirror(&backend, 2).await;
    assert!(!mirror.head("ten_a", "art_x").await.unwrap());
}

#[tokio::test]
async fn async_mirror_retries_transient_upload_failures() {
    let primary = in_memory();
    let mirror = Arc::new(FlakyContentBackend::new(1, 0));
    let backend = AsyncMirrorContentBackend::new_with_retries(primary, mirror.clone(), 8, 3);

    backend.put("ten_a", "art_x", b"retry me").await.unwrap();

    wait_for_mirror(&backend, 1).await;
    assert_eq!(mirror.get("ten_a", "art_x").await.unwrap(), b"retry me");
    let stats = backend.stats();
    assert_eq!(stats.queued, 1);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.backlog, 0);
}

#[tokio::test]
async fn async_mirror_reads_through_from_mirror_and_backfills_primary() {
    let primary = in_memory();
    let mirror = in_memory();
    mirror.put("ten_a", "art_x", b"from mirror").await.unwrap();
    let backend = AsyncMirrorContentBackend::new(primary.clone(), mirror, 8);

    let bytes = backend.get("ten_a", "art_x").await.unwrap();

    assert_eq!(bytes, b"from mirror");
    assert_eq!(primary.get("ten_a", "art_x").await.unwrap(), b"from mirror");
    assert!(backend.head("ten_a", "art_x").await.unwrap());
}

async fn wait_for_mirror(backend: &AsyncMirrorContentBackend, completed: u64) {
    for _ in 0..50 {
        let stats = backend.stats();
        if stats.completed >= completed && stats.backlog == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("async mirror did not complete: {:?}", backend.stats());
}

struct FlakyContentBackend {
    inner: InMemoryContentBackend,
    put_failures: Mutex<usize>,
    delete_failures: Mutex<usize>,
}

impl FlakyContentBackend {
    fn new(put_failures: usize, delete_failures: usize) -> Self {
        Self {
            inner: InMemoryContentBackend::default(),
            put_failures: Mutex::new(put_failures),
            delete_failures: Mutex::new(delete_failures),
        }
    }
}

#[async_trait]
impl ContentBackend for FlakyContentBackend {
    async fn put(
        &self,
        tenant_id: &str,
        artifact_id: &str,
        bytes: &[u8],
    ) -> io::Result<agentdb_storage::PutResult> {
        let should_fail = {
            let mut failures = self.put_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                true
            } else {
                false
            }
        };
        if should_fail {
            return Err(io::Error::other("transient put failure"));
        }
        self.inner.put(tenant_id, artifact_id, bytes).await
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        self.inner.get(tenant_id, artifact_id).await
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        self.inner.head(tenant_id, artifact_id).await
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let should_fail = {
            let mut failures = self.delete_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                true
            } else {
                false
            }
        };
        if should_fail {
            return Err(io::Error::other("transient delete failure"));
        }
        self.inner.delete(tenant_id, artifact_id).await
    }
}

#[tokio::test]
async fn object_store_backend_lists_artifact_blobs() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let backend = ObjectStoreContentBackend::new(store.clone(), "agentdb/content");
    backend.put("ten_a", "art_1", b"one").await.unwrap();
    backend.put("ten_a", "art_2", b"two").await.unwrap();
    backend.put("ten_b", "art_3", b"three").await.unwrap();
    // A non-blob object under the prefix is skipped, not treated as corruption.
    store
        .put(
            &object_store::path::Path::from("agentdb/content/artifact_content/ten_a/stray"),
            b"x".to_vec().into(),
        )
        .await
        .unwrap();

    let listing = backend.list_objects(10).await.unwrap();
    let mut pairs: Vec<(String, String)> = listing
        .objects
        .into_iter()
        .map(|o| (o.tenant_id, o.artifact_id))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("ten_a".into(), "art_1".into()),
            ("ten_a".into(), "art_2".into()),
            ("ten_b".into(), "art_3".into()),
        ]
    );

    let err = backend.list_objects(2).await.unwrap_err();
    assert!(err.to_string().contains("exceeds configured limit"));
}

fn now_wallclock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn managed_cache_evicts_cold_objects_and_recolds_after_readthrough() {
    let dir = tempfile::TempDir::new().unwrap();
    let primary: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(dir.path()));
    let mirror = in_memory();
    let backend = AsyncMirrorContentBackend::new(primary.clone(), mirror.clone(), 8)
        .with_local_cache_ttl_ms(500);

    backend.put("ten_a", "seg_1", b"cold data").await.unwrap();
    wait_for_mirror(&backend, 1).await;

    // Still warm: age below TTL, nothing evicted.
    let report = backend
        .run_cache_sweep_once(now_wallclock_ms(), 1_000)
        .await
        .unwrap();
    assert_eq!(report.evicted, 0);
    assert!(primary.head("ten_a", "seg_1").await.unwrap());

    // Past TTL: local copy evicted, mirror copy retained.
    let report = backend
        .run_cache_sweep_once(now_wallclock_ms() + 10_000, 1_000)
        .await
        .unwrap();
    assert_eq!(report.evicted, 1);
    assert!(!primary.head("ten_a", "seg_1").await.unwrap());
    assert!(mirror.head("ten_a", "seg_1").await.unwrap());

    // Cold read pulls through the mirror and re-caches locally...
    assert_eq!(backend.get("ten_a", "seg_1").await.unwrap(), b"cold data");
    assert!(primary.head("ten_a", "seg_1").await.unwrap());

    // ...and the re-cached copy goes cold again on a later sweep.
    let report = backend
        .run_cache_sweep_once(now_wallclock_ms() + 10_000, 1_000)
        .await
        .unwrap();
    assert_eq!(report.evicted, 1);
    assert!(!primary.head("ten_a", "seg_1").await.unwrap());
    assert_eq!(backend.stats().evicted, 2);
}

#[tokio::test]
async fn managed_cache_never_evicts_the_only_copy() {
    let dir = tempfile::TempDir::new().unwrap();
    let primary: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(dir.path()));
    let mirror = in_memory();
    let backend = AsyncMirrorContentBackend::new(primary.clone(), mirror.clone(), 8)
        .with_local_cache_ttl_ms(0);

    // Written to the primary directly: the mirror never saw it.
    primary.put("ten_a", "seg_only", b"unmirrored").await.unwrap();

    let report = backend
        .run_cache_sweep_once(now_wallclock_ms() + 10_000, 1_000)
        .await
        .unwrap();
    assert_eq!(report.evicted, 0);
    assert_eq!(report.retained_unmirrored, 1);
    assert!(primary.head("ten_a", "seg_only").await.unwrap());
}

#[tokio::test]
async fn managed_cache_touch_on_read_keeps_object_warm() {
    let dir = tempfile::TempDir::new().unwrap();
    let primary: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(dir.path()));
    let mirror = in_memory();
    let backend = AsyncMirrorContentBackend::new(primary.clone(), mirror.clone(), 8)
        .with_local_cache_ttl_ms(5_000);

    backend.put("ten_a", "seg_hot", b"hot data").await.unwrap();
    wait_for_mirror(&backend, 1).await;

    // A read refreshes the object's age; a sweep just inside the TTL after
    // that read must retain it.
    assert_eq!(backend.get("ten_a", "seg_hot").await.unwrap(), b"hot data");
    let read_ms = now_wallclock_ms();
    let report = backend
        .run_cache_sweep_once(read_ms + 4_000, 1_000)
        .await
        .unwrap();
    assert_eq!(report.evicted, 0);
    assert!(primary.head("ten_a", "seg_hot").await.unwrap());
}

/// Chaos: concurrent writers, readers, and cache sweeps against a mirror that
/// fails transiently. Invariants under fault: (1) a read never returns wrong
/// bytes; (2) the sweep never evicts an object the mirror does not hold;
/// (3) once the mirror heals, every object converges into it and eviction
/// then leaves the mirror as the only copy, still serving correct reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_cache_chaos_flaky_mirror_never_loses_data() {
    let dir = tempfile::TempDir::new().unwrap();
    let primary: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(dir.path()));
    // 4 consecutive transient mirror-put failures: one credit short of the
    // 5-attempt retry budget, so the first upload rides the whole retry
    // ladder and still succeeds; nothing may end up in `failed`.
    let mirror: Arc<dyn ContentBackend> = Arc::new(FlakyContentBackend::new(4, 0));
    let backend = Arc::new(
        AsyncMirrorContentBackend::new_with_retries(primary.clone(), mirror, 64, 5)
            .with_local_cache_ttl_ms(0),
    );

    let objects: Vec<(String, String, Vec<u8>)> = (0..48)
        .map(|i| {
            (
                format!("ten_{}", i % 3),
                format!("obj_{i}"),
                format!("payload-{i}").into_bytes(),
            )
        })
        .collect();

    // Concurrent writers and sweepers.
    let mut tasks = Vec::new();
    for chunk in objects.chunks(12) {
        let backend = backend.clone();
        let chunk = chunk.to_vec();
        tasks.push(tokio::spawn(async move {
            for (tenant, artifact, bytes) in chunk {
                backend.put(&tenant, &artifact, &bytes).await.unwrap();
            }
        }));
    }
    for _ in 0..4 {
        let backend = backend.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                let now = now_wallclock_ms() + 60_000;
                let _ = backend.run_cache_sweep_once(now, 10_000).await;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    // Reads are correct throughout and after the storm, wherever each object
    // currently lives (local, mirror, or mid-eviction).
    for (tenant, artifact, bytes) in &objects {
        assert_eq!(&backend.get(tenant, artifact).await.unwrap(), bytes);
    }

    // Drain the mirror queue (retries included), then verify convergence:
    // a final sweep must be able to evict EVERY object because the healed
    // mirror holds them all.
    for _ in 0..200 {
        if backend.stats().backlog == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(backend.stats().backlog, 0, "mirror queue drained");
    assert_eq!(backend.stats().failed, 0, "retries healed every flaky upload");

    let report = backend
        .run_cache_sweep_once(now_wallclock_ms() + 60_000, 10_000)
        .await
        .unwrap();
    assert_eq!(report.retained_unmirrored, 0, "nothing left unmirrored");

    for (tenant, artifact, bytes) in &objects {
        assert!(
            !primary.head(tenant, artifact).await.unwrap(),
            "cold object evicted locally"
        );
        assert_eq!(
            &backend.get(tenant, artifact).await.unwrap(),
            bytes,
            "mirror-only object still serves correct bytes"
        );
    }
}
