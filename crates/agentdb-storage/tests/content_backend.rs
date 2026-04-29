//! Behavioral tests for the V2.1 content backends.
//!
//! Both `LocalFsContentBackend` and `InMemoryContentBackend` implement the
//! same `ContentBackend` trait, so most tests are written against the
//! abstract trait via a dispatch helper. The local-fs-specific assertions
//! (path layout, atomic rename, restart safety) live in their own tests.

use std::sync::Arc;

use agentdb_storage::{ContentBackend, InMemoryContentBackend, LocalFsContentBackend};
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
