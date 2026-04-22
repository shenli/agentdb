//! M15 Part B — hedged reads end-to-end against a slow-object-store
//! simulator.
//!
//! A `DelayingObjectStore` wraps an inner store and adds a configurable
//! delay to the first N `get_opts` calls. We call `read_segment_hedged`
//! directly against the wrapper — bypassing the full
//! `fetch_last_n_merged` plumbing — because what we want to verify is
//! the hedge primitive's end-to-end behavior against a real
//! `ObjectStore` impl: primary fires, delay elapses, secondary fires,
//! secondary wins, metrics record it.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use agentdb_core::segment::{read_segment_hedged, segment_path, write_segment, SegmentFile};
use agentdb_core::{BoxMetrics, Event, HlcTimestamp};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use serde_json::json;
use tempfile::TempDir;

/// Wraps an inner `ObjectStore` and adds a `delay` to the first
/// `slow_reads` `get_opts()` invocations. Every call increments
/// `observed_reads`.
#[derive(Debug)]
struct DelayingObjectStore {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
    slow_reads_remaining: AtomicU64,
    observed_reads: AtomicU64,
}

impl DelayingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, delay: Duration, slow_reads: u64) -> Self {
        Self {
            inner,
            delay,
            slow_reads_remaining: AtomicU64::new(slow_reads),
            observed_reads: AtomicU64::new(0),
        }
    }

    fn observed_reads(&self) -> u64 {
        self.observed_reads.load(Ordering::SeqCst)
    }

    async fn maybe_delay(&self) {
        self.observed_reads.fetch_add(1, Ordering::SeqCst);
        let should_delay = self
            .slow_reads_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v == 0 {
                    None
                } else {
                    Some(v - 1)
                }
            })
            .is_ok();
        if should_delay {
            tokio::time::sleep(self.delay).await;
        }
    }
}

impl std::fmt::Display for DelayingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelayingObjectStore({:?})", self.delay)
    }
}

#[async_trait]
impl ObjectStore for DelayingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.maybe_delay().await;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        self.maybe_delay().await;
        self.inner.get_range(location, range).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn mk_segment() -> SegmentFile {
    SegmentFile {
        schema_version: agentdb_core::SEGMENT_SCHEMA_VERSION,
        session_internal_id: 7,
        events: (1..=3)
            .map(|seq| Event {
                seq,
                ts: seq * 10,
                hlc_ts: HlcTimestamp::ZERO,
                actor_id: None,
                payload: json!({"n": seq}),
            })
            .collect(),
    }
}

async fn setup_with(delay: Duration, slow_reads: u64) -> (TempDir, Arc<DelayingObjectStore>, String) {
    use object_store::local::LocalFileSystem;
    let obj_dir = TempDir::new().unwrap();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let delaying = Arc::new(DelayingObjectStore::new(inner.clone(), delay, slow_reads));
    // Seed a segment via the inner store (no delay on writes).
    let path = segment_path(1, 1, 7, 1_700_000_000_000);
    write_segment(&inner, &path, &mk_segment()).await.unwrap();
    (obj_dir, delaying, path)
}

#[tokio::test]
#[serial_test::serial]
async fn hedged_secondary_wins_when_primary_slow() {
    std::env::set_var("AGENTDB_HEDGE_COLD_MS", "30");
    std::env::remove_var("AGENTDB_HEDGE_ENABLED");

    let (_d, delaying, path) = setup_with(Duration::from_secs(1), 1).await;
    let metrics = BoxMetrics::new();
    let store: Arc<dyn ObjectStore> = delaying.clone();

    let started = Instant::now();
    let seg = read_segment_hedged(&store, &path, agentdb_core::types::Tier::Cold, &metrics)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(seg.events.len(), 3, "segment contents must roundtrip");
    assert!(
        elapsed < Duration::from_millis(800),
        "hedged read exceeded budget: {elapsed:?}"
    );
    let observed = delaying.observed_reads();
    assert!(
        observed >= 2,
        "expected >=2 reads during hedge window; saw {observed}"
    );

    let snap = metrics.snapshot();
    assert_eq!(snap.hedged_triggers, 1, "secondary must have fired");
    assert_eq!(snap.hedged_secondary_wins, 1);
    assert_eq!(snap.hedged_primary_wins, 0);
}

#[tokio::test]
#[serial_test::serial]
async fn hedged_no_trigger_when_primary_is_fast() {
    std::env::set_var("AGENTDB_HEDGE_COLD_MS", "30");
    std::env::remove_var("AGENTDB_HEDGE_ENABLED");

    // Inner is plain + fast; 0 slow reads configured.
    let (_d, delaying, path) = setup_with(Duration::from_millis(0), 0).await;
    let metrics = BoxMetrics::new();
    let store: Arc<dyn ObjectStore> = delaying.clone();

    read_segment_hedged(&store, &path, agentdb_core::types::Tier::Cold, &metrics)
        .await
        .unwrap();

    assert_eq!(delaying.observed_reads(), 1, "exactly one read, no hedge");
    let snap = metrics.snapshot();
    assert_eq!(snap.hedged_triggers, 0);
    assert_eq!(snap.hedged_secondary_wins, 0);
    assert_eq!(snap.hedged_primary_wins, 0);
}

#[tokio::test]
#[serial_test::serial]
async fn hedged_disabled_kill_switch_bypasses_entirely() {
    std::env::set_var("AGENTDB_HEDGE_ENABLED", "false");
    std::env::set_var("AGENTDB_HEDGE_COLD_MS", "30");

    // Even though the primary is slow, with ENABLED=false we should NOT
    // fire a secondary — the caller sees the full primary latency.
    let (_d, delaying, path) = setup_with(Duration::from_millis(150), 1).await;
    let metrics = BoxMetrics::new();
    let store: Arc<dyn ObjectStore> = delaying.clone();

    let started = Instant::now();
    read_segment_hedged(&store, &path, agentdb_core::types::Tier::Cold, &metrics)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    // Must have waited out the full delay.
    assert!(
        elapsed >= Duration::from_millis(140),
        "kill switch should disable hedging; read took only {elapsed:?}"
    );
    assert_eq!(delaying.observed_reads(), 1, "no secondary issued");
    let snap = metrics.snapshot();
    assert_eq!(snap.hedged_triggers, 0);

    std::env::remove_var("AGENTDB_HEDGE_ENABLED");
}

#[tokio::test]
#[serial_test::serial]
async fn warm_tier_is_not_hedged() {
    // `Tier::Warm` in `hedge_delay_for` returns None — `read_segment_hedged`
    // falls through to a plain read. Even a long delay on the inner store
    // is paid in full.
    std::env::remove_var("AGENTDB_HEDGE_ENABLED");
    let (_d, delaying, path) = setup_with(Duration::from_millis(150), 1).await;
    let metrics = BoxMetrics::new();
    let store: Arc<dyn ObjectStore> = delaying.clone();

    let started = Instant::now();
    read_segment_hedged(&store, &path, agentdb_core::types::Tier::Warm, &metrics)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(140),
        "Warm tier should not hedge; read took only {elapsed:?}"
    );
    assert_eq!(metrics.snapshot().hedged_triggers, 0);
}
