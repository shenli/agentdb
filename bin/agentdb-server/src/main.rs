use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agentdb_api::{now_ms, router, AppState, StreamBus, StreamEvent, TickBox};
use agentdb_core::{
    BoxRegistry, GcRunReporter, LifecycleConfig, LifecycleEngine, SegmentGc, SegmentGcConfig,
    SessionRegistry, TenantRegistry,
};
use agentdb_storage::{KvStore, RocksKvStore, SlateKvStore};
use agentdb_wire::{serve as serve_mongo, WireState};
use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tracing_subscriber::EnvFilter;

/// Build the object store chosen by `AGENTDB_OBJECT_STORE` (`local` default,
/// `s3` opt-in).
///
/// When `s3`, reads `AGENTDB_S3_BUCKET` (required), `AGENTDB_S3_REGION`
/// (default `us-east-1`), and `AGENTDB_S3_ENDPOINT` (optional, used for
/// MinIO/other S3-compatible hosts — turns on HTTP + path-style addressing).
/// Standard AWS creds via the env chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).
async fn build_object_store(
    objects_dir: &std::path::Path,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let kind = std::env::var("AGENTDB_OBJECT_STORE").unwrap_or_else(|_| "local".into());
    let store: Arc<dyn ObjectStore> = match kind.as_str() {
        "local" => {
            std::fs::create_dir_all(objects_dir).context("create objects dir")?;
            Arc::new(
                LocalFileSystem::new_with_prefix(objects_dir)
                    .context("open local object store")?,
            )
        }
        "s3" => {
            let bucket = std::env::var("AGENTDB_S3_BUCKET")
                .context("AGENTDB_S3_BUCKET is required when AGENTDB_OBJECT_STORE=s3")?;
            let region = std::env::var("AGENTDB_S3_REGION")
                .unwrap_or_else(|_| "us-east-1".into());
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(&bucket)
                .with_region(&region);
            if let Ok(endpoint) = std::env::var("AGENTDB_S3_ENDPOINT") {
                // MinIO and most S3-compatible deployments need path-style
                // addressing. `allow_http` permits `http://localhost:9000`.
                builder = builder
                    .with_endpoint(&endpoint)
                    .with_allow_http(true)
                    .with_virtual_hosted_style_request(false);
            }
            Arc::new(builder.build().context("build AmazonS3")?)
        }
        other => anyhow::bail!(
            "unknown AGENTDB_OBJECT_STORE={other}; expected local|s3"
        ),
    };

    // Liveness probe: single list call under `agentdb/`. A local FS tempdir
    // trivially succeeds; S3 catches bad creds, wrong region, unreachable
    // endpoint, or a typo'd bucket name here — well before the server starts
    // serving requests and streaming errors.
    use futures_util::StreamExt;
    let probe = object_store::path::Path::from("agentdb/");
    let mut stream = store.list(Some(&probe));
    match stream.next().await {
        None => { /* empty prefix — legitimate, probe succeeded */ }
        Some(Ok(_)) => { /* at least one entry; access works */ }
        Some(Err(e)) => {
            anyhow::bail!(
                "object store liveness probe failed under prefix 'agentdb/': {e}. \
                 Check AGENTDB_S3_BUCKET / AGENTDB_S3_REGION / AGENTDB_S3_ENDPOINT and creds."
            );
        }
    }
    drop(stream); // release the stream before returning the Arc

    Ok(store)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let data_dir: PathBuf = std::env::var_os("AGENTDB_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/rocksdb"));
    let objects_dir: PathBuf = std::env::var_os("AGENTDB_OBJECTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/objects"));
    let num_boxes: u16 = match std::env::var("AGENTDB_BOXES") {
        Ok(v) => v.parse().context("AGENTDB_BOXES must be a valid u16")?,
        Err(_) => 4,
    };
    let http_addr: SocketAddr = match std::env::var("AGENTDB_ADDR") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7878".parse().unwrap(),
    };
    let mongo_addr: SocketAddr = match std::env::var("AGENTDB_MONGO_ADDR") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_MONGO_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7879".parse().unwrap(),
    };
    let lifecycle_cfg = LifecycleConfig {
        tick: Duration::from_secs(
            std::env::var("AGENTDB_TICK_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        ),
        t_cold: Duration::from_secs(
            std::env::var("AGENTDB_T_COLD_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
        ),
    };
    let segment_gc_cfg = SegmentGcConfig {
        tick: Duration::from_secs(
            std::env::var("AGENTDB_SEGMENT_GC_TICK_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
        ),
        grace: Duration::from_secs(
            std::env::var("AGENTDB_SEGMENT_GC_GRACE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
        ),
    };

    let object_store: Arc<dyn ObjectStore> = build_object_store(&objects_dir).await?;
    let kv: Arc<dyn KvStore> = match std::env::var("AGENTDB_KV_BACKEND").as_deref() {
        Ok("slatedb") => {
            if std::env::var("AGENTDB_OBJECT_STORE").is_err() {
                tracing::warn!(
                    "AGENTDB_KV_BACKEND=slatedb but AGENTDB_OBJECT_STORE is unset; \
                     defaulting to local filesystem. Set AGENTDB_OBJECT_STORE=s3 \
                     for the production shape."
                );
            }
            let slate_path = std::env::var("AGENTDB_SLATEDB_PATH")
                .unwrap_or_else(|_| "agentdb/slate".to_string());
            Arc::new(
                SlateKvStore::open(object_store.clone(), &slate_path)
                    .await
                    .context("open slatedb")?,
            )
        }
        Ok("rocksdb") | Err(_) => {
            std::fs::create_dir_all(&data_dir).context("create data dir")?;
            Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?)
        }
        Ok(other) => anyhow::bail!(
            "unknown AGENTDB_KV_BACKEND={other}; expected rocksdb|slatedb"
        ),
    };
    let registry = Arc::new(BoxRegistry::new(kv.clone(), object_store.clone(), num_boxes)?);
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));

    // Spin up one lifecycle engine per box. Engine handles are intentionally
    // leaked for the life of the process — shutdown is process exit.
    let mut engines: Vec<LifecycleEngine> = Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        engines.push(LifecycleEngine::start(
            kv.clone(),
            object_store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_,
            lifecycle_cfg,
        ));
    }
    std::mem::forget(engines);

    let stream = StreamBus::default();

    // M9: closure that emits SegmentGcRun on SSE when GC reclaims segments.
    let reporter: GcRunReporter = {
        let stream = stream.clone();
        std::sync::Arc::new(move |box_id, reclaimed, skipped_pinned| {
            stream.emit(StreamEvent::SegmentGcRun {
                ts_ms: now_ms(),
                box_id,
                reclaimed,
                skipped_pinned,
            });
        })
    };

    // M9: spawn one SegmentGc per box.
    let mut gcs: Vec<SegmentGc> = Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        gcs.push(SegmentGc::start(
            object_store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_,
            segment_gc_cfg,
            Some(reporter.clone()),
        ));
    }
    std::mem::forget(gcs);

    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
        stream: stream.clone(),
    };
    let wire_state = WireState {
        registry: registry.clone(),
        tenants,
        sessions,
    };

    // Periodic MetricsTick broadcaster for the dashboard SSE stream.
    {
        let registry = registry.clone();
        let stream = stream.clone();
        let period = Duration::from_millis(
            std::env::var("AGENTDB_TICK_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
        );
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            loop {
                ticker.tick().await;
                let mut boxes = Vec::with_capacity(registry.num_boxes() as usize);
                for i in 0..registry.num_boxes() {
                    let Some(b) = registry.box_by_id(i) else {
                        continue;
                    };
                    let snap = b.metrics.snapshot();
                    let fault_active = b
                        .fault
                        .read()
                        .as_ref()
                        .map(|f| f.is_active())
                        .unwrap_or(false);
                    boxes.push(TickBox {
                        id: i,
                        warm_sessions: snap.gauges.warm_sessions,
                        cold_sessions: snap.gauges.cold_sessions,
                        appends: snap.appends,
                        forks: snap.forks,
                        fault_active,
                        open_snapshots: snap.open_snapshots,
                        segments_pinned_by_snapshots: snap.segments_pinned_by_snapshots,
                    });
                }
                stream.emit(StreamEvent::MetricsTick {
                    ts_ms: now_ms(),
                    boxes,
                });
            }
        });
    }

    let app = router(http_state);
    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .context("bind http listener")?;
    let mongo_listener = tokio::net::TcpListener::bind(mongo_addr)
        .await
        .context("bind mongo listener")?;
    tracing::info!(%http_addr, %mongo_addr, objects = %objects_dir.display(), "agentdb-server listening");

    let http_task =
        tokio::spawn(async move { axum::serve(http_listener, app).await.context("http serve") });
    let mongo_task = tokio::spawn(async move {
        serve_mongo(mongo_listener, wire_state)
            .await
            .map_err(|e| anyhow::anyhow!("mongo serve: {e}"))
    });

    tokio::select! {
        r = http_task => r.context("http task join")??,
        r = mongo_task => r.context("mongo task join")??,
    };
    Ok(())
}
