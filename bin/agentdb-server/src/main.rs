use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agentdb_api::{now_ms, router, AppState, StreamBus, StreamEvent, TickBox};
use agentdb_core::{
    BoxRegistry, GcRunReporter, LifecycleConfig, LifecycleEngine, SegmentGc, SegmentGcConfig,
    SessionRegistry, TenantRegistry,
};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve as serve_mongo, WireState};
use anyhow::Context;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tracing_subscriber::EnvFilter;

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

    std::fs::create_dir_all(&data_dir).context("create data dir")?;
    std::fs::create_dir_all(&objects_dir).context("create objects dir")?;

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?);
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&objects_dir).context("open object store")?);
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), num_boxes)?);
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));

    // Spin up one lifecycle engine per box. Engine handles are intentionally
    // leaked for the life of the process — shutdown is process exit.
    let mut engines: Vec<LifecycleEngine> = Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        engines.push(LifecycleEngine::start(
            kv.clone(),
            store.clone(),
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
            store.clone(),
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
