use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agentdb_api::{now_ms, router, AppState, StreamBus, StreamEvent, TickBox};
use agentdb_core::{
    BoxRegistry, GcRunReporter, HlcClock, LeaderElection, LeaderElectionConfig, LeaseRenewalJob,
    LifecycleConfig, LifecycleEngine, ReaperConfig, SegmentGc, SegmentGcConfig, SessionRegistry,
    StaleTabletReaper, TenantRegistry,
};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore, SlateKvStore};
use agentdb_wire::{serve as serve_mongo, WireState};
use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
mod tracing_init;

/// Build the object store chosen by `AGENTDB_OBJECT_STORE` (`local` default,
/// `s3` opt-in).
///
/// When `s3`, reads `AGENTDB_S3_BUCKET` (required), `AGENTDB_S3_REGION`
/// (default `us-east-1`), and `AGENTDB_S3_ENDPOINT` (optional, used for
/// MinIO/other S3-compatible hosts — turns on HTTP + path-style addressing).
/// Standard AWS creds via the env chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).
async fn build_object_store(objects_dir: &std::path::Path) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let kind = std::env::var("AGENTDB_OBJECT_STORE").unwrap_or_else(|_| "local".into());
    let store: Arc<dyn ObjectStore> = match kind.as_str() {
        "local" => {
            std::fs::create_dir_all(objects_dir).context("create objects dir")?;
            Arc::new(
                LocalFileSystem::new_with_prefix(objects_dir).context("open local object store")?,
            )
        }
        "s3" => {
            let bucket = std::env::var("AGENTDB_S3_BUCKET")
                .context("AGENTDB_S3_BUCKET is required when AGENTDB_OBJECT_STORE=s3")?;
            let region = std::env::var("AGENTDB_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
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
        other => anyhow::bail!("unknown AGENTDB_OBJECT_STORE={other}; expected local|s3"),
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

fn tier_name(t: agentdb_core::Tier) -> &'static str {
    match t {
        agentdb_core::Tier::Warm => "warm",
        agentdb_core::Tier::Cold => "cold",
        agentdb_core::Tier::Archive => "archive",
    }
}

/// Wall-clock ms since UNIX epoch, as `i64` — the shape the control
/// plane's heartbeat/lease columns expect. Saturates to 0 if the
/// wall clock is somehow before the epoch.
fn now_ms_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Keep the guard alive for the duration of main(); drop flushes
    // pending OTel spans. See `tracing_init.rs` for the env-var surface.
    let _tracer_guard = tracing_init::init()?;

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
    let admission_capacity: u32 = match std::env::var("AGENTDB_ADMISSION_CAPACITY") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_ADMISSION_CAPACITY must be a valid u32")?,
        Err(_) => agentdb_core::DEFAULT_ADMISSION_CAPACITY,
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
        t_archive: Duration::from_secs(
            std::env::var("AGENTDB_T_ARCHIVE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(600),
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
        Ok(other) => anyhow::bail!("unknown AGENTDB_KV_BACKEND={other}; expected rocksdb|slatedb"),
    };
    // Control-plane meta store (M18). Defaults to a local SQLite file
    // so `cargo run` works with no setup. Postgres is not yet exposed
    // via a constructor — follow-up PR.
    let meta_path =
        std::env::var("AGENTDB_META_PATH").unwrap_or_else(|_| "./data/meta.sqlite".to_string());
    if let Some(parent) = std::path::Path::new(&meta_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).context("create meta dir")?;
        }
    }
    let meta = Arc::new(
        MetaStore::open_sqlite(&meta_path)
            .await
            .with_context(|| format!("open meta sqlite at {meta_path}"))?,
    );

    // Node id identifies this process to the control plane. Appears as
    // `owner_node_id` on every `tablets` row this process owns.
    let node_id =
        std::env::var("AGENTDB_NODE_ID").unwrap_or_else(|_| format!("node-{}", std::process::id()));

    let hlc = Arc::new(HlcClock::default());
    tracing::info!(
        %node_id,
        %meta_path,
        num_boxes,
        admission_capacity,
        "acquiring leases for boxes via control plane"
    );
    let registry = Arc::new(
        BoxRegistry::new_with_meta_and_capacity(
            kv.clone(),
            object_store.clone(),
            num_boxes,
            hlc,
            meta.clone(),
            node_id.clone(),
            admission_capacity,
        )
        .await
        .context("acquire leases via control plane")?,
    );
    // Spawn a per-box renewal loop now that the initial lease is held.
    // Handles are intentionally forgotten — shutdown is process exit.
    let mut renewal_jobs: Vec<LeaseRenewalJob> = Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let b = registry.box_by_id(i).expect("box id in range");
        let lease = b.lease().expect("new_with_meta attaches a lease").clone();
        renewal_jobs.push(LeaseRenewalJob::start(lease));
    }
    std::mem::forget(renewal_jobs);

    // Register this process as a node in the control plane and start
    // its heartbeat loop. Advertised address is `AGENTDB_NODE_ADDRESS`
    // if set, otherwise the REST http address (sufficient for local
    // dev; production will set it explicitly to a peer-reachable host).
    let node_address =
        std::env::var("AGENTDB_NODE_ADDRESS").unwrap_or_else(|_| http_addr.to_string());
    let heartbeat_secs: u64 = std::env::var("AGENTDB_HEARTBEAT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    agentdb_meta::nodes::upsert(meta.pool(), &node_id, &node_address, "alive", now_ms_i64())
        .await
        .context("register node in control plane")?;
    tracing::info!(%node_id, %node_address, heartbeat_secs, "registered node in control plane");
    {
        let meta = meta.clone();
        let node_id = node_id.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(heartbeat_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // First tick fires immediately; skip to align with "heartbeat
            // every N seconds after the initial register".
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(err) =
                    agentdb_meta::nodes::heartbeat(meta.pool(), &node_id, now_ms_i64()).await
                {
                    tracing::warn!(
                        %node_id,
                        error = %err,
                        "node heartbeat failed; will retry on next tick"
                    );
                }
            }
        });
    }

    // M22: spawn the scheduler leader-election loop. This process
    // contests with every other node sharing this meta store; one of
    // them wins and keeps a lease fresh.
    let election_cfg = LeaderElectionConfig::from_env();
    let election = LeaderElection::start(meta.clone(), node_id.clone(), election_cfg);
    tracing::info!(
        %node_id,
        lease_secs = election_cfg.lease_duration.as_secs(),
        renew_secs = election_cfg.renew_interval.as_secs(),
        attempt_secs = election_cfg.attempt_interval.as_secs(),
        "scheduler leader-election loop started"
    );

    // M22: spawn the stale-lease reaper. Runs on every node but is
    // a no-op on followers — only the elected leader actually
    // reaps expired tablet ownership. Follow-up PRs add richer
    // reassignment policy; this loop just clears dead-owner rows
    // so admin views stay honest.
    let reaper_cfg = ReaperConfig::from_env();
    let reaper = StaleTabletReaper::start(meta.clone(), election.clone(), reaper_cfg);
    tracing::info!(
        tick_secs = reaper_cfg.tick.as_secs(),
        safety_margin_secs = reaper_cfg.safety_margin.as_secs(),
        "stale-lease tablet reaper started"
    );

    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv.clone()));

    let stream = StreamBus::default();

    let http_client = agentdb_api::webhook::new_client();

    let hook: agentdb_core::lifecycle::TierChangeHook = {
        let http = http_client.clone();
        let stream_for_hook = stream.clone();
        let tenants_for_hook = tenants.clone();
        std::sync::Arc::new(move |box_, session, from, to| {
            let tenant_external = tenants_for_hook
                .lookup_by_internal(box_.id, session.tenant_internal_id)
                .ok()
                .flatten()
                .map(|t| t.external_id)
                .unwrap_or_else(|| format!("tenant-{}", session.tenant_internal_id));

            stream_for_hook.emit(StreamEvent::TierChange {
                ts_ms: now_ms(),
                box_id: box_.id,
                session_id: session.external_id.clone(),
                from: tier_name(from).into(),
                to: tier_name(to).into(),
            });

            agentdb_api::webhook::fire_tier_change(
                &http,
                box_,
                &tenant_external,
                session,
                from,
                to,
            );
        })
    };

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
            Some(hook.clone()),
        ));
    }
    std::mem::forget(engines);

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

    // M16: spawn one periodic IndexSnapshotJob per box.
    let snapshot_cfg = agentdb_core::IndexSnapshotJobConfig::from_env();
    let mut snapshot_jobs: Vec<agentdb_core::IndexSnapshotJob> =
        Vec::with_capacity(num_boxes as usize);
    for i in 0..num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        snapshot_jobs.push(agentdb_core::IndexSnapshotJob::start(
            object_store.clone(),
            box_,
            snapshot_cfg,
        ));
    }
    std::mem::forget(snapshot_jobs);

    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
        stream: stream.clone(),
        http: http_client.clone(),
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

    // Layer the MetaStore as an axum Extension so admin handlers
    // (e.g. `GET /api/admin/nodes`) can reach it without it living
    // on `AppState`. Handlers use `Option<Extension<_>>` so tests
    // that build the router without a meta still work.
    let app = router(http_state)
        .layer(axum::Extension(meta.clone()))
        .layer(axum::Extension(election.clone()))
        .layer(axum::Extension(reaper.clone()));
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
