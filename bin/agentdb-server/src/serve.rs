//! `agentdb serve` and `agentdb serve --dev` — the server-startup
//! path. The body is what `main.rs` used to be before M26's CLI
//! refactor, wrapped in a `run()` function that accepts the parsed
//! `ServeOpts` (from clap) and applies flag/env/dev-mode defaults
//! before booting every background loop.

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

use crate::cli::ServeOpts;

/// Resolve a config value by walking `flag → env → default`. The
/// `env` lookup runs only when the flag didn't provide a value.
fn resolve_string(flag: Option<&str>, env_key: &str, default: impl FnOnce() -> String) -> String {
    if let Some(v) = flag {
        return v.to_string();
    }
    std::env::var(env_key).unwrap_or_else(|_| default())
}

/// Build the object store chosen by `AGENTDB_OBJECT_STORE` (`local` default,
/// `s3` opt-in).
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

    use futures_util::StreamExt;
    let probe = object_store::path::Path::from("agentdb/");
    let mut stream = store.list(Some(&probe));
    match stream.next().await {
        None => {}
        Some(Ok(_)) => {}
        Some(Err(e)) => {
            anyhow::bail!(
                "object store liveness probe failed under prefix 'agentdb/': {e}. \
                 Check AGENTDB_S3_BUCKET / AGENTDB_S3_REGION / AGENTDB_S3_ENDPOINT and creds."
            );
        }
    }
    drop(stream);
    Ok(store)
}

fn tier_name(t: agentdb_core::Tier) -> &'static str {
    match t {
        agentdb_core::Tier::Warm => "warm",
        agentdb_core::Tier::Cold => "cold",
        agentdb_core::Tier::Archive => "archive",
    }
}

/// Wall-clock ms since UNIX epoch, as `i64`.
fn now_ms_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Resolved configuration for the server process. Each field is the
/// end result of `default → env → flag → dev-mode override`.
struct Resolved {
    data_dir: PathBuf,
    objects_dir: PathBuf,
    meta_path: String,
    num_boxes: u16,
    admission_capacity: u32,
    http_addr: SocketAddr,
    mongo_addr: SocketAddr,
    node_id: String,
    node_address: String,
    heartbeat_secs: u64,
    lifecycle_cfg: LifecycleConfig,
    segment_gc_cfg: SegmentGcConfig,
    /// Holds the TempDir for dev mode so it lives for the life of the
    /// process (drop wipes the tempdir). `None` in non-dev mode.
    _dev_tempdir: Option<tempfile::TempDir>,
}

fn resolve_config(opts: &ServeOpts) -> anyhow::Result<Resolved> {
    // Dev mode pre-step: if --dev is set, set up ephemeral tempdirs
    // and redirect the storage envs at them. The flag overrides
    // still apply after dev mode — `--dev --data-dir /persistent`
    // lets operators mix the two.
    let dev_tempdir = if opts.dev {
        let td = tempfile::tempdir().context("create dev-mode tempdir")?;
        tracing::info!(path = %td.path().display(), "dev mode: using ephemeral tempdir");
        Some(td)
    } else {
        None
    };

    let dev_data = dev_tempdir.as_ref().map(|t| t.path().join("rocksdb"));
    let dev_objects = dev_tempdir.as_ref().map(|t| t.path().join("objects"));
    let dev_meta = dev_tempdir
        .as_ref()
        .map(|t| t.path().join("meta.sqlite").to_string_lossy().to_string());

    let data_dir: PathBuf = opts
        .data_dir
        .clone()
        .or_else(|| std::env::var_os("AGENTDB_DATA").map(PathBuf::from))
        .or(dev_data)
        .unwrap_or_else(|| PathBuf::from("./data/rocksdb"));
    let objects_dir: PathBuf = opts
        .objects_dir
        .clone()
        .or_else(|| std::env::var_os("AGENTDB_OBJECTS").map(PathBuf::from))
        .or(dev_objects)
        .unwrap_or_else(|| PathBuf::from("./data/objects"));
    let meta_path = opts.meta_path.clone().unwrap_or_else(|| {
        std::env::var("AGENTDB_META_PATH")
            .ok()
            .or(dev_meta)
            .unwrap_or_else(|| "./data/meta.sqlite".to_string())
    });
    let num_boxes: u16 = match opts.boxes {
        Some(n) => n,
        None => match std::env::var("AGENTDB_BOXES") {
            Ok(v) => v.parse().context("AGENTDB_BOXES must be a valid u16")?,
            Err(_) => 4,
        },
    };
    let admission_capacity: u32 = match std::env::var("AGENTDB_ADMISSION_CAPACITY") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_ADMISSION_CAPACITY must be a valid u32")?,
        Err(_) => agentdb_core::DEFAULT_ADMISSION_CAPACITY,
    };
    let http_addr: SocketAddr = resolve_string(opts.addr.as_deref(), "AGENTDB_ADDR", || {
        "127.0.0.1:7878".into()
    })
    .parse()
    .context("AGENTDB_ADDR / --addr must be a valid socket address")?;
    let mongo_addr: SocketAddr =
        resolve_string(opts.mongo_addr.as_deref(), "AGENTDB_MONGO_ADDR", || {
            "127.0.0.1:7879".into()
        })
        .parse()
        .context("AGENTDB_MONGO_ADDR / --mongo-addr must be a valid socket address")?;
    let node_id = resolve_string(opts.node_id.as_deref(), "AGENTDB_NODE_ID", || {
        format!("node-{}", std::process::id())
    });
    let node_address =
        std::env::var("AGENTDB_NODE_ADDRESS").unwrap_or_else(|_| http_addr.to_string());
    let heartbeat_secs: u64 = std::env::var("AGENTDB_HEARTBEAT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
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

    Ok(Resolved {
        data_dir,
        objects_dir,
        meta_path,
        num_boxes,
        admission_capacity,
        http_addr,
        mongo_addr,
        node_id,
        node_address,
        heartbeat_secs,
        lifecycle_cfg,
        segment_gc_cfg,
        _dev_tempdir: dev_tempdir,
    })
}

/// Entry point for `agentdb serve`. Consumes parsed flags, resolves
/// them against env + defaults (dev mode if requested), and runs
/// until either the HTTP or Mongo listener task terminates.
pub async fn run(opts: ServeOpts) -> anyhow::Result<()> {
    let cfg = resolve_config(&opts)?;
    if opts.dev {
        tracing::info!("running in dev mode (ephemeral state — nothing persists across restart)");
    }

    let object_store: Arc<dyn ObjectStore> = build_object_store(&cfg.objects_dir).await?;
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
            std::fs::create_dir_all(&cfg.data_dir).context("create data dir")?;
            Arc::new(RocksKvStore::open(&cfg.data_dir).context("open rocksdb")?)
        }
        Ok(other) => anyhow::bail!("unknown AGENTDB_KV_BACKEND={other}; expected rocksdb|slatedb"),
    };

    // Control-plane meta store (M18). Defaults to a local SQLite file
    // so `agentdb serve` with no config works. Postgres is not yet
    // exposed via a constructor — follow-up PR.
    if let Some(parent) = std::path::Path::new(&cfg.meta_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).context("create meta dir")?;
        }
    }
    let meta = Arc::new(
        MetaStore::open_sqlite(&cfg.meta_path)
            .await
            .with_context(|| format!("open meta sqlite at {}", cfg.meta_path))?,
    );

    let hlc = Arc::new(HlcClock::default());
    tracing::info!(
        node_id = %cfg.node_id,
        meta_path = %cfg.meta_path,
        num_boxes = cfg.num_boxes,
        admission_capacity = cfg.admission_capacity,
        "acquiring leases for boxes via control plane"
    );
    let registry = Arc::new(
        BoxRegistry::new_with_meta_and_capacity(
            kv.clone(),
            object_store.clone(),
            cfg.num_boxes,
            hlc,
            meta.clone(),
            cfg.node_id.clone(),
            cfg.admission_capacity,
        )
        .await
        .context("acquire leases via control plane")?,
    );

    // Drain-aware renewal loops.
    let mut renewal_jobs: Vec<LeaseRenewalJob> = Vec::with_capacity(cfg.num_boxes as usize);
    let drain_flag = registry.draining_flag();
    for i in 0..cfg.num_boxes {
        let b = registry.box_by_id(i).expect("box id in range");
        let lease = b.lease().expect("new_with_meta attaches a lease").clone();
        renewal_jobs.push(LeaseRenewalJob::start_with_drain(lease, drain_flag.clone()));
    }
    std::mem::forget(renewal_jobs);

    // Node membership + heartbeat.
    agentdb_meta::nodes::upsert(
        meta.pool(),
        &cfg.node_id,
        &cfg.node_address,
        "alive",
        now_ms_i64(),
    )
    .await
    .context("register node in control plane")?;
    tracing::info!(
        node_id = %cfg.node_id,
        node_address = %cfg.node_address,
        heartbeat_secs = cfg.heartbeat_secs,
        "registered node in control plane"
    );
    {
        let meta = meta.clone();
        let node_id = cfg.node_id.clone();
        let heartbeat_secs = cfg.heartbeat_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(heartbeat_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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

    // Scheduler leader election.
    let election_cfg = LeaderElectionConfig::from_env();
    let election = LeaderElection::start(meta.clone(), cfg.node_id.clone(), election_cfg);
    tracing::info!(
        node_id = %cfg.node_id,
        lease_secs = election_cfg.lease_duration.as_secs(),
        renew_secs = election_cfg.renew_interval.as_secs(),
        attempt_secs = election_cfg.attempt_interval.as_secs(),
        "scheduler leader-election loop started"
    );

    // M20: seed the shards table (idempotent) and build a
    // LocationProxy that caches shard→tablet assignments. As of
    // the M20 write-path PR the registry consults the proxy on the
    // hot path — an operator who sets a shard override via
    // `POST /api/admin/shards/:id` actually routes traffic.
    agentdb_meta::shards::seed_shards(
        meta.pool(),
        agentdb_meta::shards::DEFAULT_SHARD_COUNT,
        now_ms_i64(),
    )
    .await
    .context("seed shards table")?;
    let location_proxy = agentdb_core::LocationProxy::new(meta.clone())
        .await
        .context("initialize LocationProxy")?;
    let refresh_secs: u64 = std::env::var("AGENTDB_SHARD_REFRESH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let _shard_refresh_job = agentdb_core::LocationProxyRefreshJob::start(
        location_proxy.clone(),
        Duration::from_secs(refresh_secs),
    );
    std::mem::forget(_shard_refresh_job);
    // Wire the proxy into the registry so `route_session` starts
    // consulting it. Must happen before the HTTP server starts
    // accepting writes, otherwise early requests would silently use
    // tenant-hash routing even when overrides are set.
    registry
        .attach_location(location_proxy.clone())
        .map_err(|_| anyhow::anyhow!("location proxy already attached"))?;
    tracing::info!(
        refresh_secs,
        shard_count = agentdb_core::SHARD_COUNT,
        "location proxy attached to registry; shard refresh loop started"
    );

    // Stale-lease reaper. Started after the location proxy so it
    // can refresh the local cache immediately after reassigning
    // shards away from a reaped tablet (M22).
    let reaper_cfg = ReaperConfig::from_env();
    let reaper = StaleTabletReaper::start(
        meta.clone(),
        election.clone(),
        reaper_cfg,
        Some(location_proxy.clone()),
    );
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

    let mut engines: Vec<LifecycleEngine> = Vec::with_capacity(cfg.num_boxes as usize);
    for i in 0..cfg.num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        engines.push(LifecycleEngine::start(
            kv.clone(),
            object_store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_,
            cfg.lifecycle_cfg,
            Some(hook.clone()),
        ));
    }
    std::mem::forget(engines);

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

    let mut gcs: Vec<SegmentGc> = Vec::with_capacity(cfg.num_boxes as usize);
    for i in 0..cfg.num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        gcs.push(SegmentGc::start(
            object_store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_,
            cfg.segment_gc_cfg,
            Some(reporter.clone()),
        ));
    }
    std::mem::forget(gcs);

    let snapshot_cfg = agentdb_core::IndexSnapshotJobConfig::from_env();
    let mut snapshot_jobs: Vec<agentdb_core::IndexSnapshotJob> =
        Vec::with_capacity(cfg.num_boxes as usize);
    for i in 0..cfg.num_boxes {
        let box_ = registry.box_by_id(i).expect("box id in range");
        snapshot_jobs.push(agentdb_core::IndexSnapshotJob::start(
            object_store.clone(),
            box_,
            snapshot_cfg,
        ));
    }
    std::mem::forget(snapshot_jobs);

    // M19 auth toggle. Off by default. When on, the wire
    // server advertises SCRAM-SHA-256 in hello and requires
    // every non-SASL command to come from an authenticated
    // connection; and the REST `/api/tenants/*` subtree
    // requires HTTP Basic auth whose user maps to the
    // targeted tenant.
    let require_auth = std::env::var("AGENTDB_REQUIRE_AUTH")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let http_state = AppState {
        registry: registry.clone(),
        tenants: tenants.clone(),
        sessions: sessions.clone(),
        stream: stream.clone(),
        http: http_client.clone(),
        auth: if require_auth {
            Some(agentdb_api::AuthConfig {
                meta: Some(meta.clone()),
            })
        } else {
            None
        },
    };
    let wire_state = if require_auth {
        WireState::with_auth(
            registry.clone(),
            tenants,
            sessions,
            agentdb_wire::WireAuthConfig {
                meta: meta.clone(),
                require_auth: true,
            },
        )
    } else {
        WireState::new(registry.clone(), tenants, sessions)
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

    let app = router(http_state)
        .layer(axum::Extension(meta.clone()))
        .layer(axum::Extension(election.clone()))
        .layer(axum::Extension(reaper.clone()))
        .layer(axum::Extension(location_proxy.clone()));
    let http_listener = tokio::net::TcpListener::bind(cfg.http_addr)
        .await
        .context("bind http listener")?;
    let mongo_listener = tokio::net::TcpListener::bind(cfg.mongo_addr)
        .await
        .context("bind mongo listener")?;
    tracing::info!(
        http_addr = %cfg.http_addr,
        mongo_addr = %cfg.mongo_addr,
        objects = %cfg.objects_dir.display(),
        dev = opts.dev,
        "agentdb listening"
    );

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
