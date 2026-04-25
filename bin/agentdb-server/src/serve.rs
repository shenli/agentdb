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
    LifecycleConfig, LifecycleEngine, LockGc, LockGcConfig, NodeReaper, NodeReaperConfig,
    PlacementConfig, ReaperConfig, ReconcilerConfig, SegmentGc, SegmentGcConfig, SessionRegistry,
    StaleTabletReaper, TabletPlacement, TabletReconciler, TenantRegistry,
};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore, SlateKvStore};
use agentdb_wire::{serve as serve_mongo, serve_tls as serve_mongo_tls, WireState};
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

/// Resolve REST TLS config from CLI flags + env. Returns
/// `Some` when TLS should be active, `None` for plaintext.
///
/// Precedence: explicit `--tls-cert` + `--tls-key` wins;
/// otherwise `AGENTDB_TLS_CERT` + `AGENTDB_TLS_KEY` env
/// vars; otherwise `--dev-tls` synthesizes an ephemeral
/// self-signed cert; otherwise plaintext.
async fn resolve_tls(
    opts: &ServeOpts,
) -> anyhow::Result<Option<axum_server::tls_rustls::RustlsConfig>> {
    // Flag pair.
    if let (Some(cert), Some(key)) = (opts.tls_cert.as_ref(), opts.tls_key.as_ref()) {
        return Ok(Some(crate::tls::load_tls_config(cert, key).await?));
    }
    // Env-var pair.
    let env_cert = std::env::var("AGENTDB_TLS_CERT").ok().map(std::path::PathBuf::from);
    let env_key = std::env::var("AGENTDB_TLS_KEY").ok().map(std::path::PathBuf::from);
    if let (Some(cert), Some(key)) = (&env_cert, &env_key) {
        return Ok(Some(crate::tls::load_tls_config(cert, key).await?));
    }
    if env_cert.is_some() || env_key.is_some() {
        anyhow::bail!(
            "AGENTDB_TLS_CERT and AGENTDB_TLS_KEY must be set together (or neither)"
        );
    }
    // Dev ephemeral.
    if opts.dev_tls || opts.dev {
        // `--dev` implies dev-tls for the zero-config
        // quickstart? No — keep `--dev` as HTTP unless
        // operator opts in explicitly with --dev-tls.
        // Re-enable if feedback asks for it.
        if opts.dev_tls {
            return Ok(Some(crate::tls::generate_dev_tls().await?));
        }
    }
    Ok(None)
}

/// Pull every row from `tenant_quotas` and hydrate the
/// in-memory registry. Used both at startup and on reload.
async fn hydrate_tenant_quotas(
    registry: &Arc<agentdb_core::TenantQuotaRegistry>,
    meta: &Arc<agentdb_meta::MetaStore>,
) -> anyhow::Result<agentdb_core::TenantQuotaReloadDiff> {
    let persisted = agentdb_meta::tenant_quotas::list_all(meta.pool())
        .await
        .context("load tenant_quotas from meta")?;
    let rows: Vec<(String, agentdb_core::TenantQuotaConfig)> = persisted
        .into_iter()
        .map(|q| {
            (
                q.tenant_id,
                agentdb_core::TenantQuotaConfig {
                    max_qps: q.max_qps,
                    burst: q.burst,
                },
            )
        })
        .collect();
    let diff = registry.hydrate(&rows);
    tracing::info!(
        hydrated = rows.len(),
        added = diff.added.len(),
        removed = diff.removed.len(),
        "global per-tenant rate limits hydrated from meta",
    );
    Ok(diff)
}

/// Run one hot-reload cycle. Called from both the admin
/// endpoint and the SIGHUP handler. Each target is
/// best-effort: failure of one subsystem (e.g. a bad
/// `RUST_LOG` string) doesn't abort the others, and the
/// running values for those that did apply stay in effect.
async fn do_reload(
    meta: &Arc<agentdb_meta::MetaStore>,
    quota_registry: &Arc<agentdb_core::TenantQuotaRegistry>,
    registry: &Arc<agentdb_core::BoxRegistry>,
    tracing_reload: &crate::tracing_init::TracingReloadHandle,
) -> Result<agentdb_api::admin_reload::ReloadSummary, String> {
    use agentdb_api::admin_reload::ReloadSummary;

    // 1. Tenant quotas — re-read from meta. Errors propagate
    //    because an unavailable meta store is a hard failure;
    //    operators need to see the 500.
    let diff = hydrate_tenant_quotas(quota_registry, meta)
        .await
        .map_err(|e| format!("hydrate tenant quotas: {e:#}"))?;

    // 2. Slow-query buffer — bump threshold + capacity from
    //    env if present. If the buffer isn't attached (test
    //    registry / AGENTDB_SLOW_QUERY_DISABLE), skip; no
    //    operator change is possible there anyway.
    let slow = registry.slow_queries();
    if let Some(buf) = &slow {
        if let Some(ms) = std::env::var("AGENTDB_SLOW_QUERY_THRESHOLD_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            buf.set_threshold_ms(ms);
        }
        if let Some(cap) = std::env::var("AGENTDB_SLOW_QUERY_BUFFER_CAPACITY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            buf.set_capacity(cap);
        }
    }

    // 3. Tracing filter — only apply if `RUST_LOG` is set.
    //    Malformed filters don't disrupt the existing one;
    //    we surface the error in the response so operators
    //    know their change didn't land.
    let mut tracing_filter_applied = String::new();
    if let Ok(filter) = std::env::var("RUST_LOG") {
        match tracing_reload.reload(&filter) {
            Ok(()) => tracing_filter_applied = filter,
            Err(e) => tracing::warn!(
                error = %e,
                "reload: tracing filter rejected; previous filter kept"
            ),
        }
    }

    let slow_threshold = slow.as_ref().map(|b| b.threshold_ms()).unwrap_or(0);
    let slow_capacity = slow.as_ref().map(|b| b.capacity() as u64).unwrap_or(0);

    Ok(ReloadSummary {
        slow_query_threshold_ms: slow_threshold,
        slow_query_buffer_capacity: slow_capacity,
        tracing_filter: tracing_filter_applied,
        tenant_quotas_added: diff.added,
        tenant_quotas_removed: diff.removed,
        reloaded_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
    })
}

/// Resolve TLS for the Mongo wire listener. Flag pair
/// (`--mongo-tls-cert` + `--mongo-tls-key`) wins; otherwise
/// env pair (`AGENTDB_MONGO_TLS_CERT` + `AGENTDB_MONGO_TLS_KEY`);
/// otherwise when `AGENTDB_MONGO_TLS_REUSE_REST=1` is set and
/// REST TLS is configured, reuse the REST cert; otherwise
/// `--dev-tls` synthesizes an ephemeral cert (same cert as
/// REST); otherwise plaintext.
///
/// Returns `Some(Arc<ServerConfig>)` ready to hand to
/// `agentdb_wire::serve_tls`.
async fn resolve_mongo_tls(
    opts: &ServeOpts,
    rest_tls: Option<&axum_server::tls_rustls::RustlsConfig>,
) -> anyhow::Result<Option<std::sync::Arc<rustls::ServerConfig>>> {
    // Explicit Mongo flag pair.
    if let (Some(cert), Some(key)) = (
        opts.mongo_tls_cert.as_ref(),
        opts.mongo_tls_key.as_ref(),
    ) {
        let cfg = crate::tls::load_tls_config(cert, key).await?;
        return Ok(Some(cfg.get_inner()));
    }
    // Mongo env pair.
    let env_cert = std::env::var("AGENTDB_MONGO_TLS_CERT")
        .ok()
        .map(std::path::PathBuf::from);
    let env_key = std::env::var("AGENTDB_MONGO_TLS_KEY")
        .ok()
        .map(std::path::PathBuf::from);
    if let (Some(cert), Some(key)) = (&env_cert, &env_key) {
        let cfg = crate::tls::load_tls_config(cert, key).await?;
        return Ok(Some(cfg.get_inner()));
    }
    if env_cert.is_some() || env_key.is_some() {
        anyhow::bail!(
            "AGENTDB_MONGO_TLS_CERT and AGENTDB_MONGO_TLS_KEY must be set together (or neither)"
        );
    }
    // Opt-in: reuse the REST cert for Mongo when the
    // operator sets the env flag. Cheaper than provisioning
    // two separate certs for the same hostname.
    let reuse = std::env::var("AGENTDB_MONGO_TLS_REUSE_REST")
        .ok()
        .filter(|s| !s.is_empty() && s != "0" && s.to_lowercase() != "false");
    if let (Some(_), Some(rest)) = (reuse, rest_tls) {
        return Ok(Some(rest.get_inner()));
    }
    // Dev ephemeral applies to both ports when set.
    if opts.dev_tls {
        return Ok(Some(crate::tls::generate_dev_tls().await?.get_inner()));
    }
    Ok(None)
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
    /// Mongo wire address advertised to peers for cross-node
    /// forwarding. Defaults to `mongo_addr.to_string()` when
    /// `AGENTDB_NODE_MONGO_ADDRESS` is unset.
    node_mongo_address: String,
    /// Availability zone declared via AGENTDB_NODE_AZ. None
    /// in single-AZ deployments — placement falls back to
    /// load-only balancing.
    node_az: Option<String>,
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
    let node_mongo_address = std::env::var("AGENTDB_NODE_MONGO_ADDRESS")
        .unwrap_or_else(|_| mongo_addr.to_string());
    // Cross-AZ placement (agent-db-v2.md §2.7). NULL = single-AZ
    // deployment; placement falls back to load-only balancing.
    let node_az = std::env::var("AGENTDB_NODE_AZ").ok();
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
        node_mongo_address,
        node_az,
        heartbeat_secs,
        lifecycle_cfg,
        segment_gc_cfg,
        _dev_tempdir: dev_tempdir,
    })
}

/// Entry point for `agentdb serve`. Consumes parsed flags, resolves
/// them against env + defaults (dev mode if requested), and runs
/// until either the HTTP or Mongo listener task terminates.
pub async fn run(
    opts: ServeOpts,
    tracing_reload: crate::tracing_init::TracingReloadHandle,
) -> anyhow::Result<()> {
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

    // Control-plane meta store. Defaults to SQLite at
    // `cfg.meta_path` — matches the pre-v2 single-node
    // behavior. Operators who set `AGENTDB_META_BACKEND=postgres`
    // (+ `AGENTDB_META_URL`) get the Postgres dialect with
    // LISTEN/NOTIFY-driven change propagation.
    let meta_backend = std::env::var("AGENTDB_META_BACKEND")
        .unwrap_or_else(|_| "sqlite".into())
        .to_lowercase();
    let meta = Arc::new(match meta_backend.as_str() {
        "sqlite" => {
            if let Some(parent) = std::path::Path::new(&cfg.meta_path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).context("create meta dir")?;
                }
            }
            MetaStore::open_sqlite(&cfg.meta_path)
                .await
                .with_context(|| format!("open meta sqlite at {}", cfg.meta_path))?
        }
        "postgres" | "pg" => {
            let url = std::env::var("AGENTDB_META_URL")
                .context("AGENTDB_META_BACKEND=postgres requires AGENTDB_META_URL")?;
            MetaStore::open_postgres(&url)
                .await
                .context("open meta postgres")?
        }
        other => anyhow::bail!(
            "unknown AGENTDB_META_BACKEND={other}; expected 'sqlite' or 'postgres'"
        ),
    });

    let hlc = Arc::new(HlcClock::default());
    tracing::info!(
        node_id = %cfg.node_id,
        meta_path = %cfg.meta_path,
        num_boxes = cfg.num_boxes,
        admission_capacity = cfg.admission_capacity,
        "building empty registry; tablets will be claimed via placement+reconciler"
    );
    // Multi-node-safe boot: registry starts with NO boxes.
    // The scheduler leader's TabletPlacement loop assigns
    // tablets to alive nodes; the local TabletReconciler
    // picks up our assignments via add_box. See
    // crates/agentdb-core/src/tablet_placement.rs and
    // tablet_reconciler.rs for the full design rationale.
    //
    // Single-node deployments converge to "this process owns
    // [0..AGENTDB_BOXES)" within ~one placement+reconciler tick;
    // we run an initial synchronous pass below so /health/ready
    // and the HTTP listener start with the correct box set.
    let registry = Arc::new(
        BoxRegistry::new_with_meta_empty(
            kv.clone(),
            object_store.clone(),
            hlc,
            meta.clone(),
            cfg.node_id.clone(),
            cfg.admission_capacity,
        )
        .await
        .context("build empty registry")?,
    );

    // Renewal jobs are spawned per box once the reconciler has
    // populated the registry; see the post-reconcile block
    // below. Followers and just-joined nodes may have zero
    // boxes initially, which is fine.
    let drain_flag = registry.draining_flag();

    // Node membership + heartbeat. Advertise BOTH the REST
    // address (for node-to-node REST forwarding) and the
    // Mongo wire address (for cross-node Mongo forwarding).
    // The mongo address is optional — peer nodes that don't
    // see it fall back to the existing NotWritablePrimary
    // error.
    agentdb_meta::nodes::upsert(
        meta.pool(),
        &cfg.node_id,
        &cfg.node_address,
        Some(cfg.node_mongo_address.as_str()),
        "alive",
        now_ms_i64(),
        cfg.node_az.as_deref(),
    )
    .await
    .context("register node in control plane")?;
    tracing::info!(
        node_id = %cfg.node_id,
        node_address = %cfg.node_address,
        node_az = cfg.node_az.as_deref().unwrap_or("(unset)"),
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

    // Tablet placement + reconciler. Spawn the periodic loops
    // first, then run one synchronous pass before binding the
    // HTTP listener so single-node UX is "boot → ready in one
    // election round-trip" instead of "boot → ready after the
    // first periodic tick fires".
    let placement_cfg = PlacementConfig::from_env();
    let placement = TabletPlacement::start(meta.clone(), election.clone(), placement_cfg);
    tracing::info!(
        tick_secs = placement_cfg.tick.as_secs(),
        bootstrap_tablets = placement_cfg.bootstrap_tablets,
        "tablet placement loop started"
    );

    let reconciler_cfg = ReconcilerConfig::from_env();
    let reconciler = TabletReconciler::start(registry.clone(), reconciler_cfg);
    tracing::info!(
        tick_secs = reconciler_cfg.tick.as_secs(),
        "tablet reconciler loop started"
    );

    // Initial synchronous pass: wait for leader-election to
    // converge, then if we won, run a placement sweep so the
    // very next reconcile sees something to do. In `--dev` we
    // additionally block until at least one box has been
    // claimed locally — otherwise the first request after the
    // listener opens would hit `route_tenant` against an empty
    // box list and panic. Multi-node followers serve traffic
    // before owning local tablets (the leader is on another
    // node), so the box-claim wait only kicks in for dev mode.
    let leader_wait_deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !election.is_leader() && std::time::Instant::now() < leader_wait_deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if election.is_leader() {
        let assigns = std::sync::atomic::AtomicU64::new(0);
        let boots = std::sync::atomic::AtomicU64::new(0);
        let rebals = std::sync::atomic::AtomicU64::new(0);
        agentdb_core::tablet_placement::sweep_once(
            &meta,
            placement_cfg,
            &assigns,
            &boots,
            &rebals,
        )
        .await;
        tracing::info!(
            assigned = assigns.load(std::sync::atomic::Ordering::Acquire),
            bootstrapped = boots.load(std::sync::atomic::Ordering::Acquire) > 0,
            rebalanced = rebals.load(std::sync::atomic::Ordering::Acquire),
            "initial placement pass complete"
        );
    }
    {
        let added = std::sync::atomic::AtomicU64::new(0);
        let errs = std::sync::atomic::AtomicU64::new(0);
        if let Err(()) =
            agentdb_core::tablet_reconciler::reconcile_once(&registry, &added, &errs).await
        {
            tracing::warn!("initial reconcile pass failed; retrying via periodic loop");
        } else {
            tracing::info!(
                added = added.load(std::sync::atomic::Ordering::Acquire),
                num_boxes = registry.num_boxes(),
                "initial reconcile pass complete"
            );
        }
    }
    // Dev-mode safety net: don't open listeners on an empty
    // registry — the very first request would panic in
    // `route_tenant`. Poll for up to 15s; if nothing converges,
    // surface a clear error rather than a runtime panic.
    if opts.dev && registry.num_boxes() == 0 {
        let dev_deadline = std::time::Instant::now() + Duration::from_secs(15);
        while registry.num_boxes() == 0 && std::time::Instant::now() < dev_deadline {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let added = std::sync::atomic::AtomicU64::new(0);
            let errs = std::sync::atomic::AtomicU64::new(0);
            let _ =
                agentdb_core::tablet_reconciler::reconcile_once(&registry, &added, &errs).await;
        }
        if registry.num_boxes() == 0 {
            anyhow::bail!(
                "dev mode failed to claim any tablets within 15s; \
                 leader election or placement is stuck. Check meta-store \
                 connectivity and `RUST_LOG=debug` for details"
            );
        }
        tracing::info!(
            num_boxes = registry.num_boxes(),
            "dev mode bootstrap complete"
        );
    }

    // Drain-aware renewal loops, one per acquired box.
    {
        let mut renewal_jobs: Vec<LeaseRenewalJob> = Vec::new();
        for b in registry.boxes_snapshot() {
            if let Some(lease) = b.lease() {
                renewal_jobs.push(LeaseRenewalJob::start_with_drain(
                    lease.clone(),
                    drain_flag.clone(),
                ));
            }
        }
        // Held forever; lease lifetime equals process lifetime.
        std::mem::forget(renewal_jobs);
    }

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

    // M20 multi-node: build the node-address proxy so writes
    // for tablets owned elsewhere forward instead of falling
    // back to tenant-hash. The refresh tick is shorter than
    // LocationProxy's because node address changes (node
    // restart, IP change) are failover-critical.
    let node_address_proxy =
        agentdb_core::NodeAddressProxy::new(meta.clone(), cfg.node_id.clone())
            .await
            .context("initialize NodeAddressProxy")?;
    let node_addr_refresh_secs: u64 = std::env::var("AGENTDB_NODE_ADDR_REFRESH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let _node_addr_refresh_job = agentdb_core::NodeAddressRefreshJob::start(
        node_address_proxy.clone(),
        Duration::from_secs(node_addr_refresh_secs),
    );
    std::mem::forget(_node_addr_refresh_job);
    registry
        .attach_node_address(node_address_proxy.clone())
        .map_err(|_| anyhow::anyhow!("node address proxy already attached"))?;
    tracing::info!(
        refresh_secs = node_addr_refresh_secs,
        "node address proxy attached to registry; cross-node forwarding active"
    );

    // Global per-tenant rate limits. Hydrate the in-memory
    // registry from the meta store so the first request for a
    // tenant with a configured quota sees its bucket
    // immediately — hydration before we bind the listener
    // avoids a cold-start window where a quota'd tenant is
    // unthrottled.
    let tenant_quota_registry = Arc::new(agentdb_core::TenantQuotaRegistry::new());
    hydrate_tenant_quotas(&tenant_quota_registry, &meta).await?;
    registry
        .attach_tenant_quotas(tenant_quota_registry.clone())
        .map_err(|_| anyhow::anyhow!("tenant quota registry already attached"))?;

    // Slow-query ring buffer. `AGENTDB_SLOW_QUERY_DISABLE`
    // skips the attach entirely (useful for benchmarks where
    // recording overhead would show up in the measurement).
    // Otherwise threshold + capacity come from env.
    let slow_query_disabled = std::env::var("AGENTDB_SLOW_QUERY_DISABLE")
        .ok()
        .filter(|s| !s.is_empty())
        .is_some();
    if slow_query_disabled {
        tracing::info!("slow-query buffer disabled via AGENTDB_SLOW_QUERY_DISABLE");
    } else {
        let slow_query_buffer = Arc::new(agentdb_core::SlowQueryBuffer::from_env());
        tracing::info!(
            threshold_ms = slow_query_buffer.threshold_ms(),
            "slow-query buffer attached to registry",
        );
        registry
            .attach_slow_queries(slow_query_buffer)
            .map_err(|_| anyhow::anyhow!("slow-query buffer already attached"))?;
    }

    // Scheduled backup. Off by default. When
    // AGENTDB_BACKUP_INTERVAL_SECS is set, the scheduler loop
    // runs on the current leader; followers stay idle. Status
    // handle is always attached to the registry so admin
    // endpoints can read last-run info regardless of leader
    // state — the value carries `enabled=false` on followers.
    match agentdb_core::BackupSchedulerConfig::from_env() {
        Ok(Some(backup_cfg)) => {
            tracing::info!(
                base_dir = %backup_cfg.base_dir.display(),
                interval_secs = backup_cfg.interval.as_secs(),
                retain = backup_cfg.retain,
                "starting scheduled backup loop (leader-gated)",
            );
            let scheduler = agentdb_core::BackupScheduler::start(
                registry.kv().clone(),
                meta.clone(),
                election.clone(),
                Some(cfg.node_id.clone()),
                backup_cfg,
            );
            registry
                .attach_backup_status(scheduler.status_handle())
                .map_err(|_| anyhow::anyhow!("backup scheduler status already attached"))?;
            std::mem::forget(scheduler);
        }
        Ok(None) => {
            tracing::debug!(
                "scheduled backup off (AGENTDB_BACKUP_INTERVAL_SECS unset)"
            );
        }
        Err(msg) => {
            anyhow::bail!("backup scheduler config: {msg}");
        }
    }

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

    // Node-lifecycle reaper. Leader-owned: flips stale `alive`
    // rows to `dead` and GCs rows that have been `dead` long
    // enough. Paired with the graceful-shutdown hook below —
    // together they keep the `nodes` table honest across crashes
    // and clean restarts.
    let node_reaper_cfg = NodeReaperConfig::from_env();
    let node_reaper = NodeReaper::start(meta.clone(), election.clone(), node_reaper_cfg);
    tracing::info!(
        tick_secs = node_reaper_cfg.tick.as_secs(),
        dead_after_secs = node_reaper_cfg.dead_after.as_secs(),
        gc_after_secs = node_reaper_cfg.gc_after.as_secs(),
        "node-lifecycle reaper started"
    );

    // Periodic GC for the per-session and per-tenant lock
    // maps. Both grow monotonically as new external ids are
    // touched; this loop drops entries with no active
    // holder so memory tracks the working set rather than
    // the historical set.
    let lock_gc_cfg = LockGcConfig::from_env();
    let _lock_gc = LockGc::start(registry.clone(), lock_gc_cfg);
    tracing::info!(
        tick_secs = lock_gc_cfg.tick.as_secs(),
        "lock-map GC loop started"
    );
    std::mem::forget(_lock_gc);

    // Change-notifier: when the meta store signals a change to
    // the `nodes` or `shards` tables (pg_notify on Postgres; inert
    // on SQLite), force-refresh the in-memory caches instead of
    // waiting for the next periodic tick. Reduces cross-node
    // routing staleness after failover or shard rebalance from
    // seconds to ~tens of milliseconds.
    {
        use agentdb_meta::change_notifier::{subscribe, Channel};
        let mut rx = subscribe(&meta, &[Channel::Nodes, Channel::Shards]);
        let node_proxy = node_address_proxy.clone();
        let loc_proxy = location_proxy.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event.channel {
                    Channel::Nodes => {
                        if let Err(err) = node_proxy.refresh().await {
                            tracing::warn!(
                                %err,
                                payload = %event.payload,
                                "nodes-change refresh failed"
                            );
                        }
                    }
                    Channel::Shards => {
                        if let Err(err) = loc_proxy.refresh().await {
                            tracing::warn!(
                                %err,
                                payload = %event.payload,
                                "shards-change refresh failed"
                            );
                        }
                    }
                    Channel::Tablets => {
                        // No dedicated cache for tablets today —
                        // NodeAddressProxy already joins tablets×nodes,
                        // so any tablet ownership change also lands as
                        // a Nodes-channel event (via the reaper's
                        // follow-up writes). No-op.
                    }
                }
            }
        });
    }

    // M25 hot-tablet detector. Runs on the scheduler leader;
    // followers report an empty hot set. Admin endpoints read
    // the detector's in-memory snapshot.
    let hot_tablet_cfg = agentdb_core::HotTabletConfig::from_env();
    let hot_detector = agentdb_core::HotTabletDetector::start(
        registry.clone(),
        election.clone(),
        hot_tablet_cfg,
    );
    tracing::info!(
        tick_secs = hot_tablet_cfg.tick.as_secs(),
        wait_ratio_threshold = hot_tablet_cfg.wait_ratio_threshold,
        consecutive_samples = hot_tablet_cfg.consecutive_samples,
        "hot-tablet detector started"
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

    // Lifecycle engines, segment GCs, and index-snapshot jobs
    // are spawned per-box from the post-reconcile registry
    // snapshot. Boxes that arrive later via the reconciler's
    // periodic loop don't get these jobs in this PR — that's
    // tracked as follow-up; the dominant case (single-node and
    // bootstrap-on-boot multi-node) is covered.
    let initial_boxes = registry.boxes_snapshot();
    let mut engines: Vec<LifecycleEngine> = Vec::with_capacity(initial_boxes.len());
    for box_ in &initial_boxes {
        engines.push(LifecycleEngine::start(
            kv.clone(),
            object_store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_.clone(),
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

    let mut gcs: Vec<SegmentGc> = Vec::with_capacity(initial_boxes.len());
    for box_ in &initial_boxes {
        gcs.push(SegmentGc::start(
            object_store.clone(),
            tenants.clone(),
            sessions.clone(),
            box_.clone(),
            cfg.segment_gc_cfg,
            Some(reporter.clone()),
        ));
    }
    std::mem::forget(gcs);

    let snapshot_cfg = agentdb_core::IndexSnapshotJobConfig::from_env();
    let mut snapshot_jobs: Vec<agentdb_core::IndexSnapshotJob> =
        Vec::with_capacity(initial_boxes.len());
    for box_ in &initial_boxes {
        snapshot_jobs.push(agentdb_core::IndexSnapshotJob::start(
            object_store.clone(),
            box_.clone(),
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
    // Admin-plane auth is a separate toggle: deployments
    // might require admin auth while leaving tenant
    // endpoints open (behind a tenant-identity proxy) or
    // vice versa.
    let require_admin_auth = std::env::var("AGENTDB_REQUIRE_ADMIN_AUTH")
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
        admin_auth: if require_admin_auth {
            Some(agentdb_api::admin_auth_middleware::AdminAuthConfig {
                meta: Some(meta.clone()),
            })
        } else {
            None
        },
    };

    // Auto-shed scheduler (post-M25 operator ergonomics).
    // Off by default. When on, consumes the hot-tablet
    // detector's snapshot and fires shed-with-migrate
    // against the least-loaded sibling tablet, with a
    // per-tablet cooldown to prevent thrash.
    let auto_shed_enabled = std::env::var("AGENTDB_AUTO_SHED_ENABLED")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let _auto_shed_job = if auto_shed_enabled {
        let cfg = agentdb_api::auto_shed::AutoShedConfig::from_env();
        tracing::info!(
            tick_secs = cfg.tick.as_secs(),
            cooldown_secs = cfg.cooldown.as_secs(),
            fraction = cfg.fraction,
            "auto-shed scheduler starting"
        );
        Some(agentdb_api::auto_shed::AutoShedJob::start(
            http_state.clone(),
            hot_detector.clone(),
            meta.clone(),
            cfg,
        ))
    } else {
        None
    };
    if let Some(job) = _auto_shed_job {
        std::mem::forget(job);
    }
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

    // Reload engine: a closure that re-reads env + meta and
    // pushes fresh values into the live subsystems. Shared
    // by the POST /api/admin/reload handler and the SIGHUP
    // task below.
    let reload_handle: agentdb_api::admin_reload::ReloadHandle = {
        let meta = meta.clone();
        let quota_registry = tenant_quota_registry.clone();
        let registry_for_reload = registry.clone();
        let tracing_reload = tracing_reload.clone();
        Arc::new(move || {
            let meta = meta.clone();
            let quota_registry = quota_registry.clone();
            let registry = registry_for_reload.clone();
            let tracing_reload = tracing_reload.clone();
            Box::pin(async move {
                do_reload(&meta, &quota_registry, &registry, &tracing_reload).await
            })
        })
    };

    // SIGHUP handler (unix). Firing SIGHUP runs the same
    // logic as POST /api/admin/reload; admin CLIs that prefer
    // signals (nginx muscle memory) get the same thing.
    #[cfg(unix)]
    {
        let reload_for_signal = reload_handle.clone();
        tokio::spawn(async move {
            let mut stream = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::hangup(),
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "SIGHUP handler install failed; hot-reload requires POST /api/admin/reload");
                    return;
                }
            };
            while stream.recv().await.is_some() {
                tracing::info!("SIGHUP received; reloading config");
                match (reload_for_signal)().await {
                    Ok(summary) => tracing::info!(
                        slow_query_threshold_ms = summary.slow_query_threshold_ms,
                        tenant_quotas_added = summary.tenant_quotas_added.len(),
                        tenant_quotas_removed = summary.tenant_quotas_removed.len(),
                        "SIGHUP reload complete",
                    ),
                    Err(e) => tracing::warn!(error = %e, "SIGHUP reload failed"),
                }
            }
        });
    }

    let app = router(http_state)
        .layer(axum::Extension(meta.clone()))
        .layer(axum::Extension(election.clone()))
        .layer(axum::Extension(reaper.clone()))
        .layer(axum::Extension(node_reaper.clone()))
        .layer(axum::Extension(placement.clone()))
        .layer(axum::Extension(reconciler.clone()))
        .layer(axum::Extension(hot_detector.clone()))
        .layer(axum::Extension(location_proxy.clone()))
        .layer(axum::Extension(reload_handle));
    // Resolve TLS config. Explicit flags win; else env
    // vars; else `--dev-tls` ephemeral; else plaintext.
    let tls_cfg = resolve_tls(&opts).await?;
    // Mongo wire TLS is resolved AFTER REST so it can
    // optionally reuse the REST cert via env.
    let mongo_tls_cfg = resolve_mongo_tls(&opts, tls_cfg.as_ref()).await?;
    let mongo_listener = tokio::net::TcpListener::bind(cfg.mongo_addr)
        .await
        .context("bind mongo listener")?;
    tracing::info!(
        http_addr = %cfg.http_addr,
        mongo_addr = %cfg.mongo_addr,
        objects = %cfg.objects_dir.display(),
        dev = opts.dev,
        tls = tls_cfg.is_some(),
        mongo_tls = mongo_tls_cfg.is_some(),
        "agentdb listening"
    );
    // Dev-mode welcome panel — printed once at startup so a
    // first-time user sees the URLs, the auth posture, and a
    // copy-pasteable curl that immediately works. Suppressed
    // outside `--dev` because production operators see the
    // same info via tracing/metrics and don't want banner spam
    // in their journald.
    if opts.dev {
        print_dev_banner(&cfg, tls_cfg.is_some(), mongo_tls_cfg.is_some());
    }

    let http_task = if let Some(rustls_cfg) = tls_cfg {
        // TLS path. axum_server owns its own listener, so we
        // give it the socket address directly instead of a
        // pre-bound tokio listener.
        let addr = cfg.http_addr;
        let service = app.into_make_service();
        tokio::spawn(async move {
            axum_server::bind_rustls(addr, rustls_cfg)
                .serve(service)
                .await
                .context("https serve")
        })
    } else {
        let http_listener = tokio::net::TcpListener::bind(cfg.http_addr)
            .await
            .context("bind http listener")?;
        tokio::spawn(async move { axum::serve(http_listener, app).await.context("http serve") })
    };
    let mongo_task = if let Some(mongo_tls) = mongo_tls_cfg {
        tokio::spawn(async move {
            serve_mongo_tls(mongo_listener, mongo_tls, wire_state)
                .await
                .map_err(|e| anyhow::anyhow!("mongo tls serve: {e}"))
        })
    } else {
        tokio::spawn(async move {
            serve_mongo(mongo_listener, wire_state)
                .await
                .map_err(|e| anyhow::anyhow!("mongo serve: {e}"))
        })
    };

    // Shutdown: on SIGTERM/SIGINT, flip our own nodes row to
    // `state='leaving'` before the process exits. Peers that
    // cache node addresses invalidate immediately on the
    // change_notifier signal (Postgres) or on their next
    // periodic refresh (SQLite), so they route around us
    // without waiting for the heartbeat-stale timeout.
    let graceful = graceful_shutdown_signal();
    tokio::pin!(graceful);

    tokio::select! {
        r = http_task => r.context("http task join")??,
        r = mongo_task => r.context("mongo task join")??,
        sig = &mut graceful => {
            tracing::info!(signal = sig, "shutdown signal received; marking node as leaving");
            if let Err(err) =
                agentdb_meta::nodes::set_state(meta.pool(), &cfg.node_id, "leaving", now_ms_i64())
                    .await
            {
                tracing::warn!(
                    %err,
                    node_id = %cfg.node_id,
                    "failed to mark node as leaving on shutdown"
                );
            } else {
                tracing::info!(node_id = %cfg.node_id, "node marked as leaving");
            }
        }
    };
    Ok(())
}

/// One-screen welcome panel printed when `--dev` is set. Lists
/// the listen addresses, the dashboard, the auth posture, a
/// copy-pasteable curl that immediately works, and pointers to
/// the Python SDK + quickstart example. Newcomers see this and
/// get to "round-trip an event" without reading docs first.
fn print_dev_banner(cfg: &Resolved, rest_tls: bool, mongo_tls: bool) {
    let rest_scheme = if rest_tls { "https" } else { "http" };
    let rest_url = format!("{rest_scheme}://{}", cfg.http_addr);
    let mongo_scheme = if mongo_tls { "mongodb+srv" } else { "mongodb" };
    let mongo_url = format!("{mongo_scheme}://{}", cfg.mongo_addr);
    let auth_required = std::env::var("AGENTDB_REQUIRE_AUTH")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let auth_line = if auth_required {
        "enabled (set AGENTDB_REQUIRE_AUTH=false for fully open dev mode)"
    } else {
        "disabled (dev default — open to whoever can reach the listener)"
    };
    let bar = "─".repeat(64);
    println!();
    println!("{bar}");
    println!("  AgentDB ready (dev mode)");
    println!("{bar}");
    println!("  REST       {rest_url}");
    println!("  Dashboard  {rest_url}/dashboard");
    println!("  Metrics    {rest_url}/metrics");
    println!("  Mongo wire {mongo_url}");
    println!();
    println!("  Auth       {auth_line}");
    println!("  Storage    ephemeral — state is wiped on restart");
    println!();
    println!("  Quickstart (one round trip):");
    println!(
        "    curl -X POST {rest_url}/api/tenants/demo/sessions/agent-1/append_and_fetch \\"
    );
    println!("         -H 'Content-Type: application/json' \\");
    println!(
        "         -d '{{\"event\":{{\"role\":\"user\",\"text\":\"hello\"}},\"fetch_last_n\":5}}'"
    );
    println!();
    println!("  Python SDK:  pip install -e clients/python");
    println!("  Example:     python examples/agents/python/quickstart.py");
    println!("{bar}");
    println!();
}

/// Wait for SIGTERM or SIGINT and return the signal name.
/// Resolves the first of the two to fire. Used to drive the
/// graceful-shutdown path without hooking the whole tower
/// middleware stack.
async fn graceful_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return "sigterm-install-failed",
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(_) => return "sigint-install-failed",
        };
        tokio::select! {
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        // Non-unix platforms: Ctrl+C is the only widely
        // supported portable signal. Treat it the same as
        // SIGINT on the unix path.
        let _ = tokio::signal::ctrl_c().await;
        "CTRL_C"
    }
}
