//! `agentdb` binary — server entrypoint.
//!
//! Mounts the HTTP routes from `agentdb-api` against a `Runtime`. By default
//! the runtime is RocksDB-backed at `--data-dir/runtime`; pass `--dev` or
//! `--in-memory` to get the ephemeral backend (useful for smoke tests and
//! local dev).

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::routing::get;
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "agentdb.toml";
const SAMPLE_CONFIG: &str = include_str!("../../../config/agentdb.toml");
const SLATEDB_RUNTIME_PREFIX: &str = "agentdb/runtime";
const OBJECT_STORE_CONTENT_PREFIX: &str = "agentdb/content";
const RAFT_SNAPSHOT_OBJECT_PREFIX: &str = "agentdb/raft-snapshots";
const DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
const DEFAULT_SCHEDULER_LEASE_MS: i64 = 30_000;
const DEFAULT_SCHEDULER_RENEW_INTERVAL_MS: u64 = 10_000;
const DEFAULT_STALE_NODE_THRESHOLD_MS: i64 = 15_000;
const DEFAULT_REAPER_TICK_MS: u64 = 10_000;
const DEFAULT_REAPER_SAFETY_MARGIN_MS: i64 = 5_000;
const DEFAULT_REBALANCE_MAX_MOVEMENTS: usize = 8;
const DEFAULT_TABLET_MOVEMENT_BATCH_SIZE: i64 = 64;
const DEFAULT_TABLET_MOVEMENT_EXECUTOR_INTERVAL_MS: u64 = 5_000;
const DEFAULT_TABLET_LIFECYCLE_MAX_ACTIONS: usize = 4;
const DEFAULT_RECORD_INDEX_WORKER_INTERVAL_MS: u64 = 1_000;
const DEFAULT_RAFT_OUTBOX_INTERVAL_MS: u64 = 100;
const DEFAULT_RAFT_OUTBOX_MAX_MESSAGES_PER_TICK: usize = 1_024;
const DEFAULT_RAFT_SNAPSHOT_INTERVAL_MS: u64 = 60_000;
const DEFAULT_RAFT_SNAPSHOT_MIN_LAG: u64 = 1_024;
const DEFAULT_RAFT_SNAPSHOT_RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_RAFT_SNAPSHOT_PRUNE_INTERVAL_MS: u64 = 5 * 60_000;
const DEFAULT_RAFT_SNAPSHOT_PRUNE_BATCH: i64 = 256;
const DEFAULT_RAFT_SNAPSHOT_RESTORE_INTERVAL_MS: u64 = 30_000;
const DEFAULT_CONTENT_MIRROR_QUEUE_CAPACITY: usize = 1024;

#[derive(Parser, Debug)]
#[command(name = "agentdb", version, about = "AgentDB runtime database")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Start the HTTP server with the runtime router.
    Serve(ServeOpts),
    /// Check local single-node configuration and data directories.
    Doctor(DoctorOpts),
    /// Write a starter single-node config file.
    InitConfig(InitConfigOpts),
}

#[derive(Parser, Debug)]
struct ServeOpts {
    /// Path to a TOML config file. Defaults to `agentdb.toml` when it exists.
    #[arg(long)]
    config: Option<PathBuf>,

    /// `host:port` to bind. Defaults to `127.0.0.1:7878`.
    ///
    #[arg(long)]
    bind: Option<SocketAddr>,

    /// Filesystem path that holds the RocksDB-backed runtime state. Created if
    /// it doesn't exist. Ignored when `--in-memory` is set.
    ///
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Filesystem path that holds artifact content (raw blobs). Created if
    /// it doesn't exist. Defaults to `<data_dir>/content`. Ignored when
    /// `--in-memory` is set.
    ///
    #[arg(long)]
    content_dir: Option<PathBuf>,

    /// Use an ephemeral in-memory backend instead of RocksDB. Convenient for
    /// smoke testing; everything is lost on shutdown.
    #[arg(long)]
    in_memory: bool,

    /// Alias for `--in-memory` with friendlier local-development wording.
    #[arg(long)]
    dev: bool,

    /// Enable metadata-backed routing for this node id. When unset, the server
    /// keeps the single-node routing-disabled behavior.
    #[arg(long)]
    route_node_id: Option<String>,

    /// Shard count used by the route planner when no session locator exists.
    #[arg(long)]
    route_shard_count: Option<i64>,

    /// Optional region for residency-aware routing. Compared with
    /// `nodes.region`, falling back to `availability_zone` for older rows that
    /// only populated the prototype location label.
    #[arg(long)]
    route_region: Option<String>,

    /// Older location label. Prefer `--route-region` for new deployments.
    #[arg(long)]
    route_location: Option<String>,

    /// Route snapshot refresh interval in milliseconds. Periodic refresh is the
    /// correctness path for SQLite and session locator changes; Postgres
    /// LISTEN/NOTIFY can trigger faster refreshes for shard/node/tablet changes.
    #[arg(long)]
    route_refresh_interval_ms: Option<u64>,

    /// Timeout for same-cluster routing proxy requests.
    #[arg(long)]
    route_proxy_timeout_ms: Option<u64>,

    /// Maximum owner-node response body size that the routing proxy will pass
    /// through to the original caller.
    #[arg(long)]
    route_proxy_max_response_bytes: Option<u64>,

    /// Optional MongoDB wire-protocol compatibility listener, e.g. 127.0.0.1:7879.
    #[arg(long)]
    mongo_bind: Option<SocketAddr>,

    /// Optional MongoDB PLAIN auth username. Requires `--mongo-password`.
    #[arg(long)]
    mongo_username: Option<String>,

    /// Optional MongoDB PLAIN auth password. Requires `--mongo-username`.
    #[arg(long)]
    mongo_password: Option<String>,

    /// Shared secret for internal node-to-node APIs. Can also be set with
    /// AGENTDB_INTERNAL_AUTH_TOKEN.
    #[arg(long, env = "AGENTDB_INTERNAL_AUTH_TOKEN")]
    internal_auth_token: Option<String>,

    /// Previous internal node-to-node API token accepted during rotation. Can
    /// also be set with AGENTDB_INTERNAL_AUTH_PREVIOUS_TOKEN.
    #[arg(long, env = "AGENTDB_INTERNAL_AUTH_PREVIOUS_TOKEN")]
    internal_auth_previous_token: Option<String>,
}

#[derive(Parser, Debug)]
struct DoctorOpts {
    /// Path to a TOML config file. Defaults to `agentdb.toml` when it exists.
    #[arg(long)]
    config: Option<PathBuf>,

    /// `host:port` to bind. Overrides config file.
    #[arg(long)]
    bind: Option<SocketAddr>,

    /// Filesystem path that holds persistent runtime state.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Filesystem path that holds artifact content.
    #[arg(long)]
    content_dir: Option<PathBuf>,

    /// Check an ephemeral in-memory setup.
    #[arg(long)]
    in_memory: bool,

    /// Alias for `--in-memory`.
    #[arg(long)]
    dev: bool,
}

#[derive(Parser, Debug)]
struct InitConfigOpts {
    /// Path to write. Defaults to `agentdb.toml`.
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    output: PathBuf,

    /// Overwrite the file if it already exists.
    #[arg(long)]
    force: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    match args.cmd {
        Cmd::Serve(opts) => serve(opts).await,
        Cmd::Doctor(opts) => doctor(opts),
        Cmd::InitConfig(opts) => init_config(opts),
    }
}

async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let config = ResolvedServeConfig::from_serve_opts(opts)?;
    if !config.in_memory {
        std::fs::create_dir_all(&config.data_dir)?;
    }
    let route_location = config
        .route_region
        .clone()
        .or(config.route_location.clone());
    let raft_registry = config
        .internal_auth_token
        .as_ref()
        .map(|_| agentdb_api::raft_ingress::RaftRsGroupRegistry::shared());
    let route_startup =
        prepare_route_startup(&config, route_location.clone(), raft_registry.clone()).await?;
    let runtime_raft_logs: Option<
        BTreeMap<i64, Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>>,
    > = route_startup.as_ref().and_then(|startup| {
        startup.raft_bootstrap.as_ref().and_then(|bootstrap| {
            if bootstrap.local_logs.is_empty() {
                None
            } else {
                Some(
                    bootstrap
                        .local_logs
                        .iter()
                        .map(|(tablet_id, log)| {
                            (
                                *tablet_id,
                                log.clone()
                                    as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
                            )
                        })
                        .collect(),
                )
            }
        })
    });
    if matches!(config.deployment_mode, DeploymentMode::Cluster)
        && !runtime_raft_logs
            .as_ref()
            .is_some_and(|logs| logs.contains_key(&agentdb_core::RUNTIME_PRIMARY_TABLET_ID))
    {
        anyhow::bail!(
            "deployment.mode=cluster requires local node {} to be a voter or learner for runtime primary tablet {}",
            config.route_node_id.as_deref().unwrap_or("<missing-node-id>"),
            agentdb_core::RUNTIME_PRIMARY_TABLET_ID
        );
    }
    let (runtime, content_mirror) = if config.in_memory {
        tracing::info!("using in-memory runtime (state lost on shutdown)");
        (agentdb_core::Runtime::in_memory(), None)
    } else {
        let kv: Arc<dyn agentdb_storage::KvStore + Send + Sync> = match config.kv_backend {
            KvBackendKind::RocksDb => {
                let kv_path = config.data_dir.join("runtime");
                std::fs::create_dir_all(&kv_path)?;
                tracing::info!(path = %kv_path.display(), "opened RocksDB runtime backend");
                Arc::new(agentdb_storage::RocksKvStore::open(&kv_path)?)
            }
            KvBackendKind::SlateDb => {
                let object_store = build_object_store(
                    config
                        .object_store
                        .as_ref()
                        .expect("slatedb requires object store config"),
                )?;
                tracing::info!(prefix = %SLATEDB_RUNTIME_PREFIX, "opened SlateDB runtime backend");
                Arc::new(
                    agentdb_storage::SlateKvStore::open(object_store, SLATEDB_RUNTIME_PREFIX)
                        .await?,
                )
            }
        };

        let (content, content_mirror) = build_content_backend(&config)?;
        if let Some(logs) = runtime_raft_logs {
            tracing::info!(
                tablet_count = logs.len(),
                "opened runtime with raft-rs replicated tablet logs"
            );
            (
                agentdb_core::Runtime::with_kv_and_replicated_logs(kv, content, logs),
                content_mirror,
            )
        } else {
            (agentdb_core::Runtime::with_kv(kv, content), content_mirror)
        }
    };

    let record_index_worker_interval_ms = env_u64(
        "AGENTDB_RECORD_INDEX_WORKER_INTERVAL_MS",
        DEFAULT_RECORD_INDEX_WORKER_INTERVAL_MS,
    )
    .max(10);
    runtime
        .clone()
        .spawn_record_index_worker(Duration::from_millis(record_index_worker_interval_ms));
    tracing::info!(
        interval_ms = record_index_worker_interval_ms,
        "started record index outbox worker"
    );

    if let Some(mongo_bind) = config.mongo_bind {
        let mongo_listener = tokio::net::TcpListener::bind(mongo_bind).await?;
        let mongo_runtime = runtime.clone();
        let mongo_auth = config.mongo_auth();
        tokio::spawn(async move {
            if let Err(error) =
                agentdb_wire::server::serve_with_auth(mongo_listener, mongo_runtime, mongo_auth)
                    .await
            {
                tracing::error!(?error, "MongoDB compatibility listener stopped");
            }
        });
        tracing::info!(
            addr = %mongo_bind,
            auth_enabled = config.mongo_auth_enabled(),
            "MongoDB compatibility listener enabled"
        );
    }

    let mut state = agentdb_api::AppState::new(runtime.clone()).with_proxy_policy(
        agentdb_api::state::ProxyPolicy {
            timeout_ms: config.route_proxy_timeout_ms,
            max_response_bytes: config.route_proxy_max_response_bytes,
        },
    );
    if let Some(content_mirror) = content_mirror {
        state = state.with_content_mirror(content_mirror);
    }
    if let Some(token) = config.internal_auth_token.clone() {
        state = state.with_internal_auth_token(token);
        if let Some(previous_token) = config.internal_auth_previous_token.clone() {
            state = state.with_internal_auth_previous_token(previous_token);
        }
        if let Some(registry) = raft_registry.clone() {
            state = state.with_raft_registry(registry);
        }
    }
    if let Some(startup) = route_startup {
        spawn_route_snapshot_refresher(
            startup.meta.clone(),
            startup.routing.clone(),
            config.route_shard_count,
            route_location.clone(),
            config.route_refresh_interval_ms,
        );
        let node_address = format!("http://{}", config.bind);
        spawn_cluster_workers(
            startup.meta.clone(),
            startup.node_id.clone(),
            runtime.clone(),
            config
                .internal_auth_token
                .clone()
                .map(agentdb_api::internal_raft_http::RaftRsHttpClient::new),
            raft_registry.clone(),
            node_address,
            config.route_region.clone(),
            startup.scheduler_status.clone(),
            state.drain.clone(),
            ClusterWorkerConfig::from_env(),
        );
        if let Some(bootstrap) = startup.raft_bootstrap {
            if !bootstrap.tablet_ids.is_empty() {
                let internal_auth_token = config
                    .internal_auth_token
                    .clone()
                    .expect("raft registry requires internal auth token");
                let outbox =
                    agentdb_api::internal_raft_http::RaftRsHttpOutbox::new(internal_auth_token);
                let raft_outbox_interval_ms = env_u64(
                    "AGENTDB_RAFT_OUTBOX_INTERVAL_MS",
                    DEFAULT_RAFT_OUTBOX_INTERVAL_MS,
                )
                .max(100);
                let raft_outbox_max_messages_per_tick = env_usize(
                    "AGENTDB_RAFT_OUTBOX_MAX_MESSAGES_PER_TICK",
                    DEFAULT_RAFT_OUTBOX_MAX_MESSAGES_PER_TICK,
                )
                .max(1);
                if let Some(registry) = raft_registry.clone() {
                    refresh_raft_rs_peer_urls(&startup.meta, &registry, &outbox).await?;
                    let raft_snapshot_store = match config.object_store.as_ref() {
                        Some(object_store_config) => Some(Arc::new(RaftSnapshotObjectStore::new(
                            build_object_store(object_store_config)?,
                            RAFT_SNAPSHOT_OBJECT_PREFIX,
                        ))),
                        None => None,
                    };
                    spawn_raft_rs_registry_refresher(
                        startup.meta.clone(),
                        startup.node_id.clone(),
                        config.data_dir.join("raft-engine"),
                        registry.clone(),
                        runtime.clone(),
                        config.route_refresh_interval_ms,
                    );
                    spawn_raft_rs_peer_url_refresher(
                        startup.meta.clone(),
                        registry.clone(),
                        outbox.clone(),
                        config.route_refresh_interval_ms,
                    );
                    spawn_raft_rs_message_outbox_worker(
                        startup.meta.clone(),
                        registry.clone(),
                        outbox.clone(),
                        Duration::from_millis(raft_outbox_interval_ms),
                        raft_outbox_max_messages_per_tick,
                    );
                    let raft_snapshot_interval_ms = env_u64(
                        "AGENTDB_RAFT_SNAPSHOT_INTERVAL_MS",
                        DEFAULT_RAFT_SNAPSHOT_INTERVAL_MS,
                    )
                    .max(1_000);
                    let raft_snapshot_min_lag = env_u64(
                        "AGENTDB_RAFT_SNAPSHOT_MIN_LAG",
                        DEFAULT_RAFT_SNAPSHOT_MIN_LAG,
                    )
                    .max(1);
                    let raft_snapshot_retention_ms = env_i64(
                        "AGENTDB_RAFT_SNAPSHOT_RETENTION_MS",
                        DEFAULT_RAFT_SNAPSHOT_RETENTION_MS,
                    )
                    .max(0);
                    spawn_raft_rs_snapshot_worker(
                        startup.meta.clone(),
                        registry.clone(),
                        runtime.clone(),
                        outbox,
                        Duration::from_millis(raft_snapshot_interval_ms),
                        raft_snapshot_min_lag,
                        raft_snapshot_retention_ms,
                        raft_snapshot_store.clone(),
                    );
                    if let Some(snapshot_store) = raft_snapshot_store {
                        let restore_interval_ms = env_u64(
                            "AGENTDB_RAFT_SNAPSHOT_RESTORE_INTERVAL_MS",
                            DEFAULT_RAFT_SNAPSHOT_RESTORE_INTERVAL_MS,
                        )
                        .max(1_000);
                        spawn_raft_snapshot_restore_worker(
                            startup.meta.clone(),
                            startup.node_id.clone(),
                            registry.clone(),
                            runtime.clone(),
                            snapshot_store.clone(),
                            Duration::from_millis(restore_interval_ms),
                        );
                        let prune_interval_ms = env_u64(
                            "AGENTDB_RAFT_SNAPSHOT_PRUNE_INTERVAL_MS",
                            DEFAULT_RAFT_SNAPSHOT_PRUNE_INTERVAL_MS,
                        )
                        .max(1_000);
                        let prune_batch = env_i64(
                            "AGENTDB_RAFT_SNAPSHOT_PRUNE_BATCH",
                            DEFAULT_RAFT_SNAPSHOT_PRUNE_BATCH,
                        )
                        .max(1);
                        spawn_raft_snapshot_prune_worker(
                            startup.meta.clone(),
                            snapshot_store,
                            Duration::from_millis(prune_interval_ms),
                            prune_batch,
                        );
                    }
                }
                tracing::info!(
                    node_id = %startup.node_id,
                    tablet_count = bootstrap.tablet_ids.len(),
                    bootstrapped_tablet_count = bootstrap.bootstrapped_tablet_ids.len(),
                    outbox_interval_ms = raft_outbox_interval_ms,
                    outbox_max_messages_per_tick = raft_outbox_max_messages_per_tick,
                    "bootstrapped raft-rs tablet groups"
                );
            }
        }
        state = state
            .with_meta(startup.meta.clone())
            .with_scheduler(startup.scheduler_status)
            .with_routing(startup.routing);
        tracing::info!(
            node_id = %startup.node_id,
            shard_count = config.route_shard_count,
            location = ?route_location,
            refresh_interval_ms = config.route_refresh_interval_ms,
            "enabled metadata-backed route snapshot"
        );
    }
    let app = agentdb_api::router(state)
        .route("/dashboard/", get(agentdb_dashboard::index))
        .nest("/dashboard", agentdb_dashboard::router());
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    print_startup_banner(&config);
    tracing::info!(addr = %config.bind, "agentdb listening");
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedServeConfig {
    deployment_mode: DeploymentMode,
    bind: SocketAddr,
    data_dir: PathBuf,
    content_dir: PathBuf,
    kv_backend: KvBackendKind,
    content_backend: ContentBackendKind,
    object_store: Option<ResolvedObjectStoreConfig>,
    in_memory: bool,
    route_node_id: Option<String>,
    route_shard_count: i64,
    route_region: Option<String>,
    route_location: Option<String>,
    route_refresh_interval_ms: u64,
    route_proxy_timeout_ms: u64,
    route_proxy_max_response_bytes: u64,
    mongo_bind: Option<SocketAddr>,
    mongo_username: Option<String>,
    mongo_password: Option<String>,
    internal_auth_token: Option<String>,
    internal_auth_previous_token: Option<String>,
}

struct RouteStartup {
    node_id: String,
    meta: agentdb_meta::MetaStore,
    scheduler_status: agentdb_api::state::SchedulerStatus,
    routing: agentdb_api::routing_http::RouteRuntime,
    raft_bootstrap: Option<RaftRsRegistryBootstrap>,
}

struct RaftRsRegistryBootstrap {
    tablet_ids: Vec<i64>,
    bootstrapped_tablet_ids: Vec<i64>,
    local_logs: BTreeMap<i64, Arc<agentdb_storage::RaftRsReplicatedLog>>,
}

impl ResolvedServeConfig {
    fn mongo_auth(&self) -> Option<agentdb_wire::server::AuthConfig> {
        Some(agentdb_wire::server::AuthConfig {
            username: self.mongo_username.clone()?,
            password: self.mongo_password.clone()?,
        })
    }

    fn mongo_auth_enabled(&self) -> bool {
        self.mongo_username.is_some() && self.mongo_password.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClusterWorkerConfig {
    heartbeat_interval_ms: u64,
    scheduler_lease_ms: i64,
    scheduler_renew_interval_ms: u64,
    stale_node_threshold_ms: i64,
    reaper_tick_ms: u64,
    reaper_safety_margin_ms: i64,
    rebalance_max_movements: usize,
    tablet_movement_batch_size: i64,
    tablet_movement_executor_interval_ms: u64,
    tablet_lifecycle_split_max_shards: i64,
    tablet_lifecycle_merge_max_shards: i64,
    tablet_lifecycle_split_max_keys: i64,
    tablet_lifecycle_merge_max_keys: i64,
    tablet_lifecycle_split_max_storage_bytes: i64,
    tablet_lifecycle_merge_max_storage_bytes: i64,
    tablet_lifecycle_max_actions: usize,
}

impl ClusterWorkerConfig {
    fn from_env() -> Self {
        Self {
            heartbeat_interval_ms: env_u64(
                "AGENTDB_NODE_HEARTBEAT_MS",
                DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_MS,
            )
            .max(100),
            scheduler_lease_ms: env_i64("AGENTDB_SCHEDULER_LEASE_MS", DEFAULT_SCHEDULER_LEASE_MS)
                .max(1_000),
            scheduler_renew_interval_ms: env_u64(
                "AGENTDB_SCHEDULER_RENEW_INTERVAL_MS",
                DEFAULT_SCHEDULER_RENEW_INTERVAL_MS,
            )
            .max(100),
            stale_node_threshold_ms: env_i64(
                "AGENTDB_STALE_NODE_THRESHOLD_MS",
                DEFAULT_STALE_NODE_THRESHOLD_MS,
            )
            .max(1_000),
            reaper_tick_ms: env_u64("AGENTDB_REAPER_TICK_MS", DEFAULT_REAPER_TICK_MS).max(100),
            reaper_safety_margin_ms: env_i64(
                "AGENTDB_REAPER_SAFETY_MARGIN_MS",
                DEFAULT_REAPER_SAFETY_MARGIN_MS,
            )
            .max(0),
            rebalance_max_movements: env_usize(
                "AGENTDB_REBALANCE_MAX_MOVEMENTS",
                DEFAULT_REBALANCE_MAX_MOVEMENTS,
            ),
            tablet_movement_batch_size: env_i64(
                "AGENTDB_TABLET_MOVEMENT_BATCH_SIZE",
                DEFAULT_TABLET_MOVEMENT_BATCH_SIZE,
            )
            .clamp(1, 1_000),
            tablet_movement_executor_interval_ms: env_u64(
                "AGENTDB_TABLET_MOVEMENT_EXECUTOR_INTERVAL_MS",
                DEFAULT_TABLET_MOVEMENT_EXECUTOR_INTERVAL_MS,
            )
            .max(1_000),
            tablet_lifecycle_split_max_shards: env_i64(
                "AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_SHARDS",
                0,
            )
            .max(0),
            tablet_lifecycle_merge_max_shards: env_i64(
                "AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_SHARDS",
                0,
            )
            .max(0),
            tablet_lifecycle_split_max_keys: env_i64("AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_KEYS", 0)
                .max(0),
            tablet_lifecycle_merge_max_keys: env_i64("AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_KEYS", 0)
                .max(0),
            tablet_lifecycle_split_max_storage_bytes: env_i64(
                "AGENTDB_TABLET_LIFECYCLE_SPLIT_MAX_STORAGE_BYTES",
                0,
            )
            .max(0),
            tablet_lifecycle_merge_max_storage_bytes: env_i64(
                "AGENTDB_TABLET_LIFECYCLE_MERGE_MAX_STORAGE_BYTES",
                0,
            )
            .max(0),
            tablet_lifecycle_max_actions: env_usize(
                "AGENTDB_TABLET_LIFECYCLE_MAX_ACTIONS",
                DEFAULT_TABLET_LIFECYCLE_MAX_ACTIONS,
            )
            .max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeploymentMode {
    SingleNode,
    Cluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KvBackendKind {
    RocksDb,
    SlateDb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentBackendKind {
    LocalFs,
    ObjectStore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectStoreBackendKind {
    LocalFs,
    S3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedObjectStoreConfig {
    backend: ObjectStoreBackendKind,
    local_root: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
}

impl ResolvedServeConfig {
    fn from_serve_opts(opts: ServeOpts) -> anyhow::Result<Self> {
        let file = load_config(opts.config.as_ref())?;
        let bind = opts.bind.or(file.server.bind).unwrap_or_else(default_bind);
        let data_dir = opts
            .data_dir
            .or(file.storage.data_dir)
            .unwrap_or_else(default_data_dir);
        let content_dir = opts
            .content_dir
            .or(file.storage.content_dir)
            .unwrap_or_else(|| data_dir.join("content"));
        let deployment_mode = resolve_deployment_mode(&file.deployment)?;
        let kv_backend = resolve_kv_backend(&file.storage.kv)?;
        let content_backend = resolve_content_backend(&file.storage.content)?;
        let object_store = resolve_object_store_config(
            file.storage.object_store.as_ref(),
            data_dir.join("object-store"),
        )?;
        let in_memory = opts.in_memory || opts.dev || file.server.dev.unwrap_or(false);
        let route_node_id = opts.route_node_id.or(file.routing.node_id);
        validate_storage_backend_config(
            deployment_mode,
            kv_backend,
            content_backend,
            object_store.as_ref(),
            in_memory,
            route_node_id.as_deref(),
        )?;
        let mongo_username = opts
            .mongo_username
            .or(file.mongodb.username)
            .or_else(|| std::env::var("AGENTDB_MONGODB_USERNAME").ok());
        let mongo_password = opts
            .mongo_password
            .or(file.mongodb.password)
            .or_else(|| std::env::var("AGENTDB_MONGODB_PASSWORD").ok());
        validate_mongo_auth(&mongo_username, &mongo_password)?;
        let internal_auth_token = opts
            .internal_auth_token
            .or(file.server.internal_auth_token)
            .or_else(|| std::env::var("AGENTDB_INTERNAL_AUTH_TOKEN").ok());
        let internal_auth_previous_token = opts
            .internal_auth_previous_token
            .or(file.server.internal_auth_previous_token)
            .or_else(|| std::env::var("AGENTDB_INTERNAL_AUTH_PREVIOUS_TOKEN").ok());
        validate_internal_auth_token(deployment_mode, &internal_auth_token)?;
        validate_previous_internal_auth_token(&internal_auth_token, &internal_auth_previous_token)?;

        Ok(Self {
            deployment_mode,
            bind,
            data_dir,
            content_dir,
            kv_backend,
            content_backend,
            object_store,
            in_memory,
            route_node_id,
            route_shard_count: opts
                .route_shard_count
                .or(file.routing.shard_count)
                .unwrap_or(agentdb_meta::shards::DEFAULT_SHARD_COUNT),
            route_region: opts.route_region.or(file.routing.region),
            route_location: opts.route_location.or(file.routing.location),
            route_refresh_interval_ms: opts
                .route_refresh_interval_ms
                .or(file.routing.refresh_interval_ms)
                .unwrap_or(5_000),
            route_proxy_timeout_ms: opts
                .route_proxy_timeout_ms
                .or(file.routing.proxy_timeout_ms)
                .unwrap_or(5_000),
            route_proxy_max_response_bytes: opts
                .route_proxy_max_response_bytes
                .or(file.routing.proxy_max_response_bytes)
                .unwrap_or(16 * 1024 * 1024),
            mongo_bind: opts.mongo_bind.or(file.mongodb.bind),
            mongo_username,
            mongo_password,
            internal_auth_token,
            internal_auth_previous_token,
        })
    }

    fn from_doctor_opts(opts: DoctorOpts) -> anyhow::Result<Self> {
        let file = load_config(opts.config.as_ref())?;
        let bind = opts.bind.or(file.server.bind).unwrap_or_else(default_bind);
        let data_dir = opts
            .data_dir
            .or(file.storage.data_dir)
            .unwrap_or_else(default_data_dir);
        let content_dir = opts
            .content_dir
            .or(file.storage.content_dir)
            .unwrap_or_else(|| data_dir.join("content"));
        let deployment_mode = resolve_deployment_mode(&file.deployment)?;
        let kv_backend = resolve_kv_backend(&file.storage.kv)?;
        let content_backend = resolve_content_backend(&file.storage.content)?;
        let object_store = resolve_object_store_config(
            file.storage.object_store.as_ref(),
            data_dir.join("object-store"),
        )?;
        let in_memory = opts.in_memory || opts.dev || file.server.dev.unwrap_or(false);
        let route_node_id = file.routing.node_id;
        validate_storage_backend_config(
            deployment_mode,
            kv_backend,
            content_backend,
            object_store.as_ref(),
            in_memory,
            route_node_id.as_deref(),
        )?;
        let mongo_username = file
            .mongodb
            .username
            .or_else(|| std::env::var("AGENTDB_MONGODB_USERNAME").ok());
        let mongo_password = file
            .mongodb
            .password
            .or_else(|| std::env::var("AGENTDB_MONGODB_PASSWORD").ok());
        validate_mongo_auth(&mongo_username, &mongo_password)?;
        let internal_auth_token = file
            .server
            .internal_auth_token
            .or_else(|| std::env::var("AGENTDB_INTERNAL_AUTH_TOKEN").ok());
        let internal_auth_previous_token = file
            .server
            .internal_auth_previous_token
            .or_else(|| std::env::var("AGENTDB_INTERNAL_AUTH_PREVIOUS_TOKEN").ok());
        validate_internal_auth_token(deployment_mode, &internal_auth_token)?;
        validate_previous_internal_auth_token(&internal_auth_token, &internal_auth_previous_token)?;

        Ok(Self {
            deployment_mode,
            bind,
            data_dir,
            content_dir,
            kv_backend,
            content_backend,
            object_store,
            in_memory,
            route_node_id,
            route_shard_count: file
                .routing
                .shard_count
                .unwrap_or(agentdb_meta::shards::DEFAULT_SHARD_COUNT),
            route_region: file.routing.region,
            route_location: file.routing.location,
            route_refresh_interval_ms: file.routing.refresh_interval_ms.unwrap_or(5_000),
            route_proxy_timeout_ms: file.routing.proxy_timeout_ms.unwrap_or(5_000),
            route_proxy_max_response_bytes: file
                .routing
                .proxy_max_response_bytes
                .unwrap_or(16 * 1024 * 1024),
            mongo_bind: file.mongodb.bind,
            mongo_username,
            mongo_password,
            internal_auth_token,
            internal_auth_previous_token,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    deployment: DeploymentFileConfig,
    #[serde(default)]
    server: ServerFileConfig,
    #[serde(default)]
    storage: StorageFileConfig,
    #[serde(default)]
    routing: RoutingFileConfig,
    #[serde(default)]
    mongodb: MongoDbFileConfig,
}

#[derive(Debug, Default, Deserialize)]
struct DeploymentFileConfig {
    mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ServerFileConfig {
    bind: Option<SocketAddr>,
    dev: Option<bool>,
    internal_auth_token: Option<String>,
    internal_auth_previous_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct StorageFileConfig {
    data_dir: Option<PathBuf>,
    content_dir: Option<PathBuf>,
    #[serde(default)]
    kv: KvFileConfig,
    #[serde(default)]
    content: ContentFileConfig,
    object_store: Option<ObjectStoreFileConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct KvFileConfig {
    backend: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ContentFileConfig {
    backend: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ObjectStoreFileConfig {
    backend: Option<String>,
    local_root: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    allow_http: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct RoutingFileConfig {
    node_id: Option<String>,
    shard_count: Option<i64>,
    region: Option<String>,
    location: Option<String>,
    refresh_interval_ms: Option<u64>,
    proxy_timeout_ms: Option<u64>,
    proxy_max_response_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct MongoDbFileConfig {
    bind: Option<SocketAddr>,
    username: Option<String>,
    password: Option<String>,
}

fn validate_mongo_auth(username: &Option<String>, password: &Option<String>) -> anyhow::Result<()> {
    match (username, password) {
        (Some(username), Some(password)) if username.is_empty() || password.is_empty() => {
            anyhow::bail!("mongodb username/password must not be empty")
        }
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => anyhow::bail!(
            "mongodb username requires a password; set --mongo-password or AGENTDB_MONGODB_PASSWORD"
        ),
        (None, Some(_)) => anyhow::bail!(
            "mongodb password requires a username; set --mongo-username or AGENTDB_MONGODB_USERNAME"
        ),
    }
}

fn validate_internal_auth_token(
    deployment_mode: DeploymentMode,
    token: &Option<String>,
) -> anyhow::Result<()> {
    match (deployment_mode, token.as_deref()) {
        (_, Some(token)) if token.len() < 16 => {
            anyhow::bail!("server.internal_auth_token must be at least 16 characters")
        }
        (DeploymentMode::Cluster, None) => anyhow::bail!(
            "deployment.mode=cluster requires server.internal_auth_token or AGENTDB_INTERNAL_AUTH_TOKEN"
        ),
        _ => Ok(()),
    }
}

fn validate_previous_internal_auth_token(
    current: &Option<String>,
    previous: &Option<String>,
) -> anyhow::Result<()> {
    let Some(previous) = previous.as_deref() else {
        return Ok(());
    };
    if previous.len() < 16 {
        anyhow::bail!("server.internal_auth_previous_token must be at least 16 characters");
    }
    if current.as_deref() == Some(previous) {
        anyhow::bail!("server.internal_auth_previous_token must differ from current token");
    }
    Ok(())
}

fn resolve_kv_backend(cfg: &KvFileConfig) -> anyhow::Result<KvBackendKind> {
    match cfg.backend.as_deref().unwrap_or("rocksdb") {
        "rocksdb" => Ok(KvBackendKind::RocksDb),
        "slatedb" => Ok(KvBackendKind::SlateDb),
        other => anyhow::bail!("unknown storage.kv.backend: {other}"),
    }
}

fn resolve_deployment_mode(cfg: &DeploymentFileConfig) -> anyhow::Result<DeploymentMode> {
    match cfg.mode.as_deref().unwrap_or("single_node") {
        "single_node" => Ok(DeploymentMode::SingleNode),
        "cluster" => Ok(DeploymentMode::Cluster),
        other => anyhow::bail!("unknown deployment.mode: {other}"),
    }
}

fn resolve_content_backend(cfg: &ContentFileConfig) -> anyhow::Result<ContentBackendKind> {
    match cfg.backend.as_deref().unwrap_or("localfs") {
        "localfs" => Ok(ContentBackendKind::LocalFs),
        "object_store" => Ok(ContentBackendKind::ObjectStore),
        other => anyhow::bail!("unknown storage.content.backend: {other}"),
    }
}

fn resolve_object_store_config(
    cfg: Option<&ObjectStoreFileConfig>,
    default_local_root: PathBuf,
) -> anyhow::Result<Option<ResolvedObjectStoreConfig>> {
    let Some(cfg) = cfg else {
        return Ok(None);
    };
    let backend = match cfg.backend.as_deref().unwrap_or("localfs") {
        "localfs" => ObjectStoreBackendKind::LocalFs,
        "s3" => ObjectStoreBackendKind::S3,
        other => anyhow::bail!("unknown object store backend: {other}"),
    };
    Ok(Some(ResolvedObjectStoreConfig {
        backend,
        local_root: match backend {
            ObjectStoreBackendKind::LocalFs => {
                Some(cfg.local_root.clone().unwrap_or(default_local_root))
            }
            ObjectStoreBackendKind::S3 => None,
        },
        bucket: cfg.bucket.clone(),
        region: cfg.region.clone(),
        endpoint: cfg.endpoint.clone(),
        allow_http: cfg.allow_http.unwrap_or(false),
    }))
}

fn build_object_store(config: &ResolvedObjectStoreConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    match config.backend {
        ObjectStoreBackendKind::LocalFs => {
            let root = config
                .local_root
                .clone()
                .ok_or_else(|| anyhow::anyhow!("localfs object store requires local_root"))?;
            std::fs::create_dir_all(&root)?;
            Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?))
        }
        ObjectStoreBackendKind::S3 => {
            let bucket = config
                .bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("s3 object store requires bucket"))?;
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
            if let Some(region) = &config.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if config.allow_http {
                builder = builder.with_allow_http(true);
            }
            Ok(Arc::new(builder.build()?))
        }
    }
}

#[derive(Clone)]
struct RaftSnapshotObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl RaftSnapshotObjectStore {
    fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }

    async fn put_transfer(
        &self,
        transfer: &agentdb_storage::RaftRsSnapshotTransfer,
    ) -> anyhow::Result<String> {
        let path = self.path_for(
            transfer.tablet_id,
            transfer.from,
            transfer.to,
            transfer.index,
        );
        self.store
            .put(&path, transfer.payload.clone().into())
            .await
            .map_err(|error| anyhow::anyhow!("raft snapshot object upload failed: {error}"))?;
        Ok(format!("object://{path}"))
    }

    async fn get_transfer(
        &self,
        snapshot: &agentdb_meta::tablet_snapshots::TabletSnapshot,
    ) -> anyhow::Result<agentdb_storage::RaftRsSnapshotTransfer> {
        let uri = snapshot
            .storage_uri
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("retained snapshot has no storage_uri"))?;
        let Some(path) = uri.strip_prefix("object://") else {
            anyhow::bail!("unsupported raft snapshot storage URI: {uri}");
        };
        let result = self
            .store
            .get(&ObjectPath::from(path.to_string()))
            .await
            .map_err(|error| anyhow::anyhow!("raft snapshot object read failed: {error}"))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|error| anyhow::anyhow!("raft snapshot object bytes failed: {error}"))?
            .to_vec();
        if bytes.len() as i64 != snapshot.size_bytes {
            anyhow::bail!(
                "raft snapshot object size mismatch for {}: expected {}, got {}",
                snapshot.snapshot_id,
                snapshot.size_bytes,
                bytes.len()
            );
        }
        let sha256_hex = agentdb_storage::sha256_hex(&bytes);
        if sha256_hex != snapshot.sha256_hex {
            anyhow::bail!(
                "raft snapshot object checksum mismatch for {}",
                snapshot.snapshot_id
            );
        }
        Ok(agentdb_storage::RaftRsSnapshotTransfer {
            tablet_id: snapshot.tablet_id,
            from: agentdb_storage::stable_raft_node_id(&snapshot.source_node_id)?,
            to: agentdb_storage::stable_raft_node_id(&snapshot.target_node_id)?,
            index: u64::try_from(snapshot.snapshot_index)?,
            term: u64::try_from(snapshot.snapshot_term)?,
            payload: bytes,
        })
    }

    async fn delete_uri(&self, uri: &str) -> anyhow::Result<()> {
        let Some(path) = uri.strip_prefix("object://") else {
            anyhow::bail!("unsupported raft snapshot storage URI: {uri}");
        };
        let path = ObjectPath::from(path.to_string());
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(anyhow::anyhow!(
                "raft snapshot object delete failed for {uri}: {error}"
            )),
        }
    }

    fn path_for(&self, tablet_id: i64, from: u64, to: u64, index: u64) -> ObjectPath {
        let mut path = String::new();
        if !self.prefix.is_empty() {
            path.push_str(&self.prefix);
            path.push('/');
        }
        path.push_str(&format!(
            "tablet-{tablet_id}/from-{from}/to-{to}/idx-{index}.snapshot"
        ));
        ObjectPath::from(path)
    }
}

fn build_content_backend(
    config: &ResolvedServeConfig,
) -> anyhow::Result<(
    Arc<dyn agentdb_storage::ContentBackend>,
    Option<Arc<agentdb_storage::AsyncMirrorContentBackend>>,
)> {
    match config.content_backend {
        ContentBackendKind::LocalFs => {
            let content_dir = config.content_dir.clone();
            std::fs::create_dir_all(&content_dir)?;
            tracing::info!(path = %content_dir.display(), "opened LocalFs artifact content backend");
            Ok((
                Arc::new(agentdb_storage::LocalFsContentBackend::new(&content_dir)),
                None,
            ))
        }
        ContentBackendKind::ObjectStore
            if matches!(config.deployment_mode, DeploymentMode::Cluster) =>
        {
            let content_dir = config.content_dir.clone();
            std::fs::create_dir_all(&content_dir)?;
            let primary: Arc<dyn agentdb_storage::ContentBackend> =
                Arc::new(agentdb_storage::LocalFsContentBackend::new(&content_dir));
            let object_store = build_object_store(
                config
                    .object_store
                    .as_ref()
                    .expect("object-store content backend requires object store config"),
            )?;
            let mirror: Arc<dyn agentdb_storage::ContentBackend> =
                Arc::new(agentdb_storage::ObjectStoreContentBackend::new(
                    object_store,
                    OBJECT_STORE_CONTENT_PREFIX,
                ));
            let queue_capacity = env_usize(
                "AGENTDB_CONTENT_MIRROR_QUEUE_CAPACITY",
                DEFAULT_CONTENT_MIRROR_QUEUE_CAPACITY,
            )
            .max(1);
            let backend = Arc::new(agentdb_storage::AsyncMirrorContentBackend::new(
                primary,
                mirror,
                queue_capacity,
            ));
            tracing::info!(
                primary_path = %content_dir.display(),
                mirror_prefix = %OBJECT_STORE_CONTENT_PREFIX,
                queue_capacity,
                "opened async mirrored artifact content backend"
            );
            Ok((backend.clone(), Some(backend)))
        }
        ContentBackendKind::ObjectStore => {
            let object_store = build_object_store(
                config
                    .object_store
                    .as_ref()
                    .expect("object-store content backend requires object store config"),
            )?;
            tracing::info!(prefix = %OBJECT_STORE_CONTENT_PREFIX, "opened object-store artifact content backend");
            Ok((
                Arc::new(agentdb_storage::ObjectStoreContentBackend::new(
                    object_store,
                    OBJECT_STORE_CONTENT_PREFIX,
                )),
                None,
            ))
        }
    }
}

fn validate_storage_backend_config(
    deployment_mode: DeploymentMode,
    kv_backend: KvBackendKind,
    content_backend: ContentBackendKind,
    object_store: Option<&ResolvedObjectStoreConfig>,
    in_memory: bool,
    route_node_id: Option<&str>,
) -> anyhow::Result<()> {
    if matches!(kv_backend, KvBackendKind::SlateDb) && object_store.is_none() {
        anyhow::bail!("storage.kv.backend=slatedb requires storage.object_store");
    }
    if matches!(content_backend, ContentBackendKind::ObjectStore) && object_store.is_none() {
        anyhow::bail!("storage.content.backend=object_store requires storage.object_store");
    }
    if matches!(deployment_mode, DeploymentMode::Cluster) {
        if in_memory {
            anyhow::bail!("deployment.mode=cluster does not support in-memory/dev mode");
        }
        if !matches!(kv_backend, KvBackendKind::SlateDb) {
            anyhow::bail!("deployment.mode=cluster requires storage.kv.backend=slatedb");
        }
        if !matches!(content_backend, ContentBackendKind::ObjectStore) {
            anyhow::bail!("deployment.mode=cluster requires storage.content.backend=object_store");
        }
        let Some(object_store) = object_store else {
            anyhow::bail!("deployment.mode=cluster requires storage.object_store");
        };
        if !matches!(object_store.backend, ObjectStoreBackendKind::S3) {
            anyhow::bail!("deployment.mode=cluster requires storage.object_store.backend=s3");
        }
        if route_node_id.is_none() {
            anyhow::bail!("deployment.mode=cluster requires routing.node_id or --route-node-id");
        }
    }
    Ok(())
}

fn load_config(path: Option<&PathBuf>) -> anyhow::Result<FileConfig> {
    let Some(path) = resolve_config_path(path) else {
        return Ok(FileConfig::default());
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|error| anyhow::anyhow!("failed to read config {}: {error}", path.display()))?;
    toml::from_str(&text)
        .map_err(|error| anyhow::anyhow!("failed to parse config {}: {error}", path.display()))
}

fn resolve_config_path(path: Option<&PathBuf>) -> Option<PathBuf> {
    match path {
        Some(path) => Some(path.clone()),
        None => {
            let default = PathBuf::from(DEFAULT_CONFIG_PATH);
            default.exists().then_some(default)
        }
    }
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:7878"
        .parse()
        .expect("default bind address must parse")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn print_startup_banner(config: &ResolvedServeConfig) {
    let mode = if config.in_memory {
        "dev in-memory (state is lost on shutdown)".to_string()
    } else {
        format!(
            "persistent (kv: {}, content: {}, data: {}, artifacts: {})",
            describe_kv_backend(config),
            describe_content_backend(config),
            config.data_dir.display(),
            config.content_dir.display()
        )
    };
    println!("AgentDB listening on http://{}", config.bind);
    println!("  deployment: {}", describe_deployment_mode(config));
    println!("  mode: {mode}");
    if let Some(note) = deployment_support_note(config) {
        println!("  note: {note}");
    }
    println!("  health:    http://{}/health/ready", config.bind);
    println!("  dashboard: http://{}/dashboard/", config.bind);
    if let Some(mongo_bind) = config.mongo_bind {
        println!("  mongodb:   mongodb://{mongo_bind}/");
        println!(
            "  mongo auth: {}",
            if config.mongo_auth_enabled() {
                "PLAIN username/password"
            } else {
                "disabled"
            }
        );
    }
    println!(
        "  internal raft rpc: {}",
        if config.internal_auth_token.is_some() {
            "token auth enabled"
        } else {
            "disabled"
        }
    );
}

fn doctor(opts: DoctorOpts) -> anyhow::Result<()> {
    let config = ResolvedServeConfig::from_doctor_opts(opts)?;
    let mut failed = false;

    println!("AgentDB doctor");
    println!("  bind: {}", config.bind);
    println!("  deployment: {}", describe_deployment_mode(&config));
    if config.in_memory {
        println!("  mode: dev in-memory");
        println!("  storage: skipped");
    } else {
        println!(
            "  mode: persistent (kv: {}, content: {}, data: {}, artifacts: {})",
            describe_kv_backend(&config),
            describe_content_backend(&config),
            config.data_dir.display(),
            config.content_dir.display()
        );
        failed |= !check_writable_dir("data dir", &config.data_dir);
        match config.kv_backend {
            KvBackendKind::RocksDb => {
                failed |= !check_writable_dir("runtime dir", &config.data_dir.join("runtime"));
            }
            KvBackendKind::SlateDb => {
                failed |=
                    !check_object_store_config("shared object store", config.object_store.as_ref());
            }
        }
        match config.content_backend {
            ContentBackendKind::LocalFs => {
                failed |= !check_writable_dir("content dir", &config.content_dir);
            }
            ContentBackendKind::ObjectStore => {
                failed |=
                    !check_object_store_config("shared object store", config.object_store.as_ref());
            }
        }
    }

    match std::net::TcpListener::bind(config.bind) {
        Ok(listener) => {
            drop(listener);
            println!("  ok: bind address is available");
        }
        Err(error) => {
            failed = true;
            println!("  error: cannot bind {}: {error}", config.bind);
        }
    }

    if failed {
        anyhow::bail!("doctor found configuration problems");
    }
    if let Some(note) = deployment_support_note(&config) {
        println!("  note: {note}");
    }
    println!(
        "  ok: {} configuration looks usable",
        describe_deployment_mode(&config)
    );
    Ok(())
}

fn describe_deployment_mode(config: &ResolvedServeConfig) -> &'static str {
    match config.deployment_mode {
        DeploymentMode::SingleNode => "single_node",
        DeploymentMode::Cluster => "cluster",
    }
}

fn deployment_support_note(config: &ResolvedServeConfig) -> Option<&'static str> {
    match (
        config.deployment_mode,
        config.kv_backend,
        config.content_backend,
        config.object_store.as_ref().map(|cfg| cfg.backend),
    ) {
        (
            DeploymentMode::SingleNode,
            KvBackendKind::SlateDb,
            ContentBackendKind::ObjectStore,
            Some(ObjectStoreBackendKind::S3),
        ) => Some(
            "single-node SlateDB + S3 is intended for testing/evaluation, not the primary production deployment",
        ),
        (DeploymentMode::Cluster, ..) => Some(
            "cluster mode is forward-looking; operator APIs and automation are still incomplete",
        ),
        _ => None,
    }
}

fn describe_kv_backend(config: &ResolvedServeConfig) -> String {
    match config.kv_backend {
        KvBackendKind::RocksDb => "rocksdb".into(),
        KvBackendKind::SlateDb => {
            format!(
                "slatedb@{}",
                describe_object_store(config.object_store.as_ref())
            )
        }
    }
}

fn describe_content_backend(config: &ResolvedServeConfig) -> String {
    match config.content_backend {
        ContentBackendKind::LocalFs => "localfs".into(),
        ContentBackendKind::ObjectStore => {
            format!(
                "object_store@{}",
                describe_object_store(config.object_store.as_ref())
            )
        }
    }
}

fn describe_object_store(config: Option<&ResolvedObjectStoreConfig>) -> String {
    let Some(config) = config else {
        return "unconfigured".into();
    };
    match config.backend {
        ObjectStoreBackendKind::LocalFs => format!(
            "localfs:{}",
            config
                .local_root
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<missing>".into())
        ),
        ObjectStoreBackendKind::S3 => format!(
            "s3:{}",
            config.bucket.as_deref().unwrap_or("<missing-bucket>")
        ),
    }
}

fn check_object_store_config(label: &str, config: Option<&ResolvedObjectStoreConfig>) -> bool {
    let Some(config) = config else {
        println!("  error: {label} is not configured");
        return false;
    };
    match config.backend {
        ObjectStoreBackendKind::LocalFs => {
            let Some(root) = &config.local_root else {
                println!("  error: {label} localfs backend is missing local_root");
                return false;
            };
            check_writable_dir(label, root)
        }
        ObjectStoreBackendKind::S3 => {
            if config.bucket.as_deref().unwrap_or("").is_empty() {
                println!("  error: {label} s3 backend is missing bucket");
                return false;
            }
            println!(
                "  ok: {label} configured for s3 bucket {}",
                config.bucket.as_deref().unwrap_or("<missing-bucket>")
            );
            true
        }
    }
}

fn check_writable_dir(label: &str, path: &PathBuf) -> bool {
    if let Err(error) = std::fs::create_dir_all(path) {
        println!("  error: cannot create {label} {}: {error}", path.display());
        return false;
    }
    let probe = path.join(".agentdb-write-test");
    match std::fs::write(&probe, b"ok").and_then(|_| std::fs::remove_file(&probe)) {
        Ok(()) => {
            println!("  ok: {label} is writable ({})", path.display());
            true
        }
        Err(error) => {
            println!(
                "  error: {label} is not writable ({}): {error}",
                path.display()
            );
            false
        }
    }
}

fn init_config(opts: InitConfigOpts) -> anyhow::Result<()> {
    if opts.output.exists() && !opts.force {
        anyhow::bail!(
            "{} already exists; pass --force to overwrite it",
            opts.output.display()
        );
    }
    if let Some(parent) = opts
        .output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&opts.output, SAMPLE_CONFIG)?;
    println!("Wrote {}", opts.output.display());
    println!("Try:");
    println!("  agentdb doctor --config {}", opts.output.display());
    println!("  agentdb serve --config {}", opts.output.display());
    Ok(())
}

async fn load_route_snapshot(
    meta: &agentdb_meta::MetaStore,
    shard_count: i64,
    route_location: Option<String>,
) -> agentdb_meta::MetaResult<agentdb_api::routing_http::RouteSnapshot> {
    agentdb_api::routing_http::RouteSnapshot::load_from_meta(
        meta,
        shard_count,
        route_location,
        agentdb_meta::routing::ResidencyPolicy::default(),
    )
    .await
}

async fn prepare_route_startup(
    config: &ResolvedServeConfig,
    route_location: Option<String>,
    raft_registry: Option<Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>>,
) -> anyhow::Result<Option<RouteStartup>> {
    let Some(node_id) = config.route_node_id.as_deref() else {
        return Ok(None);
    };
    let meta = agentdb_meta::MetaStore::open_from_env().await?;
    let scheduler_status = agentdb_api::state::SchedulerStatus::new();
    let snapshot = load_route_snapshot(&meta, config.route_shard_count, route_location).await?;
    let routing = agentdb_api::routing_http::RouteRuntime::snapshot(node_id, snapshot);
    let raft_bootstrap = if let Some(registry) = raft_registry {
        let raft_engine_root = config.data_dir.join("raft-engine");
        Some(sync_raft_rs_registry(&meta, node_id, &raft_engine_root, registry).await?)
    } else {
        None
    };
    Ok(Some(RouteStartup {
        node_id: node_id.to_string(),
        meta,
        scheduler_status,
        routing,
        raft_bootstrap,
    }))
}

async fn load_raft_rs_node_configs(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    raft_engine_root: &std::path::Path,
) -> anyhow::Result<Vec<agentdb_storage::RaftRsNodeConfig>> {
    let replicas = agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), tablet_id).await?;
    let mut configs = Vec::new();
    for replica in replicas {
        if replica.role == "log_only" {
            continue;
        }
        configs.push(
            agentdb_storage::RaftRsNodeConfig::from_control_plane_replica(
                tablet_id,
                &replica.node_id,
                &replica.role,
                raft_engine_root,
            )?,
        );
    }
    if configs.is_empty() {
        anyhow::bail!("tablet {tablet_id} has no voter/learner replicas for raft-rs");
    }
    Ok(configs)
}

async fn sync_raft_rs_registry(
    meta: &agentdb_meta::MetaStore,
    local_node_id: &str,
    raft_engine_root: &std::path::Path,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
) -> anyhow::Result<RaftRsRegistryBootstrap> {
    let local_raft_node_id = agentdb_storage::stable_raft_node_id(local_node_id)?;
    let tablet_states: BTreeMap<i64, String> = agentdb_meta::tablets::list_all(meta.pool())
        .await?
        .into_iter()
        .map(|tablet| (tablet.tablet_id, tablet.state))
        .collect();
    let local_tablet_ids: BTreeSet<_> = agentdb_meta::tablet_replicas::list_all(meta.pool())
        .await?
        .into_iter()
        .filter(|replica| {
            replica.node_id == local_node_id && matches!(replica.role.as_str(), "voter" | "learner")
        })
        .filter(|replica| {
            tablet_states
                .get(&replica.tablet_id)
                .map_or(true, |state| state != "tombstoned")
        })
        .map(|replica| replica.tablet_id)
        .collect();
    let mut bootstrapped = Vec::new();
    let mut local_logs = BTreeMap::new();
    for tablet_id in registry.tablet_ids() {
        if !local_tablet_ids.contains(&tablet_id) {
            registry.unregister(tablet_id);
        }
    }
    let registered: BTreeSet<_> = registry.tablet_ids().into_iter().collect();
    for tablet_id in local_tablet_ids.iter().copied() {
        if registered.contains(&tablet_id) {
            continue;
        }
        let configs = load_raft_rs_node_configs(meta, tablet_id, raft_engine_root).await?;
        let (group, local_log) =
            agentdb_storage::RaftRsSharedGroup::open_local(tablet_id, configs, local_raft_node_id)?;
        registry.register(tablet_id, group)?;
        bootstrapped.push(tablet_id);
        local_logs.insert(tablet_id, local_log);
    }
    Ok(RaftRsRegistryBootstrap {
        tablet_ids: local_tablet_ids.into_iter().collect(),
        bootstrapped_tablet_ids: bootstrapped,
        local_logs,
    })
}

async fn sync_raft_rs_registry_and_runtime(
    meta: &agentdb_meta::MetaStore,
    local_node_id: &str,
    raft_engine_root: &std::path::Path,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
    runtime: Arc<agentdb_core::Runtime>,
) -> anyhow::Result<RaftRsRegistryBootstrap> {
    let bootstrap = sync_raft_rs_registry(meta, local_node_id, raft_engine_root, registry).await?;
    for (tablet_id, log) in &bootstrap.local_logs {
        let registered = runtime
            .register_replicated_tablet(
                *tablet_id,
                log.clone() as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
        if registered {
            tracing::info!(
                tablet_id,
                "registered runtime raft tablet after metadata sync"
            );
        }
    }
    let expected: BTreeSet<_> = bootstrap.tablet_ids.iter().copied().collect();
    for tablet_id in runtime.registered_tablet_ids() {
        if !expected.contains(&tablet_id) {
            if runtime
                .unregister_replicated_tablet(tablet_id)
                .map_err(|error| anyhow::anyhow!(error.message))?
            {
                tracing::info!(
                    tablet_id,
                    "unregistered runtime raft tablet after metadata sync"
                );
            }
        }
    }
    Ok(bootstrap)
}

async fn load_raft_rs_peer_urls(
    meta: &agentdb_meta::MetaStore,
    tablet_ids: impl IntoIterator<Item = i64>,
) -> anyhow::Result<BTreeMap<u64, String>> {
    let mut peer_urls = BTreeMap::new();
    for tablet_id in tablet_ids {
        let replicas =
            agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), tablet_id).await?;
        for replica in replicas {
            if !matches!(replica.role.as_str(), "voter" | "learner") {
                continue;
            }
            let node = agentdb_meta::nodes::get(meta.pool(), &replica.node_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "tablet {tablet_id} raft replica {} has no node metadata",
                        replica.node_id
                    )
                })?;
            if node.address.trim().is_empty() {
                anyhow::bail!(
                    "tablet {tablet_id} raft replica {} has an empty node address",
                    replica.node_id
                );
            }
            let raft_node_id = agentdb_storage::stable_raft_node_id(&replica.node_id)?;
            peer_urls.insert(raft_node_id, node.address);
        }
    }
    Ok(peer_urls)
}

async fn refresh_raft_rs_peer_urls(
    meta: &agentdb_meta::MetaStore,
    registry: &agentdb_api::raft_ingress::RaftRsGroupRegistry,
    outbox: &agentdb_api::internal_raft_http::RaftRsHttpOutbox,
) -> anyhow::Result<()> {
    let peer_urls = load_raft_rs_peer_urls(meta, registry.tablet_ids()).await?;
    outbox.replace_peer_urls(peer_urls);
    Ok(())
}

fn spawn_raft_rs_peer_url_refresher(
    meta: agentdb_meta::MetaStore,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
    outbox: agentdb_api::internal_raft_http::RaftRsHttpOutbox,
    refresh_interval_ms: u64,
) {
    let refresh_interval_ms = refresh_interval_ms.max(100);
    let mut changes = agentdb_meta::change_notifier::subscribe(
        &meta,
        &[
            agentdb_meta::change_notifier::Channel::Nodes,
            agentdb_meta::change_notifier::Channel::Tablets,
        ],
    );
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(refresh_interval_ms));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(error) = refresh_raft_rs_peer_urls(&meta, &registry, &outbox).await {
                        tracing::warn!(?error, "failed to refresh raft-rs peer URLs");
                    }
                }
                change = changes.recv() => {
                    match change {
                        Some(change) => {
                            tracing::debug!(channel = ?change.channel, payload = %change.payload, "raft-rs peer metadata change received");
                            if let Err(error) = refresh_raft_rs_peer_urls(&meta, &registry, &outbox).await {
                                tracing::warn!(?error, "failed to refresh raft-rs peer URLs");
                            }
                        }
                        None => {
                            tracing::warn!("raft-rs peer metadata change stream closed; continuing with periodic refresh");
                            changes = agentdb_meta::change_notifier::subscribe(
                                &meta,
                                &[
                                    agentdb_meta::change_notifier::Channel::Nodes,
                                    agentdb_meta::change_notifier::Channel::Tablets,
                                ],
                            );
                        }
                    }
                }
            }
        }
    });
}

fn spawn_raft_rs_message_outbox_worker(
    meta: agentdb_meta::MetaStore,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
    outbox: agentdb_api::internal_raft_http::RaftRsHttpOutbox,
    interval: Duration,
    max_messages_per_tick: usize,
) {
    let interval = interval.max(Duration::from_millis(100));
    let max_messages_per_tick = max_messages_per_tick.max(1);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(error) =
                run_raft_rs_message_outbox_tick(&meta, &registry, &outbox, max_messages_per_tick)
                    .await
            {
                tracing::warn!(?error, "failed to run raft-rs message outbox tick");
            }
        }
    });
}

async fn run_raft_rs_message_outbox_tick(
    meta: &agentdb_meta::MetaStore,
    registry: &agentdb_api::raft_ingress::RaftRsGroupRegistry,
    outbox: &agentdb_api::internal_raft_http::RaftRsHttpOutbox,
    max_messages_per_tick: usize,
) -> anyhow::Result<usize> {
    let mut sent = 0usize;
    for (tablet_id, group) in registry.groups_snapshot() {
        if sent >= max_messages_per_tick {
            break;
        }
        let remaining_budget = max_messages_per_tick - sent;
        match flush_raft_rs_group_messages_with_health(
            meta,
            tablet_id,
            &group,
            outbox,
            remaining_budget,
        )
        .await
        {
            Ok(group_sent) => sent += group_sent,
            Err(error) => {
                tracing::warn!(tablet_id, ?error, "failed to flush raft-rs group messages");
            }
        }
    }
    Ok(sent)
}

async fn flush_raft_rs_group_messages_with_health(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    group: &agentdb_storage::RaftRsSharedGroup,
    outbox: &agentdb_api::internal_raft_http::RaftRsHttpOutbox,
    max_messages: usize,
) -> anyhow::Result<usize> {
    let messages = group.take_wire_messages();
    let send_limit = messages.len().min(max_messages.max(1));
    if messages.len() > send_limit {
        group.restore_wire_messages(messages[send_limit..].to_vec())?;
    }
    let mut sent = 0usize;
    for (index, message) in messages.iter().take(send_limit).cloned().enumerate() {
        match outbox.send_message_to_peer(&message).await {
            Ok(()) => {
                set_raft_replica_health_by_raft_node_id(meta, tablet_id, message.to, "healthy")
                    .await?;
                sent += 1;
            }
            Err(error) => {
                set_raft_replica_health_by_raft_node_id(meta, tablet_id, message.to, "unreachable")
                    .await?;
                group.restore_wire_messages(messages[index..send_limit].to_vec())?;
                return Err(anyhow::anyhow!(error));
            }
        }
    }
    Ok(sent)
}

fn spawn_raft_rs_snapshot_worker(
    meta: agentdb_meta::MetaStore,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
    runtime: Arc<agentdb_core::Runtime>,
    outbox: agentdb_api::internal_raft_http::RaftRsHttpOutbox,
    interval: Duration,
    min_lag: u64,
    retention_ms: i64,
    snapshot_store: Option<Arc<RaftSnapshotObjectStore>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            interval.tick().await;
            if let Err(error) = run_raft_rs_snapshot_tick(
                &meta,
                &registry,
                runtime.clone(),
                &outbox,
                min_lag,
                retention_ms,
                snapshot_store.clone(),
            )
            .await
            {
                tracing::warn!(?error, "failed to run raft-rs snapshot tick");
            }
        }
    });
}

async fn run_raft_rs_snapshot_tick(
    meta: &agentdb_meta::MetaStore,
    registry: &agentdb_api::raft_ingress::RaftRsGroupRegistry,
    runtime: Arc<agentdb_core::Runtime>,
    outbox: &agentdb_api::internal_raft_http::RaftRsHttpOutbox,
    min_lag: u64,
    retention_ms: i64,
    snapshot_store: Option<Arc<RaftSnapshotObjectStore>>,
) -> anyhow::Result<usize> {
    let mut sent = 0usize;
    for (tablet_id, group) in registry.groups_snapshot() {
        let Some(local_status) = group.status().into_iter().next() else {
            continue;
        };
        let replicas =
            agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), tablet_id).await?;
        let source_node_id = raft_node_label(&replicas, local_status.node_id)
            .unwrap_or_else(|| local_status.node_id.to_string());
        let mut target_nodes = Vec::new();
        for replica in replicas {
            if !matches!(replica.role.as_str(), "voter" | "learner") {
                continue;
            }
            let to = agentdb_storage::stable_raft_node_id(&replica.node_id)?;
            if to == local_status.node_id {
                update_raft_replica_progress_health(
                    meta,
                    tablet_id,
                    &replica.node_id,
                    local_status.applied_index,
                    local_status.commit_index,
                    "healthy",
                )
                .await?;
                continue;
            }
            let peer_applied = replica.applied_index.max(0) as u64;
            let lag = local_status.applied_index.saturating_sub(peer_applied);
            if lag < min_lag && replica.health != "lagging" {
                continue;
            }
            if replica.health != "lagging" {
                agentdb_meta::tablet_replicas::set_health(
                    meta.pool(),
                    tablet_id,
                    &replica.node_id,
                    "lagging",
                    now_ms_i64(),
                )
                .await?;
            }
            target_nodes.push((to, replica.node_id));
        }
        if target_nodes.is_empty() {
            continue;
        }
        let application_snapshot = match runtime.export_tablet_snapshot_payload(tablet_id) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(
                    tablet_id,
                    message = %error.message,
                    "skipping raft-rs snapshot because runtime state export failed"
                );
                continue;
            }
        };
        let mut compact_through = 0u64;
        for (to, node_id) in target_nodes {
            let transfer = group.build_snapshot_transfer(
                local_status.node_id,
                to,
                application_snapshot.clone(),
            )?;
            if let Err(error) = outbox.send_snapshot_to_peer(&transfer).await {
                agentdb_meta::tablet_replicas::set_health(
                    meta.pool(),
                    tablet_id,
                    &node_id,
                    "unreachable",
                    now_ms_i64(),
                )
                .await?;
                tracing::warn!(
                    tablet_id,
                    from = local_status.node_id,
                    to,
                    ?error,
                    "failed to enqueue raft-rs snapshot to peer"
                );
                continue;
            }
            update_raft_replica_progress_health(
                meta,
                tablet_id,
                &node_id,
                transfer.index,
                transfer.index,
                "healthy",
            )
            .await?;
            let now_ms = now_ms_i64();
            let expires_at_ms = now_ms.checked_add(retention_ms);
            let storage_uri = match snapshot_store.as_ref() {
                Some(snapshot_store) => match snapshot_store.put_transfer(&transfer).await {
                    Ok(uri) => Some(uri),
                    Err(error) => {
                        tracing::warn!(
                            tablet_id,
                            from = transfer.from,
                            to = transfer.to,
                            index = transfer.index,
                            ?error,
                            "failed to upload retained raft snapshot object"
                        );
                        None
                    }
                },
                None => None,
            };
            agentdb_meta::tablet_snapshots::record(
                meta.pool(),
                &raft_snapshot_id(tablet_id, transfer.from, transfer.to, transfer.index),
                tablet_id,
                &source_node_id,
                transfer.from.min(i64::MAX as u64) as i64,
                &node_id,
                transfer.to.min(i64::MAX as u64) as i64,
                transfer.index.min(i64::MAX as u64) as i64,
                transfer.term.min(i64::MAX as u64) as i64,
                transfer.payload.len().min(i64::MAX as usize) as i64,
                &agentdb_storage::sha256_hex(&transfer.payload),
                storage_uri.as_deref(),
                "available",
                now_ms,
                expires_at_ms,
            )
            .await?;
            compact_through = compact_through.max(transfer.index);
            sent += 1;
        }
        if compact_through > 0 {
            match runtime.compact_tablet_log_through(tablet_id, compact_through) {
                Ok(compacted) if compacted > 0 => {
                    tracing::debug!(
                        tablet_id,
                        compacted,
                        compact_through,
                        "compacted runtime tablet log after snapshot send"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        tablet_id,
                        compact_through,
                        message = %error.message,
                        "failed to compact runtime tablet log after snapshot send"
                    );
                }
            }
        }
    }
    Ok(sent)
}

fn spawn_raft_snapshot_prune_worker(
    meta: agentdb_meta::MetaStore,
    snapshot_store: Arc<RaftSnapshotObjectStore>,
    interval: Duration,
    batch_size: i64,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            interval.tick().await;
            if let Err(error) =
                run_raft_snapshot_prune_tick(&meta, Some(snapshot_store.clone()), batch_size).await
            {
                tracing::warn!(?error, "failed to run raft snapshot prune tick");
            }
        }
    });
}

fn spawn_raft_snapshot_restore_worker(
    meta: agentdb_meta::MetaStore,
    local_node_id: String,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
    runtime: Arc<agentdb_core::Runtime>,
    snapshot_store: Arc<RaftSnapshotObjectStore>,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            interval.tick().await;
            if let Err(error) = run_raft_snapshot_restore_tick(
                &meta,
                &local_node_id,
                &registry,
                runtime.clone(),
                &snapshot_store,
            )
            .await
            {
                tracing::warn!(?error, "failed to run raft snapshot restore tick");
            }
        }
    });
}

async fn run_raft_snapshot_restore_tick(
    meta: &agentdb_meta::MetaStore,
    local_node_id: &str,
    registry: &agentdb_api::raft_ingress::RaftRsGroupRegistry,
    runtime: Arc<agentdb_core::Runtime>,
    snapshot_store: &RaftSnapshotObjectStore,
) -> anyhow::Result<usize> {
    let local_raft_node_id = agentdb_storage::stable_raft_node_id(local_node_id)?;
    let snapshots =
        agentdb_meta::tablet_snapshots::latest_available_for_target(meta.pool(), local_node_id)
            .await?;
    let mut installed = 0usize;
    for snapshot in snapshots {
        if snapshot.storage_uri.is_none() {
            continue;
        }
        if agentdb_storage::stable_raft_node_id(&snapshot.target_node_id)? != local_raft_node_id {
            tracing::warn!(
                snapshot_id = %snapshot.snapshot_id,
                target_node_id = %snapshot.target_node_id,
                target_raft_node_id = snapshot.target_raft_node_id,
                local_node_id,
                local_raft_node_id,
                "retained raft snapshot target does not match local node"
            );
            continue;
        }
        let statuses = match registry.status_for(snapshot.tablet_id) {
            Ok(statuses) => statuses,
            Err(error) => {
                tracing::debug!(
                    snapshot_id = %snapshot.snapshot_id,
                    tablet_id = snapshot.tablet_id,
                    ?error,
                    "skipping retained raft snapshot restore because local group is not registered"
                );
                continue;
            }
        };
        if statuses
            .iter()
            .find(|status| status.node_id == local_raft_node_id)
            .is_some_and(|status| status.applied_index >= snapshot.snapshot_index as u64)
        {
            continue;
        }
        let transfer = snapshot_store.get_transfer(&snapshot).await?;
        let application_snapshot_payload =
            registry.install_snapshot_transfer(local_raft_node_id, transfer)?;
        if !application_snapshot_payload.is_empty() {
            runtime
                .install_tablet_snapshot_payload_and_rehydrate(
                    snapshot.tablet_id,
                    &application_snapshot_payload,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        update_raft_replica_progress_health(
            meta,
            snapshot.tablet_id,
            local_node_id,
            snapshot.snapshot_index as u64,
            snapshot.snapshot_index as u64,
            "healthy",
        )
        .await?;
        installed += 1;
    }
    Ok(installed)
}

async fn run_raft_snapshot_prune_tick(
    meta: &agentdb_meta::MetaStore,
    snapshot_store: Option<Arc<RaftSnapshotObjectStore>>,
    batch_size: i64,
) -> anyhow::Result<usize> {
    let now_ms = now_ms_i64();
    let expired =
        agentdb_meta::tablet_snapshots::list_expired(meta.pool(), now_ms, batch_size).await?;
    let mut deleted = 0usize;
    for snapshot in expired {
        if let Some(uri) = snapshot.storage_uri.as_deref() {
            let Some(snapshot_store) = snapshot_store.as_ref() else {
                tracing::warn!(
                    snapshot_id = %snapshot.snapshot_id,
                    storage_uri = %uri,
                    "retained raft snapshot object is expired but no object store is configured"
                );
                continue;
            };
            snapshot_store.delete_uri(uri).await?;
        }
        if agentdb_meta::tablet_snapshots::mark_deleted(
            meta.pool(),
            &snapshot.snapshot_id,
            now_ms_i64(),
        )
        .await?
        {
            deleted += 1;
        }
    }
    Ok(deleted)
}

async fn update_raft_replica_progress_health(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    node_id: &str,
    applied_index: u64,
    match_index: u64,
    health: &str,
) -> anyhow::Result<()> {
    let now_ms = now_ms_i64();
    agentdb_meta::tablet_replicas::set_progress(
        meta.pool(),
        tablet_id,
        node_id,
        applied_index.min(i64::MAX as u64) as i64,
        match_index.min(i64::MAX as u64) as i64,
        now_ms,
    )
    .await?;
    agentdb_meta::tablet_replicas::set_health(meta.pool(), tablet_id, node_id, health, now_ms)
        .await?;
    Ok(())
}

fn raft_snapshot_id(tablet_id: i64, from: u64, to: u64, index: u64) -> String {
    format!("tablet-{tablet_id}-from-{from}-to-{to}-idx-{index}")
}

fn raft_node_label(
    replicas: &[agentdb_meta::tablet_replicas::TabletReplica],
    raft_node_id: u64,
) -> Option<String> {
    replicas.iter().find_map(|replica| {
        let candidate = agentdb_storage::stable_raft_node_id(&replica.node_id).ok()?;
        (candidate == raft_node_id).then(|| replica.node_id.clone())
    })
}

async fn set_raft_replica_health_by_raft_node_id(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    raft_node_id: u64,
    health: &str,
) -> anyhow::Result<bool> {
    let Some(node_id) = resolve_raft_replica_node_id(meta, tablet_id, raft_node_id).await? else {
        tracing::debug!(
            tablet_id,
            raft_node_id,
            health,
            "skipping raft replica health update; control-plane node not found"
        );
        return Ok(false);
    };
    let updated = agentdb_meta::tablet_replicas::set_health(
        meta.pool(),
        tablet_id,
        &node_id,
        health,
        now_ms_i64(),
    )
    .await?;
    Ok(updated)
}

async fn resolve_raft_replica_node_id(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    raft_node_id: u64,
) -> agentdb_meta::MetaResult<Option<String>> {
    for replica in agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), tablet_id).await? {
        let Ok(candidate) = agentdb_storage::stable_raft_node_id(&replica.node_id) else {
            continue;
        };
        if candidate == raft_node_id {
            return Ok(Some(replica.node_id));
        }
    }
    Ok(None)
}

fn spawn_raft_rs_registry_refresher(
    meta: agentdb_meta::MetaStore,
    local_node_id: String,
    raft_engine_root: PathBuf,
    registry: Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>,
    runtime: Arc<agentdb_core::Runtime>,
    refresh_interval_ms: u64,
) {
    let refresh_interval_ms = refresh_interval_ms.max(100);
    let mut changes = agentdb_meta::change_notifier::subscribe(
        &meta,
        &[agentdb_meta::change_notifier::Channel::Tablets],
    );
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(refresh_interval_ms));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(error) = sync_raft_rs_registry_and_runtime(&meta, &local_node_id, &raft_engine_root, registry.clone(), runtime.clone()).await {
                        tracing::warn!(?error, "failed to sync raft-rs registry");
                    }
                }
                change = changes.recv() => {
                    match change {
                        Some(change) => {
                            tracing::debug!(channel = ?change.channel, payload = %change.payload, "raft-rs replica metadata change received");
                            if let Err(error) = sync_raft_rs_registry_and_runtime(&meta, &local_node_id, &raft_engine_root, registry.clone(), runtime.clone()).await {
                                tracing::warn!(?error, "failed to sync raft-rs registry");
                            }
                        }
                        None => {
                            tracing::warn!("raft-rs registry metadata change stream closed; continuing with periodic refresh");
                            changes = agentdb_meta::change_notifier::subscribe(
                                &meta,
                                &[agentdb_meta::change_notifier::Channel::Tablets],
                            );
                        }
                    }
                }
            }
        }
    });
}

fn spawn_route_snapshot_refresher(
    meta: agentdb_meta::MetaStore,
    routing: agentdb_api::routing_http::RouteRuntime,
    shard_count: i64,
    route_location: Option<String>,
    refresh_interval_ms: u64,
) {
    let refresh_interval_ms = refresh_interval_ms.max(100);
    let mut changes = agentdb_meta::change_notifier::subscribe(
        &meta,
        &[
            agentdb_meta::change_notifier::Channel::Shards,
            agentdb_meta::change_notifier::Channel::Nodes,
            agentdb_meta::change_notifier::Channel::Tablets,
            agentdb_meta::change_notifier::Channel::SessionLocators,
        ],
    );
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(refresh_interval_ms));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    refresh_route_snapshot(&meta, &routing, shard_count, route_location.clone()).await;
                }
                change = changes.recv() => {
                    match change {
                        Some(change) => {
                            tracing::debug!(channel = ?change.channel, payload = %change.payload, "route metadata change received");
                            refresh_route_snapshot(&meta, &routing, shard_count, route_location.clone()).await;
                        }
                        None => {
                            tracing::warn!("route metadata change stream closed; continuing with periodic refresh");
                            changes = agentdb_meta::change_notifier::subscribe(
                                &meta,
                                &[
                                    agentdb_meta::change_notifier::Channel::Shards,
                                    agentdb_meta::change_notifier::Channel::Nodes,
                                    agentdb_meta::change_notifier::Channel::Tablets,
                                    agentdb_meta::change_notifier::Channel::SessionLocators,
                                ],
                            );
                        }
                    }
                }
            }
        }
    });
}

fn spawn_cluster_workers(
    meta: agentdb_meta::MetaStore,
    node_id: String,
    runtime: Arc<agentdb_core::Runtime>,
    movement_snapshot_client: Option<agentdb_api::internal_raft_http::RaftRsHttpClient>,
    raft_registry: Option<Arc<agentdb_api::raft_ingress::RaftRsGroupRegistry>>,
    node_address: String,
    region: Option<String>,
    scheduler_status: agentdb_api::state::SchedulerStatus,
    drain: agentdb_api::state::DrainState,
    config: ClusterWorkerConfig,
) {
    let heartbeat_meta = meta.clone();
    let heartbeat_node_id = node_id.clone();
    let heartbeat_node_address = node_address.clone();
    let heartbeat_region = region.clone();
    let heartbeat_runtime = runtime.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(config.heartbeat_interval_ms));
        loop {
            interval.tick().await;
            let now_ms = now_ms_i64();
            let state = if drain.is_draining() {
                "draining"
            } else {
                "alive"
            };
            if let Err(error) = heartbeat_node_once(
                &heartbeat_meta,
                &heartbeat_node_id,
                &heartbeat_node_address,
                heartbeat_region.as_deref(),
                state,
                now_ms,
            )
            .await
            {
                tracing::warn!(node_id = %heartbeat_node_id, error = %error, "node heartbeat failed");
            }
            match run_local_tablet_stats_tick(
                &heartbeat_meta,
                &heartbeat_runtime,
                &heartbeat_node_id,
                now_ms,
            )
            .await
            {
                Ok(published_count) if published_count > 0 => {
                    tracing::debug!(
                        node_id = %heartbeat_node_id,
                        published_count,
                        "published local tablet storage stats"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        node_id = %heartbeat_node_id,
                        error = %error,
                        "local tablet storage stats publication failed"
                    );
                }
            }
        }
    });

    let movement_meta = meta.clone();
    let movement_node_id = node_id.clone();
    let movement_runtime = runtime.clone();
    let movement_snapshot_client = movement_snapshot_client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(
            config.tablet_movement_executor_interval_ms,
        ));
        loop {
            interval.tick().await;
            let now_ms = now_ms_i64();
            match run_local_tablet_movement_executor_tick(
                &movement_meta,
                &movement_runtime,
                movement_snapshot_client.as_ref(),
                &movement_node_id,
                now_ms,
                config.tablet_movement_batch_size,
            )
            .await
            {
                Ok(executed_count) if executed_count > 0 => {
                    tracing::info!(
                        node_id = %movement_node_id,
                        executed_count,
                        "local tablet movement executor copied runtime snapshots"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        node_id = %movement_node_id,
                        error = %error,
                        "local tablet movement executor failed"
                    );
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(
            config
                .scheduler_renew_interval_ms
                .max(config.reaper_tick_ms),
        ));
        loop {
            interval.tick().await;
            let now_ms = now_ms_i64();
            if let Err(error) = run_cluster_scheduler_tick(
                &meta,
                &node_id,
                now_ms,
                config,
                &scheduler_status,
                raft_registry.as_deref(),
            )
            .await
            {
                tracing::warn!(node_id = %node_id, error = %error, "cluster scheduler tick failed");
            }
        }
    });
}

async fn heartbeat_node_once(
    meta: &agentdb_meta::MetaStore,
    node_id: &str,
    node_address: &str,
    region: Option<&str>,
    state: &str,
    now_ms: i64,
) -> agentdb_meta::MetaResult<()> {
    agentdb_meta::nodes::upsert(
        meta.pool(),
        node_id,
        node_address,
        None,
        state,
        now_ms,
        None,
        region,
    )
    .await
}

async fn run_local_tablet_stats_tick(
    meta: &agentdb_meta::MetaStore,
    runtime: &Arc<agentdb_core::Runtime>,
    node_id: &str,
    now_ms: i64,
) -> anyhow::Result<usize> {
    let stats = runtime
        .local_tablet_storage_stats()
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    for stats in &stats {
        agentdb_meta::tablet_stats::upsert(
            meta.pool(),
            stats.tablet_id,
            node_id,
            u64_to_i64_saturating(stats.key_count),
            u64_to_i64_saturating(stats.key_bytes),
            u64_to_i64_saturating(stats.value_bytes),
            u64_to_i64_saturating(stats.storage_bytes),
            u64_to_i64_saturating(stats.applied_index),
            now_ms,
        )
        .await?;
    }
    Ok(stats.len())
}

async fn run_cluster_scheduler_tick(
    meta: &agentdb_meta::MetaStore,
    node_id: &str,
    now_ms: i64,
    config: ClusterWorkerConfig,
    status: &agentdb_api::state::SchedulerStatus,
    raft_registry: Option<&agentdb_api::raft_ingress::RaftRsGroupRegistry>,
) -> agentdb_meta::MetaResult<()> {
    status.set_last_tick_at_ms(now_ms);
    let pool = meta.pool();
    let leader_row = match agentdb_meta::scheduler::get(pool).await? {
        Some(current) if current.node_id == node_id && current.is_live(now_ms) => {
            agentdb_meta::scheduler::renew(
                pool,
                node_id,
                current.epoch,
                now_ms + config.scheduler_lease_ms,
            )
            .await?
        }
        _ => match agentdb_meta::scheduler::try_acquire(
            pool,
            node_id,
            config.scheduler_lease_ms,
            now_ms,
        )
        .await?
        {
            Some(row) => row,
            None => {
                if let Some(current) = agentdb_meta::scheduler::get(pool).await? {
                    status.update_leader(
                        &current.node_id,
                        current.epoch,
                        current.expires_at_ms,
                        current.node_id == node_id && current.is_live(now_ms),
                    );
                } else {
                    status.set_not_leader();
                }
                return Ok(());
            }
        },
    };

    let is_leader = leader_row.node_id == node_id && leader_row.is_live(now_ms);
    status.update_leader(
        &leader_row.node_id,
        leader_row.epoch,
        leader_row.expires_at_ms,
        is_leader,
    );
    if !is_leader {
        return Ok(());
    }

    let dead_nodes =
        agentdb_meta::nodes::mark_dead_if_stale(pool, now_ms - config.stale_node_threshold_ms)
            .await?;
    if !dead_nodes.is_empty() {
        status.add_dead_nodes_marked(dead_nodes.len() as u64);
        tracing::info!(node_id = %node_id, dead_nodes = ?dead_nodes, "scheduler marked stale nodes dead");
    }
    let deleted_stale_tablet_stats = agentdb_meta::tablet_stats::delete_stale(
        pool,
        now_ms.saturating_sub(config.stale_node_threshold_ms).max(0),
    )
    .await?;
    if deleted_stale_tablet_stats > 0 {
        tracing::debug!(
            node_id = %node_id,
            deleted_stale_tablet_stats,
            "scheduler deleted stale tablet storage stats"
        );
    }

    let cleared = agentdb_meta::tablets::clear_expired_ownership(
        pool,
        config.reaper_safety_margin_ms,
        now_ms,
    )
    .await?;
    if !cleared.is_empty() {
        status.add_tablet_reassignments(cleared.len() as u64);
        tracing::info!(node_id = %node_id, tablets = ?cleared, "scheduler cleared expired tablet ownership");
    }

    if config.rebalance_max_movements > 0 {
        let report = agentdb_meta::tablet_rebalance::run(
            pool,
            agentdb_meta::tablet_rebalance::RebalanceRequest {
                dry_run: false,
                stale_threshold_ms: config.stale_node_threshold_ms,
                max_movements: config.rebalance_max_movements,
            },
            now_ms,
        )
        .await?;
        let changed_count = report.changed_count();
        if let Some(registry) = raft_registry {
            let membership_changes = if changed_count > 0 {
                apply_scheduler_rebalance_raft_membership_changes(meta, registry, &report.actions)
                    .await?
            } else {
                0
            };
            let retired_draining_sources =
                retire_draining_rebalance_raft_replicas(meta, registry).await?;
            if membership_changes > 0 || retired_draining_sources > 0 {
                tracing::info!(
                    node_id = %node_id,
                    membership_changes,
                    retired_draining_sources,
                    "scheduler converged raft membership for rebalance"
                );
            }
        }
        if changed_count > 0 {
            status.add_tablet_reassignments(changed_count as u64);
            tracing::info!(
                node_id = %node_id,
                changed_count,
                actions = ?report.actions,
                "scheduler applied tablet rebalance"
            );
        }
    }
    if config.tablet_movement_batch_size > 0 {
        let lifecycle_report = run_tablet_lifecycle_policy_tick(meta, now_ms, &config).await?;
        if lifecycle_report.prepared_split_count > 0 || lifecycle_report.prepared_merge_count > 0 {
            tracing::info!(
                node_id = %node_id,
                prepared_split_count = lifecycle_report.prepared_split_count,
                prepared_merge_count = lifecycle_report.prepared_merge_count,
                skipped_count = lifecycle_report.skipped_count,
                "scheduler prepared tablet lifecycle movements"
            );
        }
        let completed_count =
            run_tablet_movement_tick(meta, now_ms, config.tablet_movement_batch_size).await?;
        if completed_count > 0 {
            tracing::info!(
                node_id = %node_id,
                completed_count,
                "scheduler completed ready tablet movement rows"
            );
        }
        let cutover_count = complete_ready_tablet_movement_cutovers(
            meta,
            now_ms,
            config.tablet_movement_batch_size,
        )
        .await?;
        if cutover_count > 0 {
            tracing::info!(
                node_id = %node_id,
                cutover_count,
                "scheduler completed ready tablet movement cutovers"
            );
        }
        let deleted_tombstoned_replicas = cleanup_tombstoned_tablet_replicas(meta).await?;
        if deleted_tombstoned_replicas > 0 {
            tracing::info!(
                node_id = %node_id,
                deleted_tombstoned_replicas,
                "scheduler cleaned up tombstoned tablet replicas"
            );
        }
    }
    Ok(())
}

async fn apply_scheduler_rebalance_raft_membership_changes(
    meta: &agentdb_meta::MetaStore,
    registry: &agentdb_api::raft_ingress::RaftRsGroupRegistry,
    actions: &[agentdb_meta::tablet_rebalance::RebalanceAction],
) -> agentdb_meta::MetaResult<usize> {
    let mut changed = 0usize;
    for action in actions {
        if action.action != "complete_replacement" || !action.changed {
            continue;
        }
        let Some(target_node_id) = action.target_node_id.as_deref() else {
            continue;
        };
        let statuses = match registry.status_for(action.tablet_id) {
            Ok(statuses) => statuses,
            Err(error) => {
                tracing::debug!(
                    tablet_id = action.tablet_id,
                    error = %error,
                    "skipping scheduler raft membership change; local group is not registered"
                );
                continue;
            }
        };
        let Some(leader_id) = raft_leader_id_from_statuses(&statuses) else {
            tracing::warn!(
                tablet_id = action.tablet_id,
                "skipping scheduler raft membership change; no known leader"
            );
            continue;
        };
        let target_raft_node_id = match agentdb_storage::stable_raft_node_id(target_node_id) {
            Ok(node_id) => node_id,
            Err(error) => {
                tracing::warn!(
                    tablet_id = action.tablet_id,
                    target_node_id,
                    error = %error,
                    "skipping scheduler raft membership change; target node id is invalid"
                );
                continue;
            }
        };
        let source_raft_node_id = match action
            .source_node_id
            .as_deref()
            .map(agentdb_storage::stable_raft_node_id)
            .transpose()
        {
            Ok(node_id) => node_id,
            Err(error) => {
                tracing::warn!(
                    tablet_id = action.tablet_id,
                    source_node_id = action.source_node_id.as_deref(),
                    error = %error,
                    "skipping scheduler raft source retirement; source node id is invalid"
                );
                None
            }
        };
        match registry.promote_to_voter(action.tablet_id, leader_id, target_raft_node_id) {
            Ok(()) => {
                changed += 1;
                if let Some(source_raft_node_id) = source_raft_node_id {
                    match registry.remove_node(action.tablet_id, leader_id, source_raft_node_id) {
                        Ok(()) => {
                            if let Some(source_node_id) = action.source_node_id.as_deref() {
                                let deleted = agentdb_meta::tablet_replicas::delete(
                                    meta.pool(),
                                    action.tablet_id,
                                    source_node_id,
                                )
                                .await?;
                                if deleted {
                                    changed += 1;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                tablet_id = action.tablet_id,
                                source_node_id = action.source_node_id.as_deref(),
                                error = %error,
                                "scheduler failed to retire replacement source from raft"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    tablet_id = action.tablet_id,
                    target_node_id,
                    error = %error,
                    "scheduler failed to promote replacement target in raft"
                );
            }
        }
    }
    Ok(changed)
}

async fn retire_draining_rebalance_raft_replicas(
    meta: &agentdb_meta::MetaStore,
    registry: &agentdb_api::raft_ingress::RaftRsGroupRegistry,
) -> agentdb_meta::MetaResult<usize> {
    let tablets: BTreeMap<i64, agentdb_meta::tablets::Tablet> =
        agentdb_meta::tablets::list_all(meta.pool())
            .await?
            .into_iter()
            .map(|tablet| (tablet.tablet_id, tablet))
            .collect();
    let replicas = agentdb_meta::tablet_replicas::list_all(meta.pool()).await?;
    let mut retired = 0usize;
    for replica in replicas {
        if replica.role == "log_only" || replica.health != "draining" {
            continue;
        }
        if tablets
            .get(&replica.tablet_id)
            .and_then(|tablet| tablet.owner_node_id.as_deref())
            == Some(replica.node_id.as_str())
        {
            continue;
        }
        let statuses = match registry.status_for(replica.tablet_id) {
            Ok(statuses) => statuses,
            Err(error) => {
                tracing::debug!(
                    tablet_id = replica.tablet_id,
                    node_id = %replica.node_id,
                    error = %error,
                    "skipping draining raft replica retirement; local group is not registered"
                );
                continue;
            }
        };
        let Some(leader_id) = raft_leader_id_from_statuses(&statuses) else {
            tracing::warn!(
                tablet_id = replica.tablet_id,
                node_id = %replica.node_id,
                "skipping draining raft replica retirement; no known leader"
            );
            continue;
        };
        let raft_node_id = match agentdb_storage::stable_raft_node_id(&replica.node_id) {
            Ok(node_id) => node_id,
            Err(error) => {
                tracing::warn!(
                    tablet_id = replica.tablet_id,
                    node_id = %replica.node_id,
                    error = %error,
                    "skipping draining raft replica retirement; node id is invalid"
                );
                continue;
            }
        };
        match registry.remove_node(replica.tablet_id, leader_id, raft_node_id) {
            Ok(()) => {
                if agentdb_meta::tablet_replicas::delete(
                    meta.pool(),
                    replica.tablet_id,
                    &replica.node_id,
                )
                .await?
                {
                    retired += 1;
                }
            }
            Err(error) => {
                tracing::warn!(
                    tablet_id = replica.tablet_id,
                    node_id = %replica.node_id,
                    error = %error,
                    "failed to retire draining raft replica"
                );
            }
        }
    }
    Ok(retired)
}

fn raft_leader_id_from_statuses(statuses: &[agentdb_storage::RaftRsReplicaStatus]) -> Option<u64> {
    statuses
        .iter()
        .find(|status| status.role == "Leader")
        .map(|status| status.node_id)
        .or_else(|| {
            statuses
                .iter()
                .find(|status| status.leader_id != 0)
                .map(|status| status.leader_id)
        })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TabletLifecyclePolicyReport {
    prepared_split_count: usize,
    prepared_merge_count: usize,
    skipped_count: usize,
}

async fn run_tablet_lifecycle_policy_tick(
    meta: &agentdb_meta::MetaStore,
    now_ms: i64,
    config: &ClusterWorkerConfig,
) -> agentdb_meta::MetaResult<TabletLifecyclePolicyReport> {
    if config.tablet_lifecycle_split_max_shards == 0
        && config.tablet_lifecycle_merge_max_shards == 0
        && config.tablet_lifecycle_split_max_keys == 0
        && config.tablet_lifecycle_merge_max_keys == 0
        && config.tablet_lifecycle_split_max_storage_bytes == 0
        && config.tablet_lifecycle_merge_max_storage_bytes == 0
    {
        return Ok(TabletLifecyclePolicyReport::default());
    }

    let tablets = agentdb_meta::tablets::list_all(meta.pool()).await?;
    let ranges = agentdb_meta::tablet_ranges::list_all(meta.pool()).await?;
    let stats_by_tablet: BTreeMap<_, _> = agentdb_meta::tablet_stats::list_aggregates(meta.pool())
        .await?
        .into_iter()
        .map(|stats| (stats.tablet_id, stats))
        .collect();
    let open_movements =
        agentdb_meta::tablet_movements::list_open(meta.pool(), config.tablet_movement_batch_size)
            .await?;
    let mut busy_tablet_ids = BTreeSet::new();
    for movement in open_movements {
        busy_tablet_ids.insert(movement.source_tablet_id);
        busy_tablet_ids.insert(movement.destination_tablet_id);
    }

    let tablet_by_id: BTreeMap<_, _> = tablets
        .iter()
        .map(|tablet| (tablet.tablet_id, tablet.clone()))
        .collect();
    let mut next_tablet_id = tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1);
    let mut report = TabletLifecyclePolicyReport::default();

    if lifecycle_split_enabled(config) {
        let mut split_candidates: Vec<_> = tablets
            .iter()
            .filter(|tablet| tablet.state == "serving")
            .filter(|tablet| !busy_tablet_ids.contains(&tablet.tablet_id))
            .cloned()
            .collect();
        split_candidates.sort_by_key(|tablet| tablet.tablet_id);
        for tablet in split_candidates {
            if report.prepared_split_count + report.prepared_merge_count
                >= config.tablet_lifecycle_max_actions
            {
                break;
            }
            let scoped_ranges: Vec<_> = ranges
                .iter()
                .filter(|range| {
                    range.tablet_id == tablet.tablet_id
                        && range.placement_scope_id == tablet.placement_scope_id
                })
                .cloned()
                .collect();
            let Some((start_shard, end_shard)) = contiguous_range_span(&scoped_ranges) else {
                report.skipped_count += 1;
                continue;
            };
            let shard_count = shard_count(start_shard, end_shard);
            let stats = stats_by_tablet.get(&tablet.tablet_id);
            if !should_split_tablet(config, shard_count, stats) || shard_count < 2 {
                continue;
            }
            if lifecycle_source_applied_index(meta, &[tablet.tablet_id])
                .await?
                .is_none()
            {
                report.skipped_count += 1;
                continue;
            }
            let left_id = next_tablet_id;
            let right_id = next_tablet_id.saturating_add(1);
            next_tablet_id = next_tablet_id.saturating_add(2);
            let split_at = start_shard + (end_shard - start_shard) / 2;
            let child_ranges = [
                agentdb_meta::tablet_ranges::ReplacementRange {
                    start_shard,
                    end_shard: split_at,
                    tablet_id: left_id,
                },
                agentdb_meta::tablet_ranges::ReplacementRange {
                    start_shard: split_at + 1,
                    end_shard,
                    tablet_id: right_id,
                },
            ];
            prepare_lifecycle_split(meta, &tablet, &child_ranges, now_ms).await?;
            busy_tablet_ids.insert(tablet.tablet_id);
            busy_tablet_ids.insert(left_id);
            busy_tablet_ids.insert(right_id);
            report.prepared_split_count += 1;
        }
    }

    if lifecycle_merge_enabled(config)
        && report.prepared_split_count + report.prepared_merge_count
            < config.tablet_lifecycle_max_actions
    {
        let mut merge_ranges = ranges.clone();
        merge_ranges.sort_by_key(|range| {
            (
                range.placement_scope_id.clone(),
                range.start_shard,
                range.end_shard,
                range.tablet_id,
            )
        });
        for pair in merge_ranges.windows(2) {
            if report.prepared_split_count + report.prepared_merge_count
                >= config.tablet_lifecycle_max_actions
            {
                break;
            }
            let left = &pair[0];
            let right = &pair[1];
            if left.placement_scope_id != right.placement_scope_id
                || left.end_shard + 1 != right.start_shard
                || left.tablet_id == right.tablet_id
                || busy_tablet_ids.contains(&left.tablet_id)
                || busy_tablet_ids.contains(&right.tablet_id)
            {
                continue;
            }
            if !should_merge_tablets(
                config,
                shard_count(left.start_shard, left.end_shard),
                stats_by_tablet.get(&left.tablet_id),
                shard_count(right.start_shard, right.end_shard),
                stats_by_tablet.get(&right.tablet_id),
            ) {
                continue;
            }
            let Some(left_tablet) = tablet_by_id.get(&left.tablet_id) else {
                report.skipped_count += 1;
                continue;
            };
            let Some(right_tablet) = tablet_by_id.get(&right.tablet_id) else {
                report.skipped_count += 1;
                continue;
            };
            if left_tablet.state != "serving"
                || right_tablet.state != "serving"
                || left_tablet.placement_scope_id != right_tablet.placement_scope_id
                || left_tablet.encryption_policy_version != right_tablet.encryption_policy_version
                || left_tablet.placement_scope_id != left.placement_scope_id
            {
                report.skipped_count += 1;
                continue;
            }
            if lifecycle_source_applied_index(meta, &[left.tablet_id, right.tablet_id])
                .await?
                .is_none()
            {
                report.skipped_count += 1;
                continue;
            }
            let destination_tablet_id = next_tablet_id;
            next_tablet_id = next_tablet_id.saturating_add(1);
            prepare_lifecycle_merge(
                meta,
                left_tablet,
                right_tablet,
                destination_tablet_id,
                now_ms,
            )
            .await?;
            busy_tablet_ids.insert(left.tablet_id);
            busy_tablet_ids.insert(right.tablet_id);
            busy_tablet_ids.insert(destination_tablet_id);
            report.prepared_merge_count += 1;
        }
    }

    Ok(report)
}

fn lifecycle_split_enabled(config: &ClusterWorkerConfig) -> bool {
    config.tablet_lifecycle_split_max_shards > 0
        || config.tablet_lifecycle_split_max_keys > 0
        || config.tablet_lifecycle_split_max_storage_bytes > 0
}

fn lifecycle_merge_enabled(config: &ClusterWorkerConfig) -> bool {
    config.tablet_lifecycle_merge_max_shards > 0
        || config.tablet_lifecycle_merge_max_keys > 0
        || config.tablet_lifecycle_merge_max_storage_bytes > 0
}

fn should_split_tablet(
    config: &ClusterWorkerConfig,
    shard_count: i64,
    stats: Option<&agentdb_meta::tablet_stats::TabletStatsAggregate>,
) -> bool {
    (config.tablet_lifecycle_split_max_shards > 0
        && shard_count > config.tablet_lifecycle_split_max_shards)
        || stats.is_some_and(|stats| {
            (config.tablet_lifecycle_split_max_keys > 0
                && stats.key_count > config.tablet_lifecycle_split_max_keys)
                || (config.tablet_lifecycle_split_max_storage_bytes > 0
                    && stats.storage_bytes > config.tablet_lifecycle_split_max_storage_bytes)
        })
}

fn should_merge_tablets(
    config: &ClusterWorkerConfig,
    left_shard_count: i64,
    left_stats: Option<&agentdb_meta::tablet_stats::TabletStatsAggregate>,
    right_shard_count: i64,
    right_stats: Option<&agentdb_meta::tablet_stats::TabletStatsAggregate>,
) -> bool {
    tablet_within_merge_limits(config, left_shard_count, left_stats)
        && tablet_within_merge_limits(config, right_shard_count, right_stats)
}

fn tablet_within_merge_limits(
    config: &ClusterWorkerConfig,
    shard_count: i64,
    stats: Option<&agentdb_meta::tablet_stats::TabletStatsAggregate>,
) -> bool {
    if config.tablet_lifecycle_merge_max_shards > 0
        && shard_count > config.tablet_lifecycle_merge_max_shards
    {
        return false;
    }
    if config.tablet_lifecycle_merge_max_keys > 0
        && !stats.is_some_and(|stats| stats.key_count <= config.tablet_lifecycle_merge_max_keys)
    {
        return false;
    }
    if config.tablet_lifecycle_merge_max_storage_bytes > 0
        && !stats.is_some_and(|stats| {
            stats.storage_bytes <= config.tablet_lifecycle_merge_max_storage_bytes
        })
    {
        return false;
    }
    true
}

async fn prepare_lifecycle_split(
    meta: &agentdb_meta::MetaStore,
    source: &agentdb_meta::tablets::Tablet,
    child_ranges: &[agentdb_meta::tablet_ranges::ReplacementRange; 2],
    now_ms: i64,
) -> agentdb_meta::MetaResult<()> {
    let required_applied_index = lifecycle_source_applied_index(meta, &[source.tablet_id])
        .await?
        .unwrap_or(0);
    for child in child_ranges {
        let raft_group_id = format!("raft/{}", child.tablet_id);
        agentdb_meta::tablets::create_with_placement(
            meta.pool(),
            child.tablet_id,
            &source.placement_scope_id,
            "creating",
            Some(&raft_group_id),
            source.encryption_policy_version,
            now_ms,
        )
        .await?;
    }
    seed_lifecycle_destination_replicas(
        meta,
        &[source.tablet_id],
        &child_ranges
            .iter()
            .map(|range| range.tablet_id)
            .collect::<Vec<_>>(),
        now_ms,
    )
    .await?;
    agentdb_meta::tablets::set_state(meta.pool(), source.tablet_id, "splitting").await?;
    for child in child_ranges {
        let movement_id = format!("split:{}:{}", source.tablet_id, child.tablet_id);
        agentdb_meta::tablet_movements::upsert_pending(
            meta.pool(),
            &movement_id,
            "split",
            source.tablet_id,
            child.tablet_id,
            &source.placement_scope_id,
            child.start_shard,
            child.end_shard,
            required_applied_index,
            now_ms,
        )
        .await?;
    }
    Ok(())
}

async fn prepare_lifecycle_merge(
    meta: &agentdb_meta::MetaStore,
    left: &agentdb_meta::tablets::Tablet,
    right: &agentdb_meta::tablets::Tablet,
    destination_tablet_id: i64,
    now_ms: i64,
) -> agentdb_meta::MetaResult<()> {
    let source_ids = [left.tablet_id, right.tablet_id];
    let required_applied_index = lifecycle_source_applied_index(meta, &source_ids)
        .await?
        .unwrap_or(0);
    let raft_group_id = format!("raft/{destination_tablet_id}");
    agentdb_meta::tablets::create_with_placement(
        meta.pool(),
        destination_tablet_id,
        &left.placement_scope_id,
        "creating",
        Some(&raft_group_id),
        left.encryption_policy_version,
        now_ms,
    )
    .await?;
    seed_lifecycle_destination_replicas(meta, &source_ids, &[destination_tablet_id], now_ms)
        .await?;
    for source_id in source_ids {
        agentdb_meta::tablets::set_state(meta.pool(), source_id, "merging").await?;
        let ranges = scoped_tablet_ranges(meta, source_id, &left.placement_scope_id).await?;
        let Some((start_shard, end_shard)) = contiguous_range_span(&ranges) else {
            return Err(agentdb_meta::MetaError::InvalidInput(format!(
                "tablet {source_id} ranges are not contiguous for merge"
            )));
        };
        let movement_id = format!("merge:{source_id}:{destination_tablet_id}");
        agentdb_meta::tablet_movements::upsert_pending(
            meta.pool(),
            &movement_id,
            "merge",
            source_id,
            destination_tablet_id,
            &left.placement_scope_id,
            start_shard,
            end_shard,
            required_applied_index,
            now_ms,
        )
        .await?;
    }
    Ok(())
}

async fn lifecycle_source_applied_index(
    meta: &agentdb_meta::MetaStore,
    source_tablet_ids: &[i64],
) -> agentdb_meta::MetaResult<Option<i64>> {
    let mut max_index = 0;
    let mut found = false;
    for tablet_id in source_tablet_ids {
        for replica in
            agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), *tablet_id).await?
        {
            if replica.role == "log_only" {
                continue;
            }
            found = true;
            max_index = max_index.max(replica.applied_index);
        }
    }
    Ok(found.then_some(max_index))
}

async fn seed_lifecycle_destination_replicas(
    meta: &agentdb_meta::MetaStore,
    source_tablet_ids: &[i64],
    destination_tablet_ids: &[i64],
    now_ms: i64,
) -> agentdb_meta::MetaResult<()> {
    let mut by_node = BTreeMap::<String, (String, i64, i64, i64)>::new();
    for source_id in source_tablet_ids {
        for replica in
            agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), *source_id).await?
        {
            let priority = lifecycle_replica_role_priority(&replica.role);
            if priority == 0 {
                continue;
            }
            by_node
                .entry(replica.node_id)
                .and_modify(|existing| {
                    if priority > existing.1 {
                        existing.0 = replica.role.clone();
                        existing.1 = priority;
                    }
                    existing.2 = existing.2.max(replica.applied_index);
                    existing.3 = existing.3.max(replica.match_index);
                })
                .or_insert((
                    replica.role,
                    priority,
                    replica.applied_index,
                    replica.match_index,
                ));
        }
    }
    for destination_id in destination_tablet_ids {
        for (node_id, (role, _, applied_index, match_index)) in &by_node {
            agentdb_meta::tablet_replicas::upsert(
                meta.pool(),
                *destination_id,
                node_id,
                role,
                "lagging",
                *applied_index,
                *match_index,
                now_ms,
            )
            .await?;
        }
    }
    Ok(())
}

fn lifecycle_replica_role_priority(role: &str) -> i64 {
    match role {
        "voter" => 3,
        "learner" => 2,
        "log_only" => 1,
        _ => 0,
    }
}

fn contiguous_range_span(
    ranges: &[agentdb_meta::tablet_ranges::TabletRange],
) -> Option<(i64, i64)> {
    if ranges.is_empty() {
        return None;
    }
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|range| (range.start_shard, range.end_shard));
    for pair in sorted.windows(2) {
        if pair[0].end_shard + 1 != pair[1].start_shard {
            return None;
        }
    }
    Some((sorted[0].start_shard, sorted[sorted.len() - 1].end_shard))
}

fn shard_count(start_shard: i64, end_shard: i64) -> i64 {
    end_shard.saturating_sub(start_shard).saturating_add(1)
}

async fn run_tablet_movement_tick(
    meta: &agentdb_meta::MetaStore,
    now_ms: i64,
    batch_size: i64,
) -> agentdb_meta::MetaResult<usize> {
    let pending = agentdb_meta::tablet_movements::list_pending(meta.pool(), batch_size).await?;
    let mut completed = 0;
    for movement in pending {
        if tablet_movement_destination_ready(meta, &movement).await?
            && agentdb_meta::tablet_movements::mark_completed(
                meta.pool(),
                &movement.movement_id,
                now_ms,
            )
            .await?
        {
            completed += 1;
        }
    }
    Ok(completed)
}

async fn cleanup_tombstoned_tablet_replicas(
    meta: &agentdb_meta::MetaStore,
) -> agentdb_meta::MetaResult<u64> {
    let mut deleted = 0u64;
    for tablet in agentdb_meta::tablets::list_all(meta.pool()).await? {
        if tablet.state != "tombstoned" {
            continue;
        }
        deleted +=
            agentdb_meta::tablet_replicas::delete_by_tablet(meta.pool(), tablet.tablet_id).await?;
    }
    Ok(deleted)
}

async fn complete_ready_tablet_movement_cutovers(
    meta: &agentdb_meta::MetaStore,
    now_ms: i64,
    batch_size: i64,
) -> agentdb_meta::MetaResult<usize> {
    let movements = agentdb_meta::tablet_movements::list_open(meta.pool(), batch_size).await?;
    let mut split_groups =
        BTreeMap::<(String, i64), Vec<agentdb_meta::tablet_movements::TabletMovement>>::new();
    let mut merge_groups =
        BTreeMap::<(String, i64), Vec<agentdb_meta::tablet_movements::TabletMovement>>::new();
    for movement in movements {
        match movement.movement_type.as_str() {
            "split" => split_groups
                .entry((
                    movement.placement_scope_id.clone(),
                    movement.source_tablet_id,
                ))
                .or_default()
                .push(movement),
            "merge" => merge_groups
                .entry((
                    movement.placement_scope_id.clone(),
                    movement.destination_tablet_id,
                ))
                .or_default()
                .push(movement),
            _ => {}
        }
    }

    let mut cutovers = 0usize;
    for ((placement_scope_id, source_tablet_id), _) in split_groups {
        cutovers +=
            complete_ready_split_cutover(meta, &placement_scope_id, source_tablet_id, now_ms)
                .await?;
    }
    for ((placement_scope_id, destination_tablet_id), _) in merge_groups {
        cutovers +=
            complete_ready_merge_cutover(meta, &placement_scope_id, destination_tablet_id, now_ms)
                .await?;
    }
    Ok(cutovers)
}

async fn complete_ready_split_cutover(
    meta: &agentdb_meta::MetaStore,
    placement_scope_id: &str,
    source_tablet_id: i64,
    now_ms: i64,
) -> agentdb_meta::MetaResult<usize> {
    let movements = agentdb_meta::tablet_movements::list_open_split_group(
        meta.pool(),
        placement_scope_id,
        source_tablet_id,
    )
    .await?;
    if movements.is_empty()
        || movements
            .iter()
            .any(|movement| movement.status != "completed")
    {
        return Ok(0);
    }
    let Some(source) = agentdb_meta::tablets::get(meta.pool(), source_tablet_id).await? else {
        return Ok(0);
    };
    if source.state != "splitting" {
        return Ok(0);
    }
    for movement in &movements {
        if !tablet_movement_destination_ready(meta, movement).await? {
            return Ok(0);
        }
        let Some(destination) =
            agentdb_meta::tablets::get(meta.pool(), movement.destination_tablet_id).await?
        else {
            return Ok(0);
        };
        if destination.state != "creating" && destination.state != "serving" {
            return Ok(0);
        }
    }

    let old_ranges = scoped_tablet_ranges(meta, source_tablet_id, placement_scope_id).await?;
    if old_ranges.is_empty() {
        return Ok(0);
    }
    let old_bounds = range_bounds_from_ranges(&old_ranges);
    let mut new_ranges: Vec<_> = movements
        .iter()
        .map(|movement| agentdb_meta::tablet_ranges::ReplacementRange {
            start_shard: movement.start_shard,
            end_shard: movement.end_shard,
            tablet_id: movement.destination_tablet_id,
        })
        .collect();
    new_ranges.sort_by_key(|range| (range.start_shard, range.end_shard, range.tablet_id));
    if let Err(error) =
        agentdb_meta::tablet_ranges::validate_replacement_plan(&old_bounds, &new_ranges)
    {
        tracing::debug!(
            source_tablet_id,
            placement_scope_id,
            error = %error,
            "split movement group is not ready for cutover"
        );
        return Ok(0);
    }
    let next_epoch = next_range_map_epoch(&old_ranges);
    agentdb_meta::tablet_ranges::replace_ranges(
        meta.pool(),
        placement_scope_id,
        &old_bounds,
        &new_ranges,
        next_epoch,
        now_ms,
    )
    .await?;
    for movement in &movements {
        agentdb_meta::tablets::set_state(meta.pool(), movement.destination_tablet_id, "serving")
            .await?;
    }
    agentdb_meta::tablets::set_state(meta.pool(), source_tablet_id, "tombstoned").await?;
    Ok(1)
}

async fn complete_ready_merge_cutover(
    meta: &agentdb_meta::MetaStore,
    placement_scope_id: &str,
    destination_tablet_id: i64,
    now_ms: i64,
) -> agentdb_meta::MetaResult<usize> {
    let movements = agentdb_meta::tablet_movements::list_open_merge_group(
        meta.pool(),
        placement_scope_id,
        destination_tablet_id,
    )
    .await?;
    if movements.len() < 2
        || movements
            .iter()
            .any(|movement| movement.status != "completed")
    {
        return Ok(0);
    }
    let Some(destination) = agentdb_meta::tablets::get(meta.pool(), destination_tablet_id).await?
    else {
        return Ok(0);
    };
    if destination.state != "creating" && destination.state != "serving" {
        return Ok(0);
    }
    let mut old_ranges = Vec::new();
    for movement in &movements {
        if !tablet_movement_destination_ready(meta, movement).await? {
            return Ok(0);
        }
        let Some(source) =
            agentdb_meta::tablets::get(meta.pool(), movement.source_tablet_id).await?
        else {
            return Ok(0);
        };
        if source.state != "merging" {
            return Ok(0);
        }
        let source_ranges =
            scoped_tablet_ranges(meta, movement.source_tablet_id, placement_scope_id).await?;
        if source_ranges.is_empty() {
            return Ok(0);
        }
        old_ranges.extend(source_ranges);
    }
    let old_bounds = range_bounds_from_ranges(&old_ranges);
    let Some(start_shard) = old_ranges.iter().map(|range| range.start_shard).min() else {
        return Ok(0);
    };
    let end_shard = old_ranges
        .iter()
        .map(|range| range.end_shard)
        .max()
        .expect("old range start existed above");
    let new_ranges = vec![agentdb_meta::tablet_ranges::ReplacementRange {
        start_shard,
        end_shard,
        tablet_id: destination_tablet_id,
    }];
    if let Err(error) =
        agentdb_meta::tablet_ranges::validate_replacement_plan(&old_bounds, &new_ranges)
    {
        tracing::debug!(
            destination_tablet_id,
            placement_scope_id,
            error = %error,
            "merge movement group is not ready for cutover"
        );
        return Ok(0);
    }
    let next_epoch = next_range_map_epoch(&old_ranges);
    agentdb_meta::tablet_ranges::replace_ranges(
        meta.pool(),
        placement_scope_id,
        &old_bounds,
        &new_ranges,
        next_epoch,
        now_ms,
    )
    .await?;
    agentdb_meta::tablets::set_state(meta.pool(), destination_tablet_id, "serving").await?;
    for movement in &movements {
        agentdb_meta::tablets::set_state(meta.pool(), movement.source_tablet_id, "tombstoned")
            .await?;
    }
    Ok(1)
}

async fn scoped_tablet_ranges(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    placement_scope_id: &str,
) -> agentdb_meta::MetaResult<Vec<agentdb_meta::tablet_ranges::TabletRange>> {
    Ok(
        agentdb_meta::tablet_ranges::list_by_tablet(meta.pool(), tablet_id)
            .await?
            .into_iter()
            .filter(|range| range.placement_scope_id == placement_scope_id)
            .collect(),
    )
}

fn range_bounds_from_ranges(
    ranges: &[agentdb_meta::tablet_ranges::TabletRange],
) -> Vec<agentdb_meta::tablet_ranges::RangeBounds> {
    ranges
        .iter()
        .map(|range| agentdb_meta::tablet_ranges::RangeBounds {
            start_shard: range.start_shard,
            end_shard: range.end_shard,
        })
        .collect()
}

fn next_range_map_epoch(ranges: &[agentdb_meta::tablet_ranges::TabletRange]) -> i64 {
    ranges
        .iter()
        .map(|range| range.map_epoch)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

async fn run_local_tablet_movement_executor_tick(
    meta: &agentdb_meta::MetaStore,
    runtime: &Arc<agentdb_core::Runtime>,
    movement_snapshot_client: Option<&agentdb_api::internal_raft_http::RaftRsHttpClient>,
    node_id: &str,
    now_ms: i64,
    batch_size: i64,
) -> anyhow::Result<usize> {
    let pending = agentdb_meta::tablet_movements::list_pending(meta.pool(), batch_size).await?;
    let registered_tablets = runtime.registered_tablet_ids();
    let mut by_destination =
        BTreeMap::<i64, Vec<agentdb_meta::tablet_movements::TabletMovement>>::new();
    for movement in pending {
        if !registered_tablets.contains(&movement.destination_tablet_id) {
            continue;
        }
        let source_is_local = registered_tablets.contains(&movement.source_tablet_id)
            && local_source_replica_ready(
                meta,
                movement.source_tablet_id,
                node_id,
                movement.required_applied_index,
            )
            .await?;
        if !source_is_local
            && (movement_snapshot_client.is_none()
                || source_replica_url(
                    meta,
                    movement.source_tablet_id,
                    node_id,
                    movement.required_applied_index,
                )
                .await?
                .is_none())
        {
            continue;
        }
        if local_serving_replica(meta, movement.destination_tablet_id, node_id)
            .await?
            .is_none()
        {
            continue;
        }
        by_destination
            .entry(movement.destination_tablet_id)
            .or_default()
            .push(movement);
    }

    let mut executed = 0usize;
    for (destination_tablet_id, movements) in by_destination {
        let movement_type = movements[0].movement_type.as_str();
        if movement_type == "split" && movements.len() != 1 {
            tracing::warn!(
                destination_tablet_id,
                movement_count = movements.len(),
                "skipping ambiguous split movement group"
            );
            continue;
        }
        if movements
            .iter()
            .any(|movement| movement.movement_type.as_str() != movement_type)
        {
            tracing::warn!(
                destination_tablet_id,
                "skipping mixed tablet movement group"
            );
            continue;
        }

        let mut max_installed_index = 0i64;
        let mut installed_count = 0usize;
        for movement in &movements {
            let Some(scope) =
                agentdb_meta::placement_scopes::get(meta.pool(), &movement.placement_scope_id)
                    .await?
            else {
                tracing::warn!(
                    movement_id = %movement.movement_id,
                    placement_scope_id = %movement.placement_scope_id,
                    "skipping tablet movement with missing placement scope"
                );
                continue;
            };
            let payload = if registered_tablets.contains(&movement.source_tablet_id)
                && local_source_replica_ready(
                    meta,
                    movement.source_tablet_id,
                    node_id,
                    movement.required_applied_index,
                )
                .await?
            {
                runtime
                    .export_tablet_snapshot_payload_for_shard_range(
                        movement.source_tablet_id,
                        movement.destination_tablet_id,
                        movement.start_shard,
                        movement.end_shard,
                        scope.shard_count,
                    )
                    .map_err(|error| anyhow::anyhow!("{error:?}"))?
            } else {
                let client = movement_snapshot_client
                    .ok_or_else(|| anyhow::anyhow!("tablet movement snapshot client is missing"))?;
                let base_url = source_replica_url(
                    meta,
                    movement.source_tablet_id,
                    node_id,
                    movement.required_applied_index,
                )
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no healthy caught-up remote source replica URL for tablet {}",
                        movement.source_tablet_id
                    )
                })?;
                client
                    .export_tablet_movement_snapshot(
                        &base_url,
                        &agentdb_api::internal_raft_http::TabletMovementSnapshotExportRequest {
                            source_tablet_id: movement.source_tablet_id,
                            target_tablet_id: movement.destination_tablet_id,
                            start_shard: movement.start_shard,
                            end_shard: movement.end_shard,
                            shard_count: scope.shard_count,
                        },
                    )
                    .await?
                    .payload()?
            };
            let snapshot: agentdb_core::tablet_state::TabletStateSnapshot =
                serde_json::from_slice(&payload)?;
            let installed = match movement_type {
                "split" => runtime
                    .install_tablet_snapshot_payload_and_rehydrate(
                        movement.destination_tablet_id,
                        &payload,
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error:?}"))?,
                "merge" => runtime
                    .install_tablet_snapshot_payload_merge_and_rehydrate(
                        movement.destination_tablet_id,
                        &payload,
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error:?}"))?,
                other => {
                    tracing::warn!(
                        movement_id = %movement.movement_id,
                        movement_type = other,
                        "skipping unsupported tablet movement type"
                    );
                    false
                }
            };
            if installed {
                installed_count += 1;
                max_installed_index = max_installed_index.max(snapshot.applied_index as i64);
            }
        }

        if installed_count != movements.len() {
            continue;
        }
        agentdb_meta::tablet_replicas::set_progress(
            meta.pool(),
            destination_tablet_id,
            node_id,
            max_installed_index,
            max_installed_index,
            now_ms,
        )
        .await?;
        agentdb_meta::tablet_replicas::set_health(
            meta.pool(),
            destination_tablet_id,
            node_id,
            "healthy",
            now_ms,
        )
        .await?;
        executed += installed_count;
    }
    Ok(executed)
}

async fn local_serving_replica(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    node_id: &str,
) -> agentdb_meta::MetaResult<Option<agentdb_meta::tablet_replicas::TabletReplica>> {
    Ok(
        agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), tablet_id)
            .await?
            .into_iter()
            .find(|replica| replica.node_id == node_id && replica.role != "log_only"),
    )
}

async fn local_source_replica_ready(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    node_id: &str,
    required_applied_index: i64,
) -> agentdb_meta::MetaResult<bool> {
    Ok(local_serving_replica(meta, tablet_id, node_id)
        .await?
        .is_some_and(|replica| {
            replica.health == "healthy" && replica.applied_index >= required_applied_index
        }))
}

async fn source_replica_url(
    meta: &agentdb_meta::MetaStore,
    tablet_id: i64,
    local_node_id: &str,
    required_applied_index: i64,
) -> anyhow::Result<Option<String>> {
    for replica in agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), tablet_id).await? {
        if replica.node_id == local_node_id || replica.role == "log_only" {
            continue;
        }
        if replica.health != "healthy" || replica.applied_index < required_applied_index {
            continue;
        }
        let Some(node) = agentdb_meta::nodes::get(meta.pool(), &replica.node_id).await? else {
            continue;
        };
        if node.address.trim().is_empty() {
            continue;
        }
        return Ok(Some(node.address));
    }
    Ok(None)
}

async fn tablet_movement_destination_ready(
    meta: &agentdb_meta::MetaStore,
    movement: &agentdb_meta::tablet_movements::TabletMovement,
) -> agentdb_meta::MetaResult<bool> {
    let replicas =
        agentdb_meta::tablet_replicas::list_by_tablet(meta.pool(), movement.destination_tablet_id)
            .await?;
    let mut serving_replicas = 0usize;
    for replica in replicas {
        if replica.role == "log_only" {
            continue;
        }
        serving_replicas += 1;
        if replica.health != "healthy" || replica.applied_index < movement.required_applied_index {
            return Ok(false);
        }
    }
    Ok(serving_replicas > 0)
}

async fn refresh_route_snapshot(
    meta: &agentdb_meta::MetaStore,
    routing: &agentdb_api::routing_http::RouteRuntime,
    shard_count: i64,
    route_location: Option<String>,
) {
    match load_route_snapshot(meta, shard_count, route_location).await {
        Ok(snapshot) => {
            routing.refresh_snapshot(snapshot);
            tracing::debug!("refreshed route snapshot");
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to refresh route snapshot");
        }
    }
}

fn now_ms_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_meta::{
        nodes, tablet_movements, tablet_ranges, tablet_replicas, tablet_stats, tablets, MetaStore,
    };
    use agentdb_storage::ReplicatedLog;

    fn base_opts() -> ServeOpts {
        ServeOpts {
            config: None,
            bind: None,
            data_dir: None,
            content_dir: None,
            in_memory: false,
            dev: false,
            route_node_id: None,
            route_shard_count: None,
            route_region: None,
            route_location: None,
            route_refresh_interval_ms: None,
            route_proxy_timeout_ms: None,
            route_proxy_max_response_bytes: None,
            mongo_bind: None,
            mongo_username: None,
            mongo_password: None,
            internal_auth_token: None,
            internal_auth_previous_token: None,
        }
    }

    fn lifecycle_config(split_max_shards: i64, merge_max_shards: i64) -> ClusterWorkerConfig {
        ClusterWorkerConfig {
            heartbeat_interval_ms: 5_000,
            scheduler_lease_ms: 30_000,
            scheduler_renew_interval_ms: 10_000,
            stale_node_threshold_ms: 1_000,
            reaper_tick_ms: 10_000,
            reaper_safety_margin_ms: 0,
            rebalance_max_movements: 0,
            tablet_movement_batch_size: 64,
            tablet_movement_executor_interval_ms: 5_000,
            tablet_lifecycle_split_max_shards: split_max_shards,
            tablet_lifecycle_merge_max_shards: merge_max_shards,
            tablet_lifecycle_split_max_keys: 0,
            tablet_lifecycle_merge_max_keys: 0,
            tablet_lifecycle_split_max_storage_bytes: 0,
            tablet_lifecycle_merge_max_storage_bytes: 0,
            tablet_lifecycle_max_actions: 4,
        }
    }

    #[test]
    fn default_single_node_config_matches_public_quickstarts() {
        let config = ResolvedServeConfig::from_serve_opts(base_opts()).unwrap();

        assert_eq!(config.deployment_mode, DeploymentMode::SingleNode);
        assert_eq!(config.bind, default_bind());
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.content_dir, PathBuf::from("./data/content"));
        assert!(!config.in_memory);
    }

    #[test]
    fn dev_alias_uses_in_memory_runtime() {
        let mut opts = base_opts();
        opts.dev = true;

        assert!(
            ResolvedServeConfig::from_serve_opts(opts)
                .unwrap()
                .in_memory
        );
    }

    #[test]
    fn explicit_content_dir_overrides_data_dir_default() {
        let mut opts = base_opts();
        opts.data_dir = Some(PathBuf::from("/var/lib/agentdb"));
        opts.content_dir = Some(PathBuf::from("/mnt/agentdb-content"));

        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();

        assert_eq!(config.data_dir, PathBuf::from("/var/lib/agentdb"));
        assert_eq!(config.content_dir, PathBuf::from("/mnt/agentdb-content"));
    }

    #[test]
    fn config_file_sets_single_node_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[server]
bind = "127.0.0.1:8787"

[storage]
data_dir = "/tmp/agentdb-test"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);

        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();

        assert_eq!(config.bind, "127.0.0.1:8787".parse().unwrap());
        assert_eq!(config.data_dir, PathBuf::from("/tmp/agentdb-test"));
        assert_eq!(
            config.content_dir,
            PathBuf::from("/tmp/agentdb-test/content")
        );
    }

    #[test]
    fn cli_values_override_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[server]
bind = "127.0.0.1:8787"
dev = false

[storage]
data_dir = "/tmp/from-config"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        opts.bind = Some("127.0.0.1:8989".parse().unwrap());
        opts.data_dir = Some(PathBuf::from("/tmp/from-cli"));
        opts.dev = true;

        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();

        assert_eq!(config.bind, "127.0.0.1:8989".parse().unwrap());
        assert_eq!(config.data_dir, PathBuf::from("/tmp/from-cli"));
        assert!(config.in_memory);
    }

    #[test]
    fn cluster_mode_requires_slatedb() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[deployment]
mode = "cluster"

[storage.kv]
backend = "rocksdb"

[storage.content]
backend = "object_store"

[storage.object_store]
backend = "s3"
bucket = "agentdb-test"
region = "us-west-2"

[routing]
node_id = "node-a"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();
        assert!(err
            .to_string()
            .contains("deployment.mode=cluster requires storage.kv.backend=slatedb"));
    }

    #[test]
    fn cluster_mode_requires_object_store_content() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[deployment]
mode = "cluster"

[storage.kv]
backend = "slatedb"

[storage.content]
backend = "localfs"

[storage.object_store]
backend = "s3"
bucket = "agentdb-test"
region = "us-west-2"

[routing]
node_id = "node-a"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();
        assert!(err
            .to_string()
            .contains("deployment.mode=cluster requires storage.content.backend=object_store"));
    }

    #[test]
    fn cluster_mode_requires_s3_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[deployment]
mode = "cluster"

[storage.kv]
backend = "slatedb"

[storage.content]
backend = "object_store"

[storage.object_store]
backend = "localfs"
local_root = "/tmp/agentdb-test"

[routing]
node_id = "node-a"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();
        assert!(err
            .to_string()
            .contains("deployment.mode=cluster requires storage.object_store.backend=s3"));
    }

    #[test]
    fn cluster_mode_requires_node_identity() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[deployment]
mode = "cluster"

[storage.kv]
backend = "slatedb"

[storage.content]
backend = "object_store"

[storage.object_store]
backend = "s3"
bucket = "agentdb-test"
region = "us-west-2"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();
        assert!(err
            .to_string()
            .contains("deployment.mode=cluster requires routing.node_id or --route-node-id"));
    }

    #[test]
    fn cluster_mode_requires_internal_auth_token() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[deployment]
mode = "cluster"

[storage.kv]
backend = "slatedb"

[storage.content]
backend = "object_store"

[storage.object_store]
backend = "s3"
bucket = "agentdb-test"
region = "us-west-2"

[routing]
node_id = "node-a"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();
        assert!(err
            .to_string()
            .contains("deployment.mode=cluster requires server.internal_auth_token"));
    }

    #[test]
    fn internal_auth_token_must_not_be_too_short() {
        let mut opts = base_opts();
        opts.internal_auth_token = Some("short".into());

        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();

        assert!(err
            .to_string()
            .contains("server.internal_auth_token must be at least 16 characters"));
    }

    #[test]
    fn previous_internal_auth_token_must_be_distinct_for_rotation() {
        let mut opts = base_opts();
        opts.internal_auth_token = Some("current-token-123".into());
        opts.internal_auth_previous_token = Some("current-token-123".into());

        let err = ResolvedServeConfig::from_serve_opts(opts).unwrap_err();

        assert!(err
            .to_string()
            .contains("server.internal_auth_previous_token must differ"));
    }

    #[test]
    fn single_node_slatedb_s3_remains_valid() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("agentdb.toml");
        std::fs::write(
            &config_path,
            r#"
[deployment]
mode = "single_node"

[storage.kv]
backend = "slatedb"

[storage.content]
backend = "object_store"

[storage.object_store]
backend = "s3"
bucket = "agentdb-test"
region = "us-west-2"
"#,
        )
        .unwrap();
        let mut opts = base_opts();
        opts.config = Some(config_path);
        let config = ResolvedServeConfig::from_serve_opts(opts).unwrap();
        assert_eq!(config.deployment_mode, DeploymentMode::SingleNode);
        assert_eq!(
            deployment_support_note(&config),
            Some("single-node SlateDB + S3 is intended for testing/evaluation, not the primary production deployment")
        );
    }

    #[tokio::test]
    async fn heartbeat_node_once_upserts_node_row() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        heartbeat_node_once(
            &meta,
            "node-a",
            "http://127.0.0.1:18080",
            Some("us-west-2"),
            "alive",
            1_000,
        )
        .await
        .unwrap();
        let node = nodes::get(meta.pool(), "node-a").await.unwrap().unwrap();
        assert_eq!(node.address, "http://127.0.0.1:18080");
        assert_eq!(node.state, "alive");
        assert_eq!(node.region.as_deref(), Some("us-west-2"));
    }

    #[tokio::test]
    async fn scheduler_tick_marks_stale_nodes_and_clears_expired_tablets() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        nodes::upsert(
            meta.pool(),
            "node-b",
            "http://127.0.0.1:18081",
            None,
            "alive",
            100,
            None,
            Some("us-west-2"),
        )
        .await
        .unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablets::assume_ownership(meta.pool(), 7, 0, "node-b", 500)
            .await
            .unwrap();

        let status = agentdb_api::state::SchedulerStatus::new();
        let config = ClusterWorkerConfig {
            heartbeat_interval_ms: 5_000,
            scheduler_lease_ms: 30_000,
            scheduler_renew_interval_ms: 10_000,
            stale_node_threshold_ms: 1_000,
            reaper_tick_ms: 10_000,
            reaper_safety_margin_ms: 0,
            rebalance_max_movements: 8,
            tablet_movement_batch_size: 64,
            tablet_movement_executor_interval_ms: 5_000,
            tablet_lifecycle_split_max_shards: 0,
            tablet_lifecycle_merge_max_shards: 0,
            tablet_lifecycle_split_max_keys: 0,
            tablet_lifecycle_merge_max_keys: 0,
            tablet_lifecycle_split_max_storage_bytes: 0,
            tablet_lifecycle_merge_max_storage_bytes: 0,
            tablet_lifecycle_max_actions: 4,
        };
        run_cluster_scheduler_tick(&meta, "leader-a", 10_000, config, &status, None)
            .await
            .unwrap();

        let stale_node = nodes::get(meta.pool(), "node-b").await.unwrap().unwrap();
        assert_eq!(stale_node.state, "dead");
        let tablet = tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert!(tablet.owner_node_id.is_none());

        let snapshot = status.snapshot();
        assert!(snapshot.is_leader);
        assert_eq!(snapshot.dead_nodes_marked_total, 1);
        assert_eq!(snapshot.tablet_reassignments_total, 1);
        assert_eq!(snapshot.leader_node_id.as_deref(), Some("leader-a"));
    }

    #[tokio::test]
    async fn scheduler_tick_as_follower_does_not_reap() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        agentdb_meta::scheduler::try_acquire(meta.pool(), "leader-a", 30_000, 1_000)
            .await
            .unwrap()
            .unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablets::assume_ownership(meta.pool(), 7, 0, "node-b", 500)
            .await
            .unwrap();

        let status = agentdb_api::state::SchedulerStatus::new();
        let config = ClusterWorkerConfig {
            heartbeat_interval_ms: 5_000,
            scheduler_lease_ms: 30_000,
            scheduler_renew_interval_ms: 10_000,
            stale_node_threshold_ms: 1_000,
            reaper_tick_ms: 10_000,
            reaper_safety_margin_ms: 0,
            rebalance_max_movements: 8,
            tablet_movement_batch_size: 64,
            tablet_movement_executor_interval_ms: 5_000,
            tablet_lifecycle_split_max_shards: 0,
            tablet_lifecycle_merge_max_shards: 0,
            tablet_lifecycle_split_max_keys: 0,
            tablet_lifecycle_merge_max_keys: 0,
            tablet_lifecycle_split_max_storage_bytes: 0,
            tablet_lifecycle_merge_max_storage_bytes: 0,
            tablet_lifecycle_max_actions: 4,
        };
        run_cluster_scheduler_tick(&meta, "follower-b", 2_000, config, &status, None)
            .await
            .unwrap();

        let tablet = tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert_eq!(tablet.owner_node_id.as_deref(), Some("node-b"));
        let snapshot = status.snapshot();
        assert!(!snapshot.is_leader);
        assert_eq!(snapshot.leader_node_id.as_deref(), Some("leader-a"));
        assert_eq!(snapshot.tablet_reassignments_total, 0);
    }

    #[tokio::test]
    async fn scheduler_tick_assigns_unowned_tablets() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        nodes::upsert(
            meta.pool(),
            "node-a",
            "http://127.0.0.1:18080",
            None,
            "alive",
            10_000,
            None,
            Some("us-west-2"),
        )
        .await
        .unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();

        let status = agentdb_api::state::SchedulerStatus::new();
        let config = ClusterWorkerConfig {
            heartbeat_interval_ms: 5_000,
            scheduler_lease_ms: 30_000,
            scheduler_renew_interval_ms: 10_000,
            stale_node_threshold_ms: 1_000,
            reaper_tick_ms: 10_000,
            reaper_safety_margin_ms: 0,
            rebalance_max_movements: 8,
            tablet_movement_batch_size: 64,
            tablet_movement_executor_interval_ms: 5_000,
            tablet_lifecycle_split_max_shards: 0,
            tablet_lifecycle_merge_max_shards: 0,
            tablet_lifecycle_split_max_keys: 0,
            tablet_lifecycle_merge_max_keys: 0,
            tablet_lifecycle_split_max_storage_bytes: 0,
            tablet_lifecycle_merge_max_storage_bytes: 0,
            tablet_lifecycle_max_actions: 4,
        };
        run_cluster_scheduler_tick(&meta, "leader-a", 10_000, config, &status, None)
            .await
            .unwrap();

        let tablet = tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert_eq!(tablet.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(status.snapshot().tablet_reassignments_total, 1);
    }

    #[tokio::test]
    async fn scheduler_tick_prepares_replacement_for_draining_owner() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        nodes::upsert(
            meta.pool(),
            "node-a",
            "http://127.0.0.1:18080",
            None,
            "alive",
            10_000,
            None,
            Some("us-west-2"),
        )
        .await
        .unwrap();
        nodes::upsert(
            meta.pool(),
            "node-b",
            "http://127.0.0.1:18081",
            None,
            "draining",
            10_000,
            None,
            Some("us-west-2"),
        )
        .await
        .unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablets::assume_ownership(meta.pool(), 7, 0, "node-b", 20_000)
            .await
            .unwrap();

        let status = agentdb_api::state::SchedulerStatus::new();
        let config = ClusterWorkerConfig {
            heartbeat_interval_ms: 5_000,
            scheduler_lease_ms: 30_000,
            scheduler_renew_interval_ms: 10_000,
            stale_node_threshold_ms: 1_000,
            reaper_tick_ms: 10_000,
            reaper_safety_margin_ms: 0,
            rebalance_max_movements: 8,
            tablet_movement_batch_size: 64,
            tablet_movement_executor_interval_ms: 5_000,
            tablet_lifecycle_split_max_shards: 0,
            tablet_lifecycle_merge_max_shards: 0,
            tablet_lifecycle_split_max_keys: 0,
            tablet_lifecycle_merge_max_keys: 0,
            tablet_lifecycle_split_max_storage_bytes: 0,
            tablet_lifecycle_merge_max_storage_bytes: 0,
            tablet_lifecycle_max_actions: 4,
        };
        run_cluster_scheduler_tick(&meta, "leader-a", 10_000, config, &status, None)
            .await
            .unwrap();

        let tablet = tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert_eq!(tablet.owner_node_id.as_deref(), Some("node-b"));
        assert_eq!(tablet.handoff_target_node_id.as_deref(), Some("node-a"));
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id == "node-b" && replica.role == "voter"));
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id == "node-a" && replica.role == "learner"));
        assert_eq!(status.snapshot().tablet_reassignments_total, 1);
    }

    #[tokio::test]
    async fn scheduler_tick_completes_ready_replacement() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        nodes::upsert(
            meta.pool(),
            "node-a",
            "http://127.0.0.1:18080",
            None,
            "alive",
            10_000,
            None,
            Some("us-west-2"),
        )
        .await
        .unwrap();
        nodes::upsert(
            meta.pool(),
            "node-b",
            "http://127.0.0.1:18081",
            None,
            "alive",
            10_000,
            None,
            Some("us-west-2"),
        )
        .await
        .unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablets::assume_ownership(meta.pool(), 7, 0, "node-b", 20_000)
            .await
            .unwrap();
        tablets::mark_for_handoff(meta.pool(), 7, "node-a")
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 10, 10, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "learner", "healthy", 10, 10, 100)
            .await
            .unwrap();

        let status = agentdb_api::state::SchedulerStatus::new();
        let config = ClusterWorkerConfig {
            heartbeat_interval_ms: 5_000,
            scheduler_lease_ms: 30_000,
            scheduler_renew_interval_ms: 10_000,
            stale_node_threshold_ms: 1_000,
            reaper_tick_ms: 10_000,
            reaper_safety_margin_ms: 0,
            rebalance_max_movements: 8,
            tablet_movement_batch_size: 64,
            tablet_movement_executor_interval_ms: 5_000,
            tablet_lifecycle_split_max_shards: 0,
            tablet_lifecycle_merge_max_shards: 0,
            tablet_lifecycle_split_max_keys: 0,
            tablet_lifecycle_merge_max_keys: 0,
            tablet_lifecycle_split_max_storage_bytes: 0,
            tablet_lifecycle_merge_max_storage_bytes: 0,
            tablet_lifecycle_max_actions: 4,
        };
        run_cluster_scheduler_tick(&meta, "leader-a", 10_000, config, &status, None)
            .await
            .unwrap();

        let tablet = tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert_eq!(tablet.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(tablet.handoff_target_node_id, None);
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id == "node-a" && replica.role == "voter"));
        assert_eq!(status.snapshot().tablet_reassignments_total, 1);
    }

    #[tokio::test]
    async fn tablet_lifecycle_policy_prepares_oversized_range_split() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablet_ranges::upsert(meta.pool(), "default", 0, 99, 7, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 12, 12, 100)
            .await
            .unwrap();

        let report = run_tablet_lifecycle_policy_tick(&meta, 1_000, &lifecycle_config(50, 0))
            .await
            .unwrap();

        assert_eq!(report.prepared_split_count, 1);
        assert_eq!(report.prepared_merge_count, 0);
        assert_eq!(
            tablets::get(meta.pool(), 7).await.unwrap().unwrap().state,
            "splitting"
        );
        assert_eq!(
            tablets::get(meta.pool(), 8).await.unwrap().unwrap().state,
            "creating"
        );
        assert_eq!(
            tablets::get(meta.pool(), 9).await.unwrap().unwrap().state,
            "creating"
        );
        let movements = tablet_movements::list_pending(meta.pool(), 10)
            .await
            .unwrap();
        assert_eq!(movements.len(), 2);
        assert!(movements.iter().any(|movement| {
            movement.movement_id == "split:7:8"
                && movement.start_shard == 0
                && movement.end_shard == 49
                && movement.required_applied_index == 12
        }));
        assert!(movements.iter().any(|movement| {
            movement.movement_id == "split:7:9"
                && movement.start_shard == 50
                && movement.end_shard == 99
                && movement.required_applied_index == 12
        }));
        let destination_replicas = tablet_replicas::list_by_tablet(meta.pool(), 8)
            .await
            .unwrap();
        assert!(destination_replicas
            .iter()
            .any(|replica| replica.node_id == "node-a" && replica.health == "lagging"));
    }

    #[tokio::test]
    async fn tablet_lifecycle_policy_prepares_adjacent_small_range_merge() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablets::get_or_create(meta.pool(), 8, 100).await.unwrap();
        tablet_ranges::upsert(meta.pool(), "default", 0, 9, 7, 1, 100)
            .await
            .unwrap();
        tablet_ranges::upsert(meta.pool(), "default", 10, 19, 8, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 7, 7, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-a", "voter", "healthy", 9, 9, 100)
            .await
            .unwrap();

        let report = run_tablet_lifecycle_policy_tick(&meta, 1_000, &lifecycle_config(0, 10))
            .await
            .unwrap();

        assert_eq!(report.prepared_split_count, 0);
        assert_eq!(report.prepared_merge_count, 1);
        assert_eq!(
            tablets::get(meta.pool(), 7).await.unwrap().unwrap().state,
            "merging"
        );
        assert_eq!(
            tablets::get(meta.pool(), 8).await.unwrap().unwrap().state,
            "merging"
        );
        assert_eq!(
            tablets::get(meta.pool(), 9).await.unwrap().unwrap().state,
            "creating"
        );
        let movements = tablet_movements::list_pending(meta.pool(), 10)
            .await
            .unwrap();
        assert_eq!(movements.len(), 2);
        assert!(movements.iter().all(|movement| {
            movement.movement_type == "merge"
                && movement.destination_tablet_id == 9
                && movement.required_applied_index == 9
        }));
    }

    #[tokio::test]
    async fn tablet_lifecycle_policy_prepares_split_from_storage_stats() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablet_ranges::upsert(meta.pool(), "default", 0, 99, 7, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 12, 12, 100)
            .await
            .unwrap();
        tablet_stats::upsert(meta.pool(), 7, "node-a", 10, 100, 1_500, 1_600, 12, 200)
            .await
            .unwrap();

        let mut config = lifecycle_config(0, 0);
        config.tablet_lifecycle_split_max_storage_bytes = 1_000;
        let report = run_tablet_lifecycle_policy_tick(&meta, 1_000, &config)
            .await
            .unwrap();

        assert_eq!(report.prepared_split_count, 1);
        let movements = tablet_movements::list_pending(meta.pool(), 10)
            .await
            .unwrap();
        assert_eq!(movements.len(), 2);
        assert!(movements
            .iter()
            .all(|movement| movement.movement_type == "split"));
    }

    #[tokio::test]
    async fn tablet_lifecycle_policy_prepares_merge_from_storage_stats() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablets::get_or_create(meta.pool(), 7, 100).await.unwrap();
        tablets::get_or_create(meta.pool(), 8, 100).await.unwrap();
        tablet_ranges::upsert(meta.pool(), "default", 0, 49, 7, 1, 100)
            .await
            .unwrap();
        tablet_ranges::upsert(meta.pool(), "default", 50, 99, 8, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 7, 7, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-a", "voter", "healthy", 9, 9, 100)
            .await
            .unwrap();
        tablet_stats::upsert(meta.pool(), 7, "node-a", 3, 30, 300, 330, 7, 200)
            .await
            .unwrap();
        tablet_stats::upsert(meta.pool(), 8, "node-a", 4, 40, 400, 440, 9, 200)
            .await
            .unwrap();

        let mut config = lifecycle_config(0, 0);
        config.tablet_lifecycle_merge_max_storage_bytes = 500;
        let report = run_tablet_lifecycle_policy_tick(&meta, 1_000, &config)
            .await
            .unwrap();

        assert_eq!(report.prepared_merge_count, 1);
        let movements = tablet_movements::list_pending(meta.pool(), 10)
            .await
            .unwrap();
        assert_eq!(movements.len(), 2);
        assert!(movements
            .iter()
            .all(|movement| movement.movement_type == "merge"));
    }

    #[tokio::test]
    async fn scheduler_rebalance_membership_change_promotes_target_and_retires_source() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let configs = vec![
            agentdb_storage::RaftRsNodeConfig::from_control_plane_replica(
                7,
                "node-b",
                "voter",
                dir.path(),
            )
            .unwrap(),
            agentdb_storage::RaftRsNodeConfig::from_control_plane_replica(
                7,
                "node-a",
                "learner",
                dir.path(),
            )
            .unwrap(),
        ];
        let leader_id = agentdb_storage::stable_raft_node_id("node-b").unwrap();
        let (group, _log) = agentdb_storage::RaftRsSharedGroup::open_local(7, configs, leader_id)
            .expect("open local raft group");
        group.elect(leader_id).expect("elect source leader");
        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        registry.register(7, group).unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 10, 10, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 10, 10, 100)
            .await
            .unwrap();

        let changed = apply_scheduler_rebalance_raft_membership_changes(
            &meta,
            &registry,
            &[agentdb_meta::tablet_rebalance::RebalanceAction {
                action: "complete_replacement",
                tablet_id: 7,
                source_node_id: Some("node-b".into()),
                target_node_id: Some("node-a".into()),
                changed: true,
                reason: "target_caught_up",
            }],
        )
        .await
        .unwrap();

        assert_eq!(changed, 2);
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        assert!(!replicas.iter().any(|replica| replica.node_id == "node-b"));
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id == "node-a" && replica.role == "voter"));
    }

    #[tokio::test]
    async fn scheduler_rebalance_membership_change_skips_unregistered_group() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "draining", 10, 10, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 10, 10, 100)
            .await
            .unwrap();

        let changed = apply_scheduler_rebalance_raft_membership_changes(
            &meta,
            &registry,
            &[agentdb_meta::tablet_rebalance::RebalanceAction {
                action: "complete_replacement",
                tablet_id: 7,
                source_node_id: Some("node-b".into()),
                target_node_id: Some("node-a".into()),
                changed: true,
                reason: "target_caught_up",
            }],
        )
        .await
        .unwrap();

        assert_eq!(changed, 0);
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        assert!(replicas.iter().any(|replica| replica.node_id == "node-b"));
        assert!(replicas.iter().any(|replica| replica.node_id == "node-a"));

        assert_eq!(
            retire_draining_rebalance_raft_replicas(&meta, &registry)
                .await
                .unwrap(),
            0
        );
        let dir = tempfile::tempdir().unwrap();
        let configs = vec![
            agentdb_storage::RaftRsNodeConfig::from_control_plane_replica(
                7,
                "node-a",
                "voter",
                dir.path(),
            )
            .unwrap(),
            agentdb_storage::RaftRsNodeConfig::from_control_plane_replica(
                7,
                "node-b",
                "learner",
                dir.path(),
            )
            .unwrap(),
        ];
        let leader_id = agentdb_storage::stable_raft_node_id("node-a").unwrap();
        let (group, _log) = agentdb_storage::RaftRsSharedGroup::open_local(7, configs, leader_id)
            .expect("open local raft group");
        group.elect(leader_id).expect("elect target leader");
        registry.register(7, group).unwrap();
        assert_eq!(
            retire_draining_rebalance_raft_replicas(&meta, &registry)
                .await
                .unwrap(),
            1
        );
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        assert!(!replicas.iter().any(|replica| replica.node_id == "node-b"));
    }

    #[tokio::test]
    async fn tablet_movement_tick_completes_ready_destinations() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablet_movements::upsert_pending(
            meta.pool(),
            "split:7:8",
            "split",
            7,
            8,
            "tenant:paid-a",
            0,
            49,
            42,
            1_000,
        )
        .await
        .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-a", "voter", "healthy", 42, 42, 1_010)
            .await
            .unwrap();
        tablet_replicas::upsert(
            meta.pool(),
            8,
            "node-b",
            "learner",
            "healthy",
            50,
            50,
            1_010,
        )
        .await
        .unwrap();

        assert_eq!(run_tablet_movement_tick(&meta, 2_000, 64).await.unwrap(), 1);

        let movement = tablet_movements::get(meta.pool(), "split:7:8")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(movement.status, "completed");
        assert_eq!(movement.completed_at_ms, Some(2_000));
    }

    #[tokio::test]
    async fn tablet_movement_tick_leaves_unready_destinations_pending() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablet_movements::upsert_pending(
            meta.pool(),
            "merge:7:8:9",
            "merge",
            7,
            9,
            "tenant:paid-a",
            0,
            99,
            42,
            1_000,
        )
        .await
        .unwrap();
        tablet_replicas::upsert(meta.pool(), 9, "node-a", "voter", "healthy", 42, 42, 1_010)
            .await
            .unwrap();
        tablet_replicas::upsert(
            meta.pool(),
            9,
            "node-b",
            "learner",
            "healthy",
            41,
            41,
            1_010,
        )
        .await
        .unwrap();

        assert_eq!(run_tablet_movement_tick(&meta, 2_000, 64).await.unwrap(), 0);

        let movement = tablet_movements::get(meta.pool(), "merge:7:8:9")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(movement.status, "pending");
        assert_eq!(movement.completed_at_ms, None);
    }

    #[tokio::test]
    async fn tombstoned_tablet_cleanup_deletes_replica_rows() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let scope = agentdb_meta::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;
        tablets::create_with_placement(meta.pool(), 7, scope, "tombstoned", Some("raft/7"), 0, 100)
            .await
            .unwrap();
        tablets::create_with_placement(meta.pool(), 8, scope, "serving", Some("raft/8"), 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 1, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "learner", "healthy", 1, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-a", "voter", "healthy", 1, 1, 100)
            .await
            .unwrap();

        assert_eq!(cleanup_tombstoned_tablet_replicas(&meta).await.unwrap(), 2);
        assert!(tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            tablet_replicas::list_by_tablet(meta.pool(), 8)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn tablet_movement_cutover_completes_ready_split_group() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let scope = agentdb_meta::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;
        tablets::create_with_placement(meta.pool(), 7, scope, "splitting", Some("raft/7"), 0, 100)
            .await
            .unwrap();
        tablets::create_with_placement(meta.pool(), 8, scope, "creating", Some("raft/8"), 0, 100)
            .await
            .unwrap();
        tablets::create_with_placement(meta.pool(), 9, scope, "creating", Some("raft/9"), 0, 100)
            .await
            .unwrap();
        tablet_ranges::upsert(meta.pool(), scope, 0, 99, 7, 1, 100)
            .await
            .unwrap();
        for tablet_id in [8, 9] {
            tablet_replicas::upsert(
                meta.pool(),
                tablet_id,
                "node-a",
                "voter",
                "healthy",
                42,
                42,
                150,
            )
            .await
            .unwrap();
        }
        tablet_movements::upsert_pending(
            meta.pool(),
            "split:7:8",
            "split",
            7,
            8,
            scope,
            0,
            49,
            42,
            200,
        )
        .await
        .unwrap();
        tablet_movements::upsert_pending(
            meta.pool(),
            "split:7:9",
            "split",
            7,
            9,
            scope,
            50,
            99,
            42,
            200,
        )
        .await
        .unwrap();
        tablet_movements::mark_completed(meta.pool(), "split:7:8", 300)
            .await
            .unwrap();

        assert_eq!(
            complete_ready_tablet_movement_cutovers(&meta, 400, 64)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            tablets::get(meta.pool(), 7).await.unwrap().unwrap().state,
            "splitting"
        );

        tablet_movements::mark_completed(meta.pool(), "split:7:9", 500)
            .await
            .unwrap();
        assert_eq!(
            complete_ready_tablet_movement_cutovers(&meta, 600, 64)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            tablets::get(meta.pool(), 7).await.unwrap().unwrap().state,
            "tombstoned"
        );
        assert_eq!(
            tablets::get(meta.pool(), 8).await.unwrap().unwrap().state,
            "serving"
        );
        let ranges = tablet_ranges::list_all(meta.pool()).await.unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].tablet_id, 8);
        assert_eq!(ranges[0].map_epoch, 2);
        assert_eq!(ranges[1].tablet_id, 9);
        assert_eq!(ranges[1].map_epoch, 2);
    }

    #[tokio::test]
    async fn tablet_movement_cutover_completes_ready_merge_group() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let scope = agentdb_meta::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;
        tablets::create_with_placement(meta.pool(), 7, scope, "merging", Some("raft/7"), 0, 100)
            .await
            .unwrap();
        tablets::create_with_placement(meta.pool(), 8, scope, "merging", Some("raft/8"), 0, 100)
            .await
            .unwrap();
        tablets::create_with_placement(meta.pool(), 9, scope, "creating", Some("raft/9"), 0, 100)
            .await
            .unwrap();
        tablet_ranges::upsert(meta.pool(), scope, 0, 49, 7, 3, 100)
            .await
            .unwrap();
        tablet_ranges::upsert(meta.pool(), scope, 50, 99, 8, 3, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 9, "node-a", "voter", "healthy", 42, 42, 150)
            .await
            .unwrap();
        tablet_movements::upsert_pending(
            meta.pool(),
            "merge:7:9",
            "merge",
            7,
            9,
            scope,
            0,
            49,
            42,
            200,
        )
        .await
        .unwrap();
        tablet_movements::upsert_pending(
            meta.pool(),
            "merge:8:9",
            "merge",
            8,
            9,
            scope,
            50,
            99,
            42,
            200,
        )
        .await
        .unwrap();
        tablet_movements::mark_completed(meta.pool(), "merge:7:9", 300)
            .await
            .unwrap();
        tablet_movements::mark_completed(meta.pool(), "merge:8:9", 300)
            .await
            .unwrap();

        assert_eq!(
            complete_ready_tablet_movement_cutovers(&meta, 400, 64)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            tablets::get(meta.pool(), 7).await.unwrap().unwrap().state,
            "tombstoned"
        );
        assert_eq!(
            tablets::get(meta.pool(), 8).await.unwrap().unwrap().state,
            "tombstoned"
        );
        assert_eq!(
            tablets::get(meta.pool(), 9).await.unwrap().unwrap().state,
            "serving"
        );
        let ranges = tablet_ranges::list_all(meta.pool()).await.unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].tablet_id, 9);
        assert_eq!(ranges[0].start_shard, 0);
        assert_eq!(ranges[0].end_shard, 99);
        assert_eq!(ranges[0].map_epoch, 4);
    }

    #[tokio::test]
    async fn local_tablet_movement_executor_installs_split_snapshot_and_updates_progress() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 1, 1, 1_000)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-a", "learner", "lagging", 0, 0, 1_000)
            .await
            .unwrap();
        tablet_movements::upsert_pending(
            meta.pool(),
            "split:7:8",
            "split",
            7,
            8,
            agentdb_meta::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID,
            0,
            16_383,
            1,
            1_000,
        )
        .await
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let kv: Arc<dyn agentdb_storage::KvStore + Send + Sync> =
            Arc::new(agentdb_storage::RocksKvStore::open(dir.path()).unwrap());
        let runtime = agentdb_core::Runtime::with_kv_and_replicated_logs(
            kv.clone(),
            Arc::new(agentdb_storage::InMemoryContentBackend::default()),
            BTreeMap::from([
                (
                    agentdb_core::RUNTIME_PRIMARY_TABLET_ID,
                    Arc::new(agentdb_storage::SingleNodeReplicatedLog::new(kv.clone()))
                        as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
                ),
                (
                    7,
                    Arc::new(agentdb_storage::SingleNodeReplicatedLog::new(kv.clone()))
                        as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
                ),
                (
                    8,
                    Arc::new(agentdb_storage::SingleNodeReplicatedLog::new(kv))
                        as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
                ),
            ]),
        );
        let create_runtime = runtime.clone();
        let tenant = runtime
            .with_tablet_context(7, async move {
                create_runtime
                    .create_tenant(agentdb_core::NewTenant {
                        external_key: "local-movement".into(),
                        policy: serde_json::json!({}),
                    })
                    .await
            })
            .await
            .unwrap();

        let executed =
            run_local_tablet_movement_executor_tick(&meta, &runtime, None, "node-a", 2_000, 64)
                .await
                .unwrap();

        assert_eq!(executed, 1);
        let replica = tablet_replicas::list_by_tablet(meta.pool(), 8)
            .await
            .unwrap()
            .into_iter()
            .find(|replica| replica.node_id == "node-a")
            .unwrap();
        assert_eq!(replica.health, "healthy");
        assert_eq!(replica.applied_index, 1);

        let read_runtime = runtime.clone();
        let moved_tenant = runtime
            .with_tablet_context(8, async move {
                read_runtime.get_tenant(&tenant.tenant_id).await
            })
            .await
            .unwrap();
        assert_eq!(moved_tenant.external_key, "local-movement");
    }

    #[tokio::test]
    async fn tablet_movement_source_selection_requires_healthy_caught_up_replica() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        nodes::upsert(
            meta.pool(),
            "node-b",
            "http://127.0.0.1:18081",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        nodes::upsert(
            meta.pool(),
            "node-c",
            "http://127.0.0.1:18082",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 9, 9, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 9, 9, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-c", "voter", "healthy", 10, 10, 100)
            .await
            .unwrap();

        assert!(!local_source_replica_ready(&meta, 7, "node-a", 10)
            .await
            .unwrap());
        assert_eq!(
            source_replica_url(&meta, 7, "node-a", 10).await.unwrap(),
            Some("http://127.0.0.1:18082".to_string())
        );
    }

    #[tokio::test]
    async fn raft_rs_node_configs_load_from_control_plane_replicas() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "learner", "lagging", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-c", "log_only", "healthy", 0, 0, 100)
            .await
            .unwrap();

        let configs = load_raft_rs_node_configs(&meta, 7, dir.path())
            .await
            .unwrap();

        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].role, agentdb_storage::RaftRsNodeRole::Voter);
        assert!(configs[0].raft_engine_dir.ends_with("tablet-7/node-a"));
        assert_eq!(configs[1].role, agentdb_storage::RaftRsNodeRole::Learner);
        assert!(configs[1].raft_engine_dir.ends_with("tablet-7/node-b"));
    }

    #[tokio::test]
    async fn raft_rs_node_configs_require_raft_replicas() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-c", "log_only", "healthy", 0, 0, 100)
            .await
            .unwrap();

        let err = load_raft_rs_node_configs(&meta, 7, dir.path())
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("tablet 7 has no voter/learner replicas"));
    }

    #[tokio::test]
    async fn raft_rs_registry_bootstrap_registers_local_replicas() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        nodes::upsert(
            meta.pool(),
            "node-a",
            "http://127.0.0.1:18080",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        nodes::upsert(
            meta.pool(),
            "node-b",
            "http://127.0.0.1:18081",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();

        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        let bootstrap = sync_raft_rs_registry(&meta, "node-a", dir.path(), registry.clone())
            .await
            .unwrap();

        assert_eq!(bootstrap.tablet_ids, vec![7]);
        assert_eq!(bootstrap.bootstrapped_tablet_ids, vec![7]);
        assert!(bootstrap.local_logs.contains_key(&7));
        assert_eq!(registry.tablet_count(), 1);
        let groups = registry.groups_snapshot();
        assert_eq!(groups[0].1.status().len(), 1);
        assert_eq!(
            groups[0].1.status()[0].node_id,
            agentdb_storage::stable_raft_node_id("node-a").unwrap()
        );
    }

    #[tokio::test]
    async fn raft_rs_registry_bootstrap_skips_log_only_local_replicas() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "log_only", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();

        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        let bootstrap = sync_raft_rs_registry(&meta, "node-a", dir.path(), registry.clone())
            .await
            .unwrap();

        assert!(bootstrap.tablet_ids.is_empty());
        assert!(bootstrap.bootstrapped_tablet_ids.is_empty());
        assert!(bootstrap.local_logs.is_empty());
        assert_eq!(registry.tablet_count(), 0);
    }

    #[tokio::test]
    async fn raft_rs_registry_sync_unregisters_removed_local_replicas() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        sync_raft_rs_registry(&meta, "node-a", dir.path(), registry.clone())
            .await
            .unwrap();
        assert_eq!(registry.tablet_ids(), vec![7]);

        tablet_replicas::upsert(meta.pool(), 7, "node-a", "log_only", "healthy", 0, 0, 200)
            .await
            .unwrap();
        let bootstrap = sync_raft_rs_registry(&meta, "node-a", dir.path(), registry.clone())
            .await
            .unwrap();

        assert!(bootstrap.tablet_ids.is_empty());
        assert!(registry.tablet_ids().is_empty());
    }

    #[tokio::test]
    async fn raft_rs_registry_sync_ignores_tombstoned_local_replicas() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let scope = agentdb_meta::placement_scopes::DEFAULT_PLACEMENT_SCOPE_ID;
        tablets::create_with_placement(meta.pool(), 7, scope, "tombstoned", Some("raft/7"), 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();

        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        let bootstrap = sync_raft_rs_registry(&meta, "node-a", dir.path(), registry.clone())
            .await
            .unwrap();

        assert!(bootstrap.tablet_ids.is_empty());
        assert!(bootstrap.bootstrapped_tablet_ids.is_empty());
        assert!(bootstrap.local_logs.is_empty());
        assert!(registry.tablet_ids().is_empty());
    }

    #[tokio::test]
    async fn raft_rs_registry_sync_bootstraps_split_destination_runtime_tablet() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        let kv: Arc<dyn agentdb_storage::KvStore + Send + Sync> =
            Arc::new(agentdb_storage::RocksKvStore::open(dir.path().join("runtime")).unwrap());
        let raft_engine_root = dir.path().join("raft-engine");
        let runtime = agentdb_core::Runtime::with_kv_and_replicated_logs(
            kv.clone(),
            Arc::new(agentdb_storage::InMemoryContentBackend::default()),
            BTreeMap::from([(
                agentdb_core::RUNTIME_PRIMARY_TABLET_ID,
                Arc::new(agentdb_storage::SingleNodeReplicatedLog::new(kv))
                    as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
            )]),
        );
        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        sync_raft_rs_registry_and_runtime(
            &meta,
            "node-a",
            &raft_engine_root,
            registry.clone(),
            runtime.clone(),
        )
        .await
        .unwrap();
        assert!(runtime.registered_tablet_ids().contains(&7));

        tablet_replicas::upsert(meta.pool(), 8, "node-a", "learner", "lagging", 0, 0, 200)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 8, "node-b", "voter", "healthy", 0, 0, 200)
            .await
            .unwrap();
        let bootstrap = sync_raft_rs_registry_and_runtime(
            &meta,
            "node-a",
            &raft_engine_root,
            registry.clone(),
            runtime.clone(),
        )
        .await
        .unwrap();

        assert_eq!(bootstrap.bootstrapped_tablet_ids, vec![8]);
        assert!(registry.tablet_ids().contains(&8));
        assert!(runtime.registered_tablet_ids().contains(&8));
    }

    #[tokio::test]
    async fn raft_rs_peer_urls_load_from_control_plane_nodes() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        nodes::upsert(
            meta.pool(),
            "node-a",
            "http://127.0.0.1:18080",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        nodes::upsert(
            meta.pool(),
            "node-b",
            "http://127.0.0.1:18081",
            None,
            "alive",
            100,
            None,
            None,
        )
        .await
        .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "learner", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-c", "log_only", "healthy", 0, 0, 100)
            .await
            .unwrap();

        let peer_urls = load_raft_rs_peer_urls(&meta, [7]).await.unwrap();

        assert_eq!(
            peer_urls.get(&agentdb_storage::stable_raft_node_id("node-a").unwrap()),
            Some(&"http://127.0.0.1:18080".to_string())
        );
        assert_eq!(
            peer_urls.get(&agentdb_storage::stable_raft_node_id("node-b").unwrap()),
            Some(&"http://127.0.0.1:18081".to_string())
        );
        assert!(!peer_urls.contains_key(&agentdb_storage::stable_raft_node_id("node-c").unwrap()));
    }

    #[tokio::test]
    async fn raft_rs_snapshot_tick_marks_progress_and_unreachable_peers() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "unknown", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "learner", "healthy", 0, 0, 100)
            .await
            .unwrap();
        let raft_configs = load_raft_rs_node_configs(&meta, 7, dir.path())
            .await
            .unwrap();
        let local_node_id = agentdb_storage::stable_raft_node_id("node-a").unwrap();
        let (group, tablet_log) =
            agentdb_storage::RaftRsSharedGroup::open_local(7, raft_configs, local_node_id).unwrap();
        group.elect(local_node_id).unwrap();

        let kv: Arc<dyn agentdb_storage::KvStore + Send + Sync> =
            Arc::new(agentdb_storage::RocksKvStore::open(dir.path().join("runtime")).unwrap());
        let primary_log: Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync> =
            Arc::new(agentdb_storage::SingleNodeReplicatedLog::new(kv.clone()));
        let runtime = agentdb_core::Runtime::with_kv_and_replicated_logs(
            kv,
            Arc::new(agentdb_storage::InMemoryContentBackend::default()),
            BTreeMap::from([
                (agentdb_core::RUNTIME_PRIMARY_TABLET_ID, primary_log),
                (
                    7,
                    tablet_log as Arc<dyn agentdb_storage::ReplicatedLog + Send + Sync>,
                ),
            ]),
        );
        let create_runtime = runtime.clone();
        runtime
            .with_tablet_context(7, async move {
                create_runtime
                    .create_tenant(agentdb_core::NewTenant {
                        external_key: "snapshot-progress".into(),
                        policy: serde_json::json!({}),
                    })
                    .await
            })
            .await
            .unwrap();

        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        registry.register(7, group).unwrap();
        let outbox = agentdb_api::internal_raft_http::RaftRsHttpOutbox::new("secret");
        let sent = run_raft_rs_snapshot_tick(
            &meta,
            &registry,
            runtime,
            &outbox,
            1,
            DEFAULT_RAFT_SNAPSHOT_RETENTION_MS,
            None,
        )
        .await
        .unwrap();

        assert_eq!(sent, 0);
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        let local = replicas
            .iter()
            .find(|replica| replica.node_id == "node-a")
            .unwrap();
        assert_eq!(local.health, "healthy");
        assert!(local.applied_index > 0);
        assert!(local.match_index > 0);
        let remote = replicas
            .iter()
            .find(|replica| replica.node_id == "node-b")
            .unwrap();
        assert_eq!(remote.health, "unreachable");
    }

    #[tokio::test]
    async fn raft_snapshot_object_store_puts_and_deletes_payload() {
        let dir = tempfile::tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let snapshot_store =
            RaftSnapshotObjectStore::new(object_store.clone(), RAFT_SNAPSHOT_OBJECT_PREFIX);
        let transfer = agentdb_storage::RaftRsSnapshotTransfer {
            tablet_id: 7,
            from: 1,
            to: 2,
            index: 42,
            term: 3,
            payload: b"retained-snapshot".to_vec(),
        };

        let uri = snapshot_store.put_transfer(&transfer).await.unwrap();
        let path = uri.strip_prefix("object://").unwrap();
        let result = object_store.get(&ObjectPath::from(path)).await.unwrap();
        assert_eq!(result.bytes().await.unwrap().as_ref(), b"retained-snapshot");

        snapshot_store.delete_uri(&uri).await.unwrap();
        assert!(matches!(
            object_store.head(&ObjectPath::from(path)).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn raft_snapshot_prune_deletes_object_before_metadata() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let snapshot_store = Arc::new(RaftSnapshotObjectStore::new(
            object_store.clone(),
            RAFT_SNAPSHOT_OBJECT_PREFIX,
        ));
        let transfer = agentdb_storage::RaftRsSnapshotTransfer {
            tablet_id: 7,
            from: 1,
            to: 2,
            index: 42,
            term: 3,
            payload: b"expired-snapshot".to_vec(),
        };
        let uri = snapshot_store.put_transfer(&transfer).await.unwrap();
        let path = uri.strip_prefix("object://").unwrap().to_string();
        agentdb_meta::tablet_snapshots::record(
            meta.pool(),
            "snap-expired-object",
            7,
            "node-a",
            1,
            "node-b",
            2,
            42,
            3,
            transfer.payload.len() as i64,
            &agentdb_storage::sha256_hex(&transfer.payload),
            Some(&uri),
            "available",
            1_000,
            Some(1),
        )
        .await
        .unwrap();

        let deleted = run_raft_snapshot_prune_tick(&meta, Some(snapshot_store), 10)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(matches!(
            object_store.head(&ObjectPath::from(path)).await,
            Err(object_store::Error::NotFound { .. })
        ));
        let snapshots = agentdb_meta::tablet_snapshots::list_recent(meta.pool(), None, true, 10)
            .await
            .unwrap();
        assert_eq!(snapshots[0].status, "deleted");
    }

    #[tokio::test]
    async fn raft_snapshot_restore_installs_retained_object_snapshot() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 1, 1, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "learner", "lagging", 0, 0, 100)
            .await
            .unwrap();
        let source_raft_configs = load_raft_rs_node_configs(&meta, 7, source_dir.path())
            .await
            .unwrap();
        let target_raft_configs = load_raft_rs_node_configs(&meta, 7, target_dir.path())
            .await
            .unwrap();
        let node_a = agentdb_storage::stable_raft_node_id("node-a").unwrap();
        let node_b = agentdb_storage::stable_raft_node_id("node-b").unwrap();
        let (source_group, logs) =
            agentdb_storage::RaftRsSharedGroup::open(7, source_raft_configs).unwrap();
        source_group.elect(node_a).unwrap();
        logs[0].append(7, b"snapshot-source".to_vec()).unwrap();
        let transfer = source_group
            .build_snapshot_transfer(node_a, node_b, Vec::new())
            .unwrap();
        drop(source_group);
        drop(logs);

        let object_dir = tempfile::tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(object_dir.path()).unwrap());
        let snapshot_store = Arc::new(RaftSnapshotObjectStore::new(
            object_store,
            RAFT_SNAPSHOT_OBJECT_PREFIX,
        ));
        let uri = snapshot_store.put_transfer(&transfer).await.unwrap();
        agentdb_meta::tablet_snapshots::record(
            meta.pool(),
            "snap-restore-object",
            7,
            "node-a",
            node_a.min(i64::MAX as u64) as i64,
            "node-b",
            node_b.min(i64::MAX as u64) as i64,
            transfer.index as i64,
            transfer.term as i64,
            transfer.payload.len() as i64,
            &agentdb_storage::sha256_hex(&transfer.payload),
            Some(&uri),
            "available",
            1_000,
            None,
        )
        .await
        .unwrap();

        let (target_group, _target_log) =
            agentdb_storage::RaftRsSharedGroup::open_local(7, target_raft_configs, node_b).unwrap();
        let registry = agentdb_api::raft_ingress::RaftRsGroupRegistry::shared();
        registry.register(7, target_group.clone()).unwrap();
        let runtime = agentdb_core::Runtime::in_memory();

        let installed =
            run_raft_snapshot_restore_tick(&meta, "node-b", &registry, runtime, &snapshot_store)
                .await
                .unwrap();

        assert_eq!(installed, 1);
        assert_eq!(target_group.status()[0].applied_index, transfer.index);
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        let target = replicas
            .iter()
            .find(|replica| replica.node_id == "node-b")
            .unwrap();
        assert_eq!(target.health, "healthy");
        assert_eq!(target.applied_index, transfer.index as i64);
    }

    #[tokio::test]
    async fn raft_rs_message_outbox_marks_unreachable_and_restores_messages() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-a", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(meta.pool(), 7, "node-b", "voter", "healthy", 0, 0, 100)
            .await
            .unwrap();
        let raft_configs = load_raft_rs_node_configs(&meta, 7, dir.path())
            .await
            .unwrap();
        let node_a = agentdb_storage::stable_raft_node_id("node-a").unwrap();
        let node_b = agentdb_storage::stable_raft_node_id("node-b").unwrap();
        let (group, _log) =
            agentdb_storage::RaftRsSharedGroup::open_local(7, raft_configs, node_a).unwrap();
        group
            .restore_wire_messages(vec![agentdb_storage::RaftRsWireMessage {
                tablet_id: 7,
                from: node_a,
                to: node_b,
                payload: b"pending-message".to_vec(),
            }])
            .unwrap();
        let outbox = agentdb_api::internal_raft_http::RaftRsHttpOutbox::new("secret");

        let err = flush_raft_rs_group_messages_with_health(&meta, 7, &group, &outbox, 1_024)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("peer URL is not configured"));
        assert_eq!(group.take_wire_messages().len(), 1);
        let replicas = tablet_replicas::list_by_tablet(meta.pool(), 7)
            .await
            .unwrap();
        let remote = replicas
            .iter()
            .find(|replica| replica.node_id == "node-b")
            .unwrap();
        assert_eq!(remote.health, "unreachable");
    }

    #[tokio::test]
    async fn cluster_object_store_content_uses_async_local_primary_mirror() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = ResolvedServeConfig::from_serve_opts(base_opts()).unwrap();
        config.deployment_mode = DeploymentMode::Cluster;
        config.content_backend = ContentBackendKind::ObjectStore;
        config.content_dir = dir.path().join("content-primary");
        config.object_store = Some(ResolvedObjectStoreConfig {
            backend: ObjectStoreBackendKind::LocalFs,
            local_root: Some(dir.path().join("content-mirror")),
            bucket: None,
            region: None,
            endpoint: None,
            allow_http: false,
        });

        let (content, mirror) = build_content_backend(&config).unwrap();
        content.put("ten_a", "art_x", b"hello").await.unwrap();

        let mirror = mirror.expect("cluster object-store content should be mirrored");
        for _ in 0..100 {
            if mirror.stats().completed >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stats = mirror.stats();
        assert_eq!(stats.queued, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.backlog, 0);
        assert_eq!(content.get("ten_a", "art_x").await.unwrap(), b"hello");
    }
}
