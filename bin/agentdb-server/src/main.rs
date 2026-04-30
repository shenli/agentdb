//! `agentdb` binary — V2 server entrypoint.
//!
//! Mounts the V2 HTTP routes from `agentdb-api` against a `Runtime`. By default
//! the runtime is RocksDB-backed at `--data-dir/runtime`; pass `--in-memory` to
//! get the ephemeral backend (useful for smoke tests and local dev).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "agentdb", about = "AgentDB runtime database (v2)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Start the HTTP server with the V2 router.
    Serve(ServeOpts),
}

#[derive(Parser, Debug)]
struct ServeOpts {
    /// `host:port` to bind. Defaults to `127.0.0.1:8080`.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    /// Filesystem path that holds the RocksDB-backed runtime state. Created if
    /// it doesn't exist. Ignored when `--in-memory` is set.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Filesystem path that holds artifact content (raw blobs). Created if
    /// it doesn't exist. Defaults to `<data_dir>/content`. Ignored when
    /// `--in-memory` is set.
    #[arg(long)]
    content_dir: Option<PathBuf>,

    /// Use an ephemeral in-memory backend instead of RocksDB. Convenient for
    /// smoke testing; everything is lost on shutdown.
    #[arg(long)]
    in_memory: bool,

    /// Enable metadata-backed routing for this node id. When unset, the server
    /// keeps the single-node routing-disabled behavior.
    #[arg(long)]
    route_node_id: Option<String>,

    /// Shard count used by the route planner when no session locator exists.
    #[arg(long, default_value_t = agentdb_meta::shards::DEFAULT_SHARD_COUNT)]
    route_shard_count: i64,

    /// Optional location label for residency-aware routing. Currently compared
    /// with node availability_zone until region and AZ split.
    #[arg(long)]
    route_location: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    match args.cmd {
        Cmd::Serve(opts) => serve(opts).await,
    }
}

async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let runtime = if opts.in_memory {
        tracing::info!("using in-memory runtime (state lost on shutdown)");
        agentdb_core::Runtime::in_memory()
    } else {
        let kv_path = opts.data_dir.join("runtime");
        std::fs::create_dir_all(&kv_path)?;
        let kv = Arc::new(agentdb_storage::RocksKvStore::open(&kv_path)?)
            as Arc<dyn agentdb_storage::KvStore + Send + Sync>;
        tracing::info!(path = %kv_path.display(), "opened RocksDB runtime backend");

        // Artifact content lives in a separate filesystem tree so the
        // RocksDB metadata directory stays small and operators can mount the
        // content tree on a different disk if they need to. Default is
        // `<data_dir>/content`; `--content-dir` overrides it.
        let content_dir = opts
            .content_dir
            .clone()
            .unwrap_or_else(|| opts.data_dir.join("content"));
        std::fs::create_dir_all(&content_dir)?;
        tracing::info!(path = %content_dir.display(), "opened LocalFs artifact content backend");
        let content: Arc<dyn agentdb_storage::ContentBackend> =
            Arc::new(agentdb_storage::LocalFsContentBackend::new(&content_dir));
        agentdb_core::Runtime::with_kv(kv, content)
    };

    let mut state = agentdb_api::AppState::new(runtime);
    if let Some(node_id) = opts.route_node_id.as_deref() {
        let meta = agentdb_meta::MetaStore::open_from_env().await?;
        let snapshot = agentdb_api::routing_http::RouteSnapshot::load_from_meta(
            &meta,
            opts.route_shard_count,
            opts.route_location.clone(),
            agentdb_meta::routing::ResidencyPolicy::default(),
        )
        .await?;
        state = state.with_routing(agentdb_api::routing_http::RouteRuntime::snapshot(
            node_id, snapshot,
        ));
        tracing::info!(
            node_id,
            shard_count = opts.route_shard_count,
            location = ?opts.route_location,
            "enabled metadata-backed route snapshot"
        );
    }
    let app = agentdb_api::router(state).nest("/dashboard", agentdb_dashboard::router());
    let listener = tokio::net::TcpListener::bind(opts.bind).await?;
    tracing::info!(addr = %opts.bind, "agentdb listening");
    axum::serve(listener, app).await?;
    Ok(())
}
