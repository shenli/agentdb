//! `agentdb` binary — V2.0 server entrypoint.
//!
//! Mounts the v2 HTTP routes from `agentdb-api` against a `Runtime`. By default
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
    /// Start the HTTP server with the V2.0 router.
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

    /// Use an ephemeral in-memory backend instead of RocksDB. Convenient for
    /// smoke testing; everything is lost on shutdown.
    #[arg(long)]
    in_memory: bool,
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
        agentdb_core::Runtime::with_kv(kv)
    };

    let state = agentdb_api::AppState { runtime };
    let app = agentdb_api::router(state);
    let listener = tokio::net::TcpListener::bind(opts.bind).await?;
    tracing::info!(addr = %opts.bind, "agentdb listening");
    axum::serve(listener, app).await?;
    Ok(())
}
