//! `agentdb` binary — minimal scaffold.
//!
//! The V1 server (Mongo wire, V1 REST, admin CLI subcommands) was deleted in
//! the v2.0 restructure. The next milestone wires the new flat-namespace
//! runtime + HTTP routes through this binary; for now it accepts a `--bind`
//! address and starts an empty router so the build (and `cargo run --bin
//! agentdb -- serve`) still produces something runnable.

use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "agentdb", about = "AgentDB runtime database (v2)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Start the HTTP server with the empty v2 router.
    Serve(ServeOpts),
}

#[derive(Parser, Debug)]
struct ServeOpts {
    /// `host:port` to bind. Defaults to `127.0.0.1:8080`.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
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
    let runtime = agentdb_core::Runtime::in_memory();
    let state = agentdb_api::AppState { runtime };
    let app = agentdb_api::router(state);
    let listener = tokio::net::TcpListener::bind(opts.bind).await?;
    tracing::info!(addr = %opts.bind, "agentdb listening");
    axum::serve(listener, app).await?;
    Ok(())
}
