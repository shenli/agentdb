//! Command-line shape for the `agentdb` binary. Parsed via clap.
//! Subcommands: `serve`, `dump-config`, plus admin HTTP-client
//! wrappers (`drain`, `show-tablets`, `show-scheduler`, `show-nodes`).
//!
//! Every env var the server previously honored still works — flags
//! are overrides, not replacements. The new contract is:
//!
//!   default < env var < explicit flag
//!
//! so operators who already script around env vars lose nothing.

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "agentdb",
    version,
    about = "Agent-native operational database",
    long_about = "Single-binary entry point for the AgentDB server and its admin client. \
                  The server process runs under `agentdb serve`; admin operations \
                  (drain, show-*) are HTTP-client wrappers that talk to a running server."
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Start the AgentDB server.
    Serve(ServeOpts),
    /// Print the effective configuration (default / env / flag) as JSON.
    DumpConfig(DumpConfigOpts),
    /// Mark this server as draining — refuses new writes, stops lease
    /// renewal. HTTP-client wrapper for `POST /api/admin/drain`.
    Drain(AdminOpts),
    /// List every tablet row in the control plane. HTTP-client
    /// wrapper for `GET /api/admin/tablets`.
    ShowTablets(AdminOpts),
    /// Show scheduler leader election + reassignment counters.
    /// HTTP-client wrapper for `GET /api/admin/scheduler`.
    ShowScheduler(AdminOpts),
    /// List registered cluster nodes. HTTP-client wrapper for
    /// `GET /api/admin/nodes`.
    ShowNodes(AdminOpts),
    /// Resolve a session id to its shard + tablet assignment.
    /// HTTP-client wrapper for `GET /api/admin/shards?session_id=...`.
    ShowShard(ShowShardOpts),
}

#[derive(Args, Debug, Clone)]
pub struct ShowShardOpts {
    /// Base URL of the server to talk to.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
    /// Session id to hash + look up. Required positional argument.
    pub session_id: String,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ServeOpts {
    /// Dev mode: ephemeral tempdirs for KV + object store, in-memory
    /// SQLite meta, relaxed timings. Perfect for `cargo run` or a
    /// first-time-user quickstart. Overrides every storage-path env
    /// var but leaves admission / lease config alone.
    #[arg(long)]
    pub dev: bool,

    /// Override AGENTDB_ADDR. HTTP listen address.
    #[arg(long)]
    pub addr: Option<String>,

    /// Override AGENTDB_MONGO_ADDR. Mongo wire listen address.
    #[arg(long = "mongo-addr")]
    pub mongo_addr: Option<String>,

    /// Override AGENTDB_BOXES. Number of boxes per process.
    #[arg(long)]
    pub boxes: Option<u16>,

    /// Override AGENTDB_NODE_ID. This process's node id. Appears in
    /// lease rows and the nodes table.
    #[arg(long = "node-id")]
    pub node_id: Option<String>,

    /// Override AGENTDB_DATA. Local KV (RocksDB) directory.
    #[arg(long = "data-dir")]
    pub data_dir: Option<std::path::PathBuf>,

    /// Override AGENTDB_OBJECTS. Local object-store root directory.
    #[arg(long = "objects-dir")]
    pub objects_dir: Option<std::path::PathBuf>,

    /// Override AGENTDB_META_PATH. Control-plane SQLite file.
    #[arg(long = "meta-path")]
    pub meta_path: Option<String>,
}

#[derive(Args, Debug, Default, Clone)]
pub struct DumpConfigOpts {
    /// Alternate `--dev` preview: print what the config *would*
    /// look like with `--dev` enabled, without actually running.
    #[arg(long)]
    pub dev: bool,
}

#[derive(Args, Debug, Clone)]
pub struct AdminOpts {
    /// Base URL of the server to talk to. Defaults to the local
    /// REST listener at `http://127.0.0.1:7878`.
    #[arg(long = "server-url", default_value = "http://127.0.0.1:7878")]
    pub server_url: String,
}
