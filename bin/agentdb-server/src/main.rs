//! `agentdb` binary — dispatcher. Parses CLI flags (via clap) and
//! hands control to one of the subcommand modules (serve / admin /
//! dump_config).
//!
//! This file used to be 500 lines of server-startup logic; M26
//! split it into `serve.rs` so the dispatcher stays small and the
//! server body is readable on its own.

use clap::Parser;

mod admin;
mod cli;
mod dump_config;
mod serve;
mod tls;
mod tracing_init;

use cli::{AdminTokenCmd, Cli, Cmd, UserCmd};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    match args.cmd {
        Cmd::Serve(opts) => {
            // Tracing init only for the long-running server.
            // Admin CLI subcommands run briefly and print JSON; noisy
            // tracing would drown out their output.
            let (_tracer_guard, tracing_reload) = tracing_init::init()?;
            serve::run(opts, tracing_reload).await
        }
        Cmd::DumpConfig(opts) => dump_config::run(opts),
        Cmd::Drain(opts) => admin::drain(opts).await,
        Cmd::ShowTablets(opts) => admin::show_tablets(opts).await,
        Cmd::ShowScheduler(opts) => admin::show_scheduler(opts).await,
        Cmd::ShowNodes(opts) => admin::show_nodes(opts).await,
        Cmd::ShowShard(opts) => admin::show_shard(opts).await,
        Cmd::SetShard(opts) => admin::set_shard(opts).await,
        Cmd::ListShardOverrides(opts) => admin::list_shard_overrides(opts).await,
        Cmd::User(user_cmd) => match user_cmd {
            UserCmd::Add(opts) => admin::user_add(opts).await,
            UserCmd::List(opts) => admin::user_list(opts).await,
            UserCmd::Delete(opts) => admin::user_delete(opts).await,
        },
        Cmd::ShowHotTablets(opts) => admin::show_hot_tablets(opts).await,
        Cmd::ShowTabletLoad(opts) => admin::show_tablet_load(opts).await,
        Cmd::ShedTablet(opts) => admin::shed_tablet(opts).await,
        Cmd::CreateTablet(opts) => admin::create_tablet(opts).await,
        Cmd::AdminToken(cmd) => match cmd {
            AdminTokenCmd::Add(opts) => admin::admin_token_add(opts).await,
            AdminTokenCmd::List(opts) => admin::admin_token_list(opts).await,
            AdminTokenCmd::Revoke(opts) => admin::admin_token_revoke(opts).await,
        },
        Cmd::MigrateSession(opts) => admin::migrate_session(opts).await,
        Cmd::ShowLatency(opts) => admin::show_latency(opts).await,
        Cmd::AuditLog(opts) => admin::audit_log(opts).await,
        Cmd::SetTenantQuota(opts) => admin::set_tenant_quota(opts).await,
        Cmd::ShowTenantQuota(opts) => admin::show_tenant_quota(opts).await,
        Cmd::ClearTenantQuota(opts) => admin::clear_tenant_quota(opts).await,
        Cmd::ShowSlowQueries(opts) => admin::show_slow_queries(opts).await,
        Cmd::Backup(opts) => admin::backup(opts).await,
        Cmd::Reload(opts) => admin::reload(opts).await,
    }
}
