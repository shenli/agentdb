use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use agentdb_api::{router, AppState};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let data_dir: PathBuf = std::env::var_os("AGENTDB_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/rocksdb"));
    let num_boxes: u16 = match std::env::var("AGENTDB_BOXES") {
        Ok(v) => v.parse().context("AGENTDB_BOXES must be a valid u16")?,
        Err(_) => 4,
    };
    let addr: SocketAddr = match std::env::var("AGENTDB_ADDR") {
        Ok(v) => v
            .parse()
            .context("AGENTDB_ADDR must be a valid socket address")?,
        Err(_) => "127.0.0.1:7878".parse().unwrap(),
    };

    std::fs::create_dir_all(&data_dir).context("create data dir")?;
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(&data_dir).context("open rocksdb")?);
    let registry = Arc::new(BoxRegistry::new(kv.clone(), num_boxes)?);
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));
    let state = AppState {
        registry,
        tenants,
        sessions,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind listener")?;
    tracing::info!(%addr, "agentdb-server listening");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}
