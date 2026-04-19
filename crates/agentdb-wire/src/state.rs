use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};

#[derive(Clone)]
pub struct WireState {
    pub registry: Arc<BoxRegistry>,
    pub tenants: Arc<TenantRegistry>,
    pub sessions: Arc<SessionRegistry>,
}
