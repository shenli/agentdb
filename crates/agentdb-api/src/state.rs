//! Shared HTTP handler state.
//!
//! Holds the runtime handle so axum extractors can pull it off the router.
//! Cloning `AppState` is cheap — `Runtime` is wrapped in `Arc`.

use std::sync::Arc;

use agentdb_core::Runtime;

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<Runtime>,
}
