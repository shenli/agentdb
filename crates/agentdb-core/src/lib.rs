//! AgentDB core types, metadata registries, and box runtime.

pub mod box_runtime;
pub mod error;
pub mod indexes;
pub(crate) mod meta;
pub mod primitives;
pub mod registry;
pub mod types;

pub use box_runtime::{BoxRegistry, Box_};
pub use error::{Error, Result};
pub use indexes::PerBoxIndexes;
pub use primitives::{
    append_and_fetch, append_event, fetch_last_n, scan_session_events, AppendFetchOutput,
    DEFAULT_BRANCH_ID,
};
pub use registry::{SessionRegistry, TenantRegistry};
pub use types::{Document, Event, Session, Tenant, Tier};
