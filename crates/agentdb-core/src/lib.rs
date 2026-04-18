//! AgentDB core types, metadata registries, and box runtime.

pub mod box_runtime;
pub mod error;
pub mod meta;
pub mod primitives;
pub mod registry;
pub mod types;

pub use box_runtime::{BoxRegistry, Box_};
pub use error::{Error, Result};
pub use meta::{
    key_next_session_id, key_next_tenant_id, key_session_by_external, key_session_record,
    key_tenant_by_external, key_tenant_record,
};
pub use primitives::{append_event, fetch_last_n, DEFAULT_BRANCH_ID};
pub use registry::{SessionRegistry, TenantRegistry};
pub use types::{Document, Event, Session, Tenant, Tier};
// Other re-exports added as types land.
