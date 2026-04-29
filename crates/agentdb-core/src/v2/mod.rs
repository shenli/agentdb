//! V2 runtime database. See `docs/v2_design/agent-runtime-database.md` for the
//! architectural overview.

/// V2 schema/API version, ISO date — opaque, do not parse.
pub const VERSION: &str = "2026-04-28";

pub mod agent;
pub mod errors;
pub mod event;
pub mod id;
pub mod session;
pub mod tenant;

pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
