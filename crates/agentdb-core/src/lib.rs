//! AgentDB core types, metadata registries, and box runtime.

pub mod box_runtime;
pub mod error;
pub mod meta;
pub mod primitives;
pub mod registry;
pub mod types;

pub use error::{Error, Result};
// Other re-exports added as types land.
