//! MongoDB wire compatibility for AgentDB.
//!
//! This crate intentionally implements a narrow compatibility subset. It is an
//! adapter onto AgentDB runtime semantics, not a second storage engine.

pub mod bsonx;
pub mod command;
pub mod error;
pub mod opmsg;
pub mod server;

pub use error::{WireError, WireResult};
