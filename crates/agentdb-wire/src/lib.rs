//! AgentDB MongoDB wire protocol subset.

pub mod bsonx;
pub mod command;
pub mod error;
pub mod handlers;
pub mod opmsg;
pub mod server;

pub use error::{WireError, WireResult};
