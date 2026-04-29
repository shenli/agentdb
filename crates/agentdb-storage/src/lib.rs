//! AgentDB storage layer.
//!
//! Provides the `KvStore` trait and a RocksDB-backed implementation.
//! Keys are produced via the helpers in [`keys`] (tenant-first logical
//! string keys) to guarantee tenant-scoped prefix scans.

pub mod error;
pub mod keys;
pub mod kv;
pub mod rocks;
pub mod slate;

pub use error::{Error, Result};
pub use kv::{KvIter, KvSnapshot, KvStore, WriteOp};
pub use rocks::RocksKvStore;
pub use slate::SlateKvStore;
