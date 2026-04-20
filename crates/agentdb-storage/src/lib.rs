//! AgentDB storage layer.
//!
//! Provides the `KvStore` trait and a RocksDB-backed implementation.
//! Keys are produced via the helpers in [`key`] to guarantee
//! session-locality through lexicographic ordering.

pub mod error;
pub mod key;
pub mod kv;
pub mod rocks;
pub mod slate;

pub use error::{Error, Result};
pub use key::{
    decode_key, encode_key, prefix_for_box, prefix_for_session, prefix_for_session_branch,
    prefix_for_tenant, BoxId, BranchId, DecodedKey, Namespace, Seq, SessionId, TenantId, KEY_LEN,
};
pub use kv::{KvIter, KvSnapshot, KvStore, WriteOp};
pub use rocks::RocksKvStore;
pub use slate::SlateKvStore;
