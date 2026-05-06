//! AgentDB storage layer.
//!
//! Provides the `KvStore` trait and a RocksDB-backed implementation.
//! Keys are produced via the helpers in [`keys`] (tenant-first logical
//! string keys) to guarantee tenant-scoped prefix scans.

pub mod content;
pub mod content_crypto;
pub mod error;
pub mod keys;
pub mod kms;
pub mod kv;
pub mod metadata_crypto;
pub mod raft_rs_log;
pub mod replicated_log;
pub mod rocks;
pub mod slate;
pub mod tablet_store;

pub use content::{
    sha256_hex, AsyncMirrorContentBackend, AsyncMirrorStats, ContentBackend,
    InMemoryContentBackend, LocalFsContentBackend, ObjectStoreContentBackend, PutResult,
};
pub use content_crypto::{
    ContentCryptoAdapter, ContentCryptoContext, IdentityContentCryptoAdapter,
};
pub use error::{Error, Result};
pub use kms::{
    AllowAllKmsKeyResolver, KeyOperation, KeyResolution, KmsError, KmsKeyResolver, KmsResult,
    StaticKeyState, StaticKmsKeyResolver,
};
pub use kv::{KvIter, KvSnapshot, KvStore, WriteOp};
pub use metadata_crypto::{
    IdentityMetadataCryptoAdapter, MetadataCryptoAdapter, MetadataCryptoContext, MetadataObjectKind,
};
pub use raft_rs_log::{
    RaftRsNodeConfig, RaftRsReplicaStatus, RaftRsReplicatedLog, RaftRsSharedGroup,
};
pub use replicated_log::{
    LogCommit, LogEntry, RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftLogRecord,
    RaftLogTransport, RaftQuorumReplicatedLog, ReplicatedLog, SingleNodeReplicatedLog,
};
pub use rocks::RocksKvStore;
pub use slate::SlateKvStore;
pub use tablet_store::TabletKvStore;
