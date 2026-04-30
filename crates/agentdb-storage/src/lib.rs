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
pub mod rocks;
pub mod slate;

pub use content::{
    sha256_hex, ContentBackend, InMemoryContentBackend, LocalFsContentBackend, PutResult,
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
pub use rocks::RocksKvStore;
pub use slate::SlateKvStore;
