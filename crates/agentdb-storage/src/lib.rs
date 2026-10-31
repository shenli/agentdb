//! AgentDB storage layer.
//!
//! Provides the `KvStore` trait and a RocksDB-backed implementation.
//! Keys are produced via the helpers in [`keys`] (tenant-first logical
//! string keys) to guarantee tenant-scoped prefix scans.

pub mod content;
pub mod content_crypto;
pub mod crypto;
pub mod error;
pub mod keys;
pub mod kms;
pub mod kv;
pub mod metadata_crypto;
pub mod raft_rs_log;
pub mod recovery;
pub mod replicated_log;
pub mod rocks;
pub mod store_identity;
pub mod tablet_store;

pub use content::{
    sha256_hex, AsyncMirrorContentBackend, AsyncMirrorStats, ContentBackend, ContentListing,
    ContentObject, InMemoryContentBackend, LatencyContentBackend, LocalFsContentBackend,
    MirrorCacheSweepReport, MirrorReconcileReport, ObjectStoreContentBackend, PutResult,
    MIRROR_BACKPRESSURE_MARKER,
};
pub use content_crypto::{
    ContentCryptoAdapter, ContentCryptoContext, IdentityContentCryptoAdapter,
};
pub use crypto::{
    envelope_dek_version, is_envelope_v2, AeadTenantKeyring, CatalogKeyProvider,
    CommandKekProvider, CryptoConfig, CryptoContext, CryptoError, CryptoMode, Dek, DekCacheConfig,
    DekLease, DekRecord, DekStatus, DisabledEnvelopeCrypto, EnvelopeCrypto, EnvelopeParts,
    KekProvider, KeyCatalog, KeyMetadata, KeyProvider, KeyPurpose, LocalKekProvider,
    LocalKeyProvider, Suite, WrappedDek,
};
pub use error::{Error, Result};
pub use kms::{
    AllowAllKmsKeyResolver, KeyOperation, KeyResolution, KmsError, KmsKeyResolver, KmsResult,
    StaticKeyState, StaticKmsKeyResolver,
};
pub use kv::{
    available_disk_bytes, InMemoryKvStore, KvIter, KvSnapshot, KvStore, KvStoreMetrics, WriteOp,
};
pub use metadata_crypto::{
    IdentityMetadataCryptoAdapter, MetadataCryptoAdapter, MetadataCryptoContext, MetadataObjectKind,
};
pub use raft_rs_log::{
    stable_raft_node_id, RaftRsNodeConfig, RaftRsNodeRole, RaftRsReplicaStatus,
    RaftRsReplicatedLog, RaftRsSharedGroup, RaftRsSnapshotTransfer, RaftRsTransport,
    RaftRsWireMessage,
};
pub use recovery::{
    export_recovery_manifest, import_recovery_manifest, read_recovery_manifest_header,
    RecoveryImportStats, RecoveryManifestHeader, RecoveryObjectStore,
    RECOVERY_MANIFEST_FORMAT_VERSION,
};
pub use replicated_log::{LogCommit, LogEntry, ReplicatedLog, SingleNodeReplicatedLog};
pub use rocks::{rocksdb_sync_writes_enabled, RocksKvStore, RocksKvStoreConfig};
pub use tablet_store::{
    summarize_checkpoint_dir, GroupedRocksDbTabletStore, PrefixScopedTabletStore, TabletKvStore,
    TabletStoreCheckpointSummary, TabletStoreDomainMetrics, TabletStoreLifecycle,
    TabletStoreManifest,
};
