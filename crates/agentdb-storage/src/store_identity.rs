//! Durable store identity — a fail-closed marker written into a node's
//! data directory on first boot and verified on every subsequent boot.
//!
//! A data directory must never be silently reused by the wrong node or the
//! wrong cluster: a swapped disk, a copied directory, or a config typo that
//! points a fresh node at another node's storage can corrupt a cluster in
//! ways that are hard to diagnose after the fact. This module makes those
//! mismatches loud startup failures (see
//! `docs/design/distributed-safety-hardening.md` §3).
//!
//! The identity file is `<data_dir>/IDENTITY.json`. It is self-describing
//! and integrity-checked: a truncated or edited file fails closed rather
//! than being trusted.
//!
//! `store_incarnation` is a random id minted once, when the directory is
//! first initialized. It identifies the physical store, not the logical
//! node — rebuilding a node onto a fresh disk mints a new incarnation.
//! This module owns the local, self-contained half (cluster id, node id,
//! and integrity); in cluster mode the incarnation is additionally admitted
//! against the control-plane registry in `agentdb_meta::store_admission`,
//! which is what catches a retired or displaced disk trying to rejoin.

use std::path::{Path, PathBuf};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

/// Bumped only on an incompatible change to the identity file layout.
pub const STORE_IDENTITY_FORMAT_VERSION: u32 = 1;

const IDENTITY_FILE_NAME: &str = "IDENTITY.json";

/// The identity a booting node expects its data directory to carry.
#[derive(Debug, Clone)]
pub struct ExpectedIdentity {
    pub node_id: String,
    pub cell_id: String,
    /// Cluster id from configuration, if the deployment sets one. When
    /// `None`, first boot mints and persists one, and reboots accept
    /// whatever the file already carries (single-node / unconfigured).
    pub cluster_id: Option<String>,
}

/// The persisted contents of `<data_dir>/IDENTITY.json`. `digest` covers
/// every other field; a mismatch means the file was truncated or edited.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreIdentity {
    pub format_version: u32,
    pub cluster_id: String,
    pub node_id: String,
    pub cell_id: String,
    pub store_incarnation: String,
    pub created_at_ms: u64,
    pub digest: String,
}

impl StoreIdentity {
    fn compute_digest(&self) -> String {
        // Canonical, field-ordered bytes — never serde_json of self, whose
        // field order/whitespace could drift and silently invalidate old
        // files.
        let mut hasher = Sha256::new();
        hasher.update(self.format_version.to_be_bytes());
        hasher.update([0]);
        hasher.update(self.cluster_id.as_bytes());
        hasher.update([0]);
        hasher.update(self.node_id.as_bytes());
        hasher.update([0]);
        hasher.update(self.cell_id.as_bytes());
        hasher.update([0]);
        hasher.update(self.store_incarnation.as_bytes());
        hasher.update([0]);
        hasher.update(self.created_at_ms.to_be_bytes());
        hex(&hasher.finalize())
    }

    fn integrity_ok(&self) -> bool {
        // Constant-time-ish equality is unnecessary here (not a secret);
        // plain comparison of the hex digests is fine.
        self.compute_digest() == self.digest
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn mint_incarnation() -> String {
    let mut raw = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut raw);
    hex(&raw)
}

pub fn identity_path(data_dir: &Path) -> PathBuf {
    data_dir.join(IDENTITY_FILE_NAME)
}

const CLUSTER_WATERMARK_FILE_NAME: &str = "CLUSTER_WATERMARK.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterWatermark {
    bootstrap_generation: i64,
}

pub fn cluster_watermark_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CLUSTER_WATERMARK_FILE_NAME)
}

/// The highest cluster bootstrap generation this store has ever observed, or
/// `None` if it has never recorded one. Used by the control-plane cluster-root
/// cross-check to detect a metadata database that was restored to an *older*
/// generation than this node already saw (a rollback / split-brain hazard).
/// Unlike `IDENTITY.json`, this file is mutable and not integrity-sealed: it is
/// a monotonic hint, and a corrupt/absent file simply reads as `None` (the
/// check then trusts the control plane, exactly as a first boot would).
pub fn read_cluster_watermark(data_dir: &Path) -> Option<i64> {
    let bytes = std::fs::read(cluster_watermark_path(data_dir)).ok()?;
    serde_json::from_slice::<ClusterWatermark>(&bytes)
        .ok()
        .map(|watermark| watermark.bootstrap_generation)
}

/// Persist an observed cluster bootstrap generation. Monotonic: a value at or
/// below the current watermark is ignored, so a transient misread can never
/// lower the watermark and erase rollback detection.
pub fn record_cluster_watermark(data_dir: &Path, generation: i64) -> Result<()> {
    if let Some(current) = read_cluster_watermark(data_dir) {
        if generation <= current {
            return Ok(());
        }
    }
    let bytes = serde_json::to_vec_pretty(&ClusterWatermark {
        bootstrap_generation: generation,
    })
    .map_err(|error| Error::Other(format!("serialize cluster watermark: {error}")))?;
    let path = cluster_watermark_path(data_dir);
    std::fs::write(&path, bytes)
        .map_err(|error| Error::Other(format!("write cluster watermark {}: {error}", path.display())))
}

/// Load and verify the store identity, or initialize it on first boot.
///
/// First boot (no file): mint an incarnation, adopt the expected cluster
/// id (or mint one when unconfigured), write the file, return it.
///
/// Subsequent boot: read and integrity-check the file, then fail closed on
/// any mismatch — node id, a configured cluster id, or an unknown format
/// version. `now_ms` is injected so callers control the clock (and tests
/// stay deterministic).
pub fn load_or_init(
    data_dir: &Path,
    expected: &ExpectedIdentity,
    now_ms: u64,
) -> Result<StoreIdentity> {
    let path = identity_path(data_dir);
    if !path.exists() {
        let cluster_id = expected
            .cluster_id
            .clone()
            .unwrap_or_else(|| format!("cluster-{}", mint_incarnation()));
        let mut identity = StoreIdentity {
            format_version: STORE_IDENTITY_FORMAT_VERSION,
            cluster_id,
            node_id: expected.node_id.clone(),
            cell_id: expected.cell_id.clone(),
            store_incarnation: mint_incarnation(),
            created_at_ms: now_ms,
            digest: String::new(),
        };
        identity.digest = identity.compute_digest();
        let bytes = serde_json::to_vec_pretty(&identity)
            .map_err(|error| Error::Other(format!("serialize store identity: {error}")))?;
        std::fs::write(&path, bytes).map_err(|error| {
            Error::Other(format!("write store identity {}: {error}", path.display()))
        })?;
        return Ok(identity);
    }

    let bytes = std::fs::read(&path).map_err(|error| {
        Error::Other(format!("read store identity {}: {error}", path.display()))
    })?;
    let identity: StoreIdentity = serde_json::from_slice(&bytes).map_err(|error| {
        Error::Other(format!(
            "store identity {} is malformed or truncated: {error}",
            path.display()
        ))
    })?;

    if identity.format_version != STORE_IDENTITY_FORMAT_VERSION {
        return Err(Error::Other(format!(
            "store identity {} has unsupported format version {} (this build expects {})",
            path.display(),
            identity.format_version,
            STORE_IDENTITY_FORMAT_VERSION
        )));
    }
    if !identity.integrity_ok() {
        return Err(Error::Other(format!(
            "store identity {} failed its integrity digest; the file is corrupt or was edited",
            path.display()
        )));
    }
    if identity.node_id != expected.node_id {
        return Err(Error::Other(format!(
            "data directory belongs to node {:?}, but this process is node {:?}; \
             refusing to reuse another node's store (move the data directory aside \
             or fix the node id)",
            identity.node_id, expected.node_id
        )));
    }
    if let Some(expected_cluster) = expected.cluster_id.as_ref() {
        if &identity.cluster_id != expected_cluster {
            return Err(Error::Other(format!(
                "data directory belongs to cluster {:?}, but this process is configured for \
                 cluster {:?}; refusing to join the wrong cluster",
                identity.cluster_id, expected_cluster
            )));
        }
    }

    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected(node: &str) -> ExpectedIdentity {
        ExpectedIdentity {
            node_id: node.to_string(),
            cell_id: "cell-a".to_string(),
            cluster_id: Some("cluster-1".to_string()),
        }
    }

    #[test]
    fn first_boot_mints_and_reboot_reloads_same_identity() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_init(dir.path(), &expected("node-a"), 1_000).unwrap();
        assert_eq!(first.node_id, "node-a");
        assert_eq!(first.cluster_id, "cluster-1");
        assert!(!first.store_incarnation.is_empty());

        // Reboot: same identity, incarnation stable, integrity holds.
        let second = load_or_init(dir.path(), &expected("node-a"), 9_999).unwrap();
        assert_eq!(first, second);
        assert_eq!(second.created_at_ms, 1_000, "created_at is stable");
    }

    #[test]
    fn wrong_node_id_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        load_or_init(dir.path(), &expected("node-a"), 1_000).unwrap();
        let err = load_or_init(dir.path(), &expected("node-b"), 2_000).unwrap_err();
        assert!(format!("{err:?}").contains("belongs to node"));
    }

    #[test]
    fn wrong_cluster_id_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        load_or_init(dir.path(), &expected("node-a"), 1_000).unwrap();
        let mut other = expected("node-a");
        other.cluster_id = Some("cluster-2".to_string());
        let err = load_or_init(dir.path(), &other, 2_000).unwrap_err();
        assert!(format!("{err:?}").contains("wrong cluster"));
    }

    #[test]
    fn corrupted_file_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        load_or_init(dir.path(), &expected("node-a"), 1_000).unwrap();
        // Tamper with a field without recomputing the digest.
        let path = identity_path(dir.path());
        let mut identity: StoreIdentity =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        identity.cell_id = "cell-tampered".to_string();
        std::fs::write(&path, serde_json::to_vec(&identity).unwrap()).unwrap();
        let err = load_or_init(dir.path(), &expected("node-a"), 2_000).unwrap_err();
        assert!(format!("{err:?}").contains("integrity"));
    }

    #[test]
    fn truncated_file_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        load_or_init(dir.path(), &expected("node-a"), 1_000).unwrap();
        std::fs::write(identity_path(dir.path()), b"{ not valid json").unwrap();
        let err = load_or_init(dir.path(), &expected("node-a"), 2_000).unwrap_err();
        assert!(format!("{err:?}").contains("malformed or truncated"));
    }

    #[test]
    fn cluster_watermark_round_trips_and_only_advances() {
        let dir = tempfile::tempdir().unwrap();
        // Never recorded → None.
        assert_eq!(read_cluster_watermark(dir.path()), None);
        record_cluster_watermark(dir.path(), 3).unwrap();
        assert_eq!(read_cluster_watermark(dir.path()), Some(3));
        // A higher generation advances it.
        record_cluster_watermark(dir.path(), 5).unwrap();
        assert_eq!(read_cluster_watermark(dir.path()), Some(5));
        // A lower or equal generation is ignored (monotonic).
        record_cluster_watermark(dir.path(), 4).unwrap();
        record_cluster_watermark(dir.path(), 5).unwrap();
        assert_eq!(read_cluster_watermark(dir.path()), Some(5));
    }

    #[test]
    fn unconfigured_cluster_id_is_minted_and_accepted_on_reboot() {
        let dir = tempfile::tempdir().unwrap();
        let mut exp = expected("node-a");
        exp.cluster_id = None;
        let first = load_or_init(dir.path(), &exp, 1_000).unwrap();
        assert!(first.cluster_id.starts_with("cluster-"));
        // Reboot with still-unconfigured cluster id accepts the minted one.
        let second = load_or_init(dir.path(), &exp, 2_000).unwrap();
        assert_eq!(first.cluster_id, second.cluster_id);
    }
}
