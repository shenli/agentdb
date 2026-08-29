//! Replicated log boundary for tablet writes.
//!
//! The production implementation should be backed by `raft-rs` plus
//! `raft-engine`. This module provides the interface and a single-node durable
//! implementation so the state machine can be wired through a log boundary
//! before distributed consensus lands.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{Error, KvStore, Result, WriteOp};

const LOG_PREFIX: &[u8] = b"tablet_log/";
const LOG_NEXT_PREFIX: &[u8] = b"tablet_log_next/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub tablet_id: i64,
    pub index: u64,
    pub term: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCommit {
    pub tablet_id: i64,
    pub index: u64,
    pub term: u64,
}

pub trait ReplicatedLog: Send + Sync {
    /// Append a command and return once the entry is committed.
    ///
    /// In the single-node implementation, append and commit are the same
    /// operation. A future Raft implementation should return only after quorum
    /// commit.
    fn append(&self, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit>;

    /// Read committed entries after `after_index`, ordered by log index.
    fn committed_entries(
        &self,
        tablet_id: i64,
        after_index: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>>;

    /// Delete committed log entries through `up_to_index`, inclusive.
    ///
    /// Callers should only compact after a snapshot covering `up_to_index` is
    /// durable enough for recovery/catch-up.
    fn compact(&self, tablet_id: i64, up_to_index: u64) -> Result<usize>;
}

#[derive(Clone)]
pub struct SingleNodeReplicatedLog {
    kv: Arc<dyn KvStore + Send + Sync>,
    term: u64,
    next_index_by_tablet: Arc<Mutex<BTreeMap<i64, u64>>>,
}

impl SingleNodeReplicatedLog {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self {
            kv,
            term: 1,
            next_index_by_tablet: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_term(kv: Arc<dyn KvStore + Send + Sync>, term: u64) -> Self {
        Self {
            kv,
            term,
            next_index_by_tablet: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl ReplicatedLog for SingleNodeReplicatedLog {
    fn append(&self, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        validate_tablet_id(tablet_id)?;
        let mut next_by_tablet = self
            .next_index_by_tablet
            .lock()
            .expect("single-node log next-index lock poisoned");
        let index = match next_by_tablet.get(&tablet_id).copied() {
            Some(next_index) => next_index,
            None => load_next_log_index(self.kv.as_ref(), tablet_id)?,
        };
        let next_index = index.saturating_add(1);
        self.kv.write_batch(vec![
            WriteOp::Put {
                key: log_key(tablet_id, index),
                value: command,
            },
            WriteOp::Put {
                key: log_next_key(tablet_id),
                value: next_index.to_be_bytes().to_vec(),
            },
        ])?;
        next_by_tablet.insert(tablet_id, next_index);
        Ok(LogCommit {
            tablet_id,
            index,
            term: self.term,
        })
    }

    fn committed_entries(
        &self,
        tablet_id: i64,
        after_index: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        validate_tablet_id(tablet_id)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = log_prefix(tablet_id);
        let start = log_key(tablet_id, after_index.saturating_add(1));
        let mut entries = Vec::new();
        for item in self.kv.scan_prefix_from(&prefix, &start)? {
            let (key, command) = item?;
            let Some(index) = parse_log_index(&key, &prefix) else {
                continue;
            };
            entries.push(LogEntry {
                tablet_id,
                index,
                term: self.term,
                command,
            });
            if entries.len() >= limit {
                break;
            }
        }
        Ok(entries)
    }

    fn compact(&self, tablet_id: i64, up_to_index: u64) -> Result<usize> {
        validate_tablet_id(tablet_id)?;
        if up_to_index == 0 {
            return Ok(0);
        }
        let prefix = log_prefix(tablet_id);
        let mut ops = Vec::new();
        for item in self.kv.scan_prefix(&prefix)? {
            let (key, _) = item?;
            let Some(index) = parse_log_index(&key, &prefix) else {
                continue;
            };
            if index <= up_to_index {
                ops.push(WriteOp::Delete { key });
            }
        }
        let deleted = ops.len();
        if deleted > 0 {
            self.kv.write_batch(ops)?;
        }
        Ok(deleted)
    }
}

fn validate_tablet_id(tablet_id: i64) -> Result<()> {
    if tablet_id < 0 {
        return Err(Error::Other("tablet_id must be non-negative".into()));
    }
    Ok(())
}

fn max_index(kv: &dyn KvStore, tablet_id: i64) -> Result<u64> {
    let prefix = log_prefix(tablet_id);
    let mut max = 0;
    for item in kv.scan_prefix(&prefix)? {
        let (key, _) = item?;
        if let Some(index) = parse_log_index(&key, &prefix) {
            max = max.max(index);
        }
    }
    Ok(max)
}

fn load_next_log_index(kv: &dyn KvStore, tablet_id: i64) -> Result<u64> {
    if let Some(bytes) = kv.get(&log_next_key(tablet_id))? {
        let raw: [u8; 8] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| Error::Other("invalid single-node log next index".into()))?;
        return Ok(u64::from_be_bytes(raw));
    }
    Ok(max_index(kv, tablet_id)?.saturating_add(1))
}

fn log_prefix(tablet_id: i64) -> Vec<u8> {
    format!("{}{:020}/", String::from_utf8_lossy(LOG_PREFIX), tablet_id).into_bytes()
}

fn log_next_key(tablet_id: i64) -> Vec<u8> {
    format!(
        "{}{:020}",
        String::from_utf8_lossy(LOG_NEXT_PREFIX),
        tablet_id
    )
    .into_bytes()
}

fn log_key(tablet_id: i64, index: u64) -> Vec<u8> {
    let mut key = log_prefix(tablet_id);
    key.extend_from_slice(format!("{index:020}").as_bytes());
    key
}

fn parse_log_index(key: &[u8], prefix: &[u8]) -> Option<u64> {
    let suffix = key.strip_prefix(prefix)?;
    std::str::from_utf8(suffix).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksKvStore;

    #[test]
    fn single_node_log_appends_and_reads_committed_entries() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = SingleNodeReplicatedLog::new(kv);

        let first = log.append(7, b"first".to_vec()).unwrap();
        let second = log.append(7, b"second".to_vec()).unwrap();

        assert_eq!(first.index, 1);
        assert_eq!(second.index, 2);

        let entries = log.committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].command, b"first");
        assert_eq!(entries[1].command, b"second");
    }

    #[test]
    fn single_node_log_filters_by_index_and_limit() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = SingleNodeReplicatedLog::new(kv);

        log.append(7, b"first".to_vec()).unwrap();
        log.append(7, b"second".to_vec()).unwrap();
        log.append(7, b"third".to_vec()).unwrap();

        let entries = log.committed_entries(7, 1, 1).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 2);
        assert_eq!(entries[0].command, b"second");
    }

    #[test]
    fn single_node_log_is_scoped_by_tablet() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = SingleNodeReplicatedLog::new(kv);

        log.append(7, b"seven".to_vec()).unwrap();
        log.append(8, b"eight".to_vec()).unwrap();

        let entries = log.committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command, b"seven");
    }

    #[test]
    fn single_node_log_compacts_committed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = SingleNodeReplicatedLog::new(kv);

        log.append(7, b"first".to_vec()).unwrap();
        log.append(7, b"second".to_vec()).unwrap();
        log.append(7, b"third".to_vec()).unwrap();

        assert_eq!(log.compact(7, 2).unwrap(), 2);

        let entries = log.committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 3);
        assert_eq!(entries[0].command, b"third");
    }

    #[test]
    fn single_node_log_persists_next_index_across_compaction_restart() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = SingleNodeReplicatedLog::new(kv.clone());

        log.append(7, b"first".to_vec()).unwrap();
        log.append(7, b"second".to_vec()).unwrap();
        assert_eq!(log.compact(7, 2).unwrap(), 2);

        let restarted = SingleNodeReplicatedLog::new(kv);
        let third = restarted.append(7, b"third".to_vec()).unwrap();

        assert_eq!(third.index, 3);
    }

    #[test]
    fn single_node_log_compaction_is_scoped_by_tablet() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = SingleNodeReplicatedLog::new(kv);

        log.append(7, b"seven".to_vec()).unwrap();
        log.append(8, b"eight".to_vec()).unwrap();

        assert_eq!(log.compact(7, 1).unwrap(), 1);

        assert!(log.committed_entries(7, 0, 10).unwrap().is_empty());
        let entries = log.committed_entries(8, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command, b"eight");
    }
}
