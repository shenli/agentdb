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
const RAFT_LOG_PREFIX: &[u8] = b"raft_log/";
const RAFT_COMMIT_PREFIX: &[u8] = b"raft_commit/";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftLogRecord {
    pub index: u64,
    pub term: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftAppendEntriesRequest {
    pub tablet_id: i64,
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entry: Option<RaftLogRecord>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftAppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

pub trait RaftLogTransport: Send + Sync {
    fn append_entries(
        &self,
        target_node_id: &str,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse>;
}

#[derive(Clone)]
pub struct RaftQuorumReplicatedLog {
    kv: Arc<dyn KvStore + Send + Sync>,
    local_node_id: String,
    voters: Vec<String>,
    term: u64,
    transport: Arc<dyn RaftLogTransport>,
}

impl RaftQuorumReplicatedLog {
    pub fn new(
        kv: Arc<dyn KvStore + Send + Sync>,
        local_node_id: impl Into<String>,
        voters: Vec<String>,
        term: u64,
        transport: Arc<dyn RaftLogTransport>,
    ) -> Result<Self> {
        let local_node_id = local_node_id.into();
        if term == 0 {
            return Err(Error::Other("raft term must be positive".into()));
        }
        if voters.is_empty() {
            return Err(Error::Other("raft voters cannot be empty".into()));
        }
        if !voters.iter().any(|node_id| node_id == &local_node_id) {
            return Err(Error::Other(format!(
                "local node `{local_node_id}` is not a raft voter"
            )));
        }
        Ok(Self {
            kv,
            local_node_id,
            voters,
            term,
            transport,
        })
    }

    pub fn receive_append_entries(
        &self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse> {
        validate_tablet_id(request.tablet_id)?;
        if request.term < self.term {
            return Ok(RaftAppendEntriesResponse {
                term: self.term,
                success: false,
                match_index: max_raft_index(self.kv.as_ref(), request.tablet_id)?,
            });
        }
        let last_index = max_raft_index(self.kv.as_ref(), request.tablet_id)?;
        if request.prev_log_index > last_index {
            return Ok(RaftAppendEntriesResponse {
                term: self.term.max(request.term),
                success: false,
                match_index: last_index,
            });
        }
        if request.prev_log_index > 0 {
            let Some(prev) =
                read_raft_record(self.kv.as_ref(), request.tablet_id, request.prev_log_index)?
            else {
                return Ok(RaftAppendEntriesResponse {
                    term: self.term.max(request.term),
                    success: false,
                    match_index: last_index,
                });
            };
            if prev.term != request.prev_log_term {
                delete_raft_suffix(self.kv.as_ref(), request.tablet_id, request.prev_log_index)?;
                return Ok(RaftAppendEntriesResponse {
                    term: self.term.max(request.term),
                    success: false,
                    match_index: request.prev_log_index.saturating_sub(1),
                });
            }
        }

        let mut match_index = request.prev_log_index;
        if let Some(entry) = request.entry {
            if entry.index != request.prev_log_index + 1 {
                return Ok(RaftAppendEntriesResponse {
                    term: self.term.max(request.term),
                    success: false,
                    match_index,
                });
            }
            if let Some(existing) =
                read_raft_record(self.kv.as_ref(), request.tablet_id, entry.index)?
            {
                if existing.term != entry.term || existing.command != entry.command {
                    delete_raft_suffix(self.kv.as_ref(), request.tablet_id, entry.index - 1)?;
                }
            }
            write_raft_record(self.kv.as_ref(), request.tablet_id, &entry)?;
            match_index = entry.index;
        }

        let local_last_index = max_raft_index(self.kv.as_ref(), request.tablet_id)?;
        let commit_index = request.leader_commit.min(local_last_index);
        if commit_index > read_raft_commit_index(self.kv.as_ref(), request.tablet_id)? {
            write_raft_commit_index(self.kv.as_ref(), request.tablet_id, commit_index)?;
        }

        Ok(RaftAppendEntriesResponse {
            term: self.term.max(request.term),
            success: true,
            match_index: match_index.max(commit_index),
        })
    }

    fn quorum_size(&self) -> usize {
        (self.voters.len() / 2) + 1
    }
}

impl ReplicatedLog for RaftQuorumReplicatedLog {
    fn append(&self, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        validate_tablet_id(tablet_id)?;
        let prev_log_index = max_raft_index(self.kv.as_ref(), tablet_id)?;
        let index = prev_log_index + 1;
        let record = RaftLogRecord {
            index,
            term: self.term,
            command,
        };
        write_raft_record(self.kv.as_ref(), tablet_id, &record)?;

        let leader_commit = read_raft_commit_index(self.kv.as_ref(), tablet_id)?;
        let mut acknowledgements = 1usize;
        for voter in &self.voters {
            if voter == &self.local_node_id {
                continue;
            }
            match self.replicate_through(voter, tablet_id, index, leader_commit) {
                Ok(()) => {
                    acknowledgements += 1;
                }
                Err(err) => {
                    tracing::warn!(
                        target_node_id = %voter,
                        ?err,
                        "raft follower replication failed"
                    );
                }
            }
        }

        if acknowledgements < self.quorum_size() {
            return Err(Error::Other(format!(
                "raft quorum not reached for tablet {tablet_id} index {index}: {acknowledgements}/{} acknowledgements",
                self.voters.len()
            )));
        }

        write_raft_commit_index(self.kv.as_ref(), tablet_id, index)?;

        for voter in &self.voters {
            if voter == &self.local_node_id {
                continue;
            }
            let _ = self.transport.append_entries(
                voter,
                RaftAppendEntriesRequest {
                    tablet_id,
                    term: self.term,
                    leader_id: self.local_node_id.clone(),
                    prev_log_index: index,
                    prev_log_term: self.term,
                    entry: None,
                    leader_commit: index,
                },
            );
        }

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
        let commit_index = read_raft_commit_index(self.kv.as_ref(), tablet_id)?;
        let prefix = raft_log_prefix(tablet_id);
        let mut entries = Vec::new();
        for item in self.kv.scan_prefix(&prefix)? {
            let (key, value) = item?;
            let Some(index) = parse_log_index(&key, &prefix) else {
                continue;
            };
            if index > after_index && index <= commit_index {
                let record = decode_raft_record(index, &value)?;
                entries.push(LogEntry {
                    tablet_id,
                    index,
                    term: record.term,
                    command: record.command,
                });
            }
        }
        entries.sort_by_key(|entry| entry.index);
        entries.truncate(limit);
        Ok(entries)
    }

    fn compact(&self, tablet_id: i64, up_to_index: u64) -> Result<usize> {
        validate_tablet_id(tablet_id)?;
        let commit_index = read_raft_commit_index(self.kv.as_ref(), tablet_id)?;
        if up_to_index > commit_index {
            return Err(Error::Other(format!(
                "cannot compact raft log through {up_to_index}; commit index is {commit_index}"
            )));
        }
        compact_raft_log(self.kv.as_ref(), tablet_id, up_to_index)
    }
}

impl RaftQuorumReplicatedLog {
    fn replicate_through(
        &self,
        voter: &str,
        tablet_id: i64,
        target_index: u64,
        leader_commit: u64,
    ) -> Result<()> {
        let mut next_index = target_index;
        let max_attempts = target_index.saturating_mul(2).saturating_add(2);
        for _ in 0..max_attempts {
            let Some(record) = read_raft_record(self.kv.as_ref(), tablet_id, next_index)? else {
                return Err(Error::Other(format!(
                    "leader is missing raft log entry {next_index} for tablet {tablet_id}"
                )));
            };
            let prev_log_index = next_index.saturating_sub(1);
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                read_raft_record(self.kv.as_ref(), tablet_id, prev_log_index)?
                    .map(|record| record.term)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "leader is missing raft prev log entry {prev_log_index} for tablet {tablet_id}"
                        ))
                    })?
            };
            let response = self.transport.append_entries(
                voter,
                RaftAppendEntriesRequest {
                    tablet_id,
                    term: self.term,
                    leader_id: self.local_node_id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entry: Some(record),
                    leader_commit,
                },
            )?;
            if response.term > self.term {
                return Err(Error::Other(format!(
                    "raft follower {voter} reports newer term {} than local term {}",
                    response.term, self.term
                )));
            }
            if response.success {
                if response.match_index >= target_index {
                    return Ok(());
                }
                next_index = response.match_index.saturating_add(1);
            } else {
                next_index = response.match_index.saturating_add(1).max(1);
            }
            if next_index > target_index {
                return Err(Error::Other(format!(
                    "raft follower {voter} advanced past target index {target_index}"
                )));
            }
        }
        Err(Error::Other(format!(
            "raft follower {voter} did not catch up to tablet {tablet_id} index {target_index}"
        )))
    }
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

fn max_raft_index(kv: &dyn KvStore, tablet_id: i64) -> Result<u64> {
    let prefix = raft_log_prefix(tablet_id);
    let mut max = 0;
    for item in kv.scan_prefix(&prefix)? {
        let (key, _) = item?;
        if let Some(index) = parse_log_index(&key, &prefix) {
            max = max.max(index);
        }
    }
    Ok(max)
}

fn read_raft_record(kv: &dyn KvStore, tablet_id: i64, index: u64) -> Result<Option<RaftLogRecord>> {
    kv.get(&raft_log_key(tablet_id, index))?
        .map(|value| decode_raft_record(index, &value))
        .transpose()
}

fn write_raft_record(kv: &dyn KvStore, tablet_id: i64, record: &RaftLogRecord) -> Result<()> {
    kv.write_batch(vec![WriteOp::Put {
        key: raft_log_key(tablet_id, record.index),
        value: encode_raft_record(record),
    }])
}

fn read_raft_commit_index(kv: &dyn KvStore, tablet_id: i64) -> Result<u64> {
    let Some(bytes) = kv.get(&raft_commit_key(tablet_id))? else {
        return Ok(0);
    };
    let raw: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::Other("invalid raft commit index".into()))?;
    Ok(u64::from_be_bytes(raw))
}

fn write_raft_commit_index(kv: &dyn KvStore, tablet_id: i64, commit_index: u64) -> Result<()> {
    kv.write_batch(vec![WriteOp::Put {
        key: raft_commit_key(tablet_id),
        value: commit_index.to_be_bytes().to_vec(),
    }])
}

fn delete_raft_suffix(kv: &dyn KvStore, tablet_id: i64, keep_through_index: u64) -> Result<usize> {
    let prefix = raft_log_prefix(tablet_id);
    let mut ops = Vec::new();
    for item in kv.scan_prefix(&prefix)? {
        let (key, _) = item?;
        let Some(index) = parse_log_index(&key, &prefix) else {
            continue;
        };
        if index > keep_through_index {
            ops.push(WriteOp::Delete { key });
        }
    }
    let deleted = ops.len();
    if deleted > 0 {
        kv.write_batch(ops)?;
    }
    Ok(deleted)
}

fn compact_raft_log(kv: &dyn KvStore, tablet_id: i64, up_to_index: u64) -> Result<usize> {
    if up_to_index == 0 {
        return Ok(0);
    }
    let prefix = raft_log_prefix(tablet_id);
    let mut ops = Vec::new();
    for item in kv.scan_prefix(&prefix)? {
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
        kv.write_batch(ops)?;
    }
    Ok(deleted)
}

fn encode_raft_record(record: &RaftLogRecord) -> Vec<u8> {
    let mut value = Vec::with_capacity(8 + record.command.len());
    value.extend_from_slice(&record.term.to_be_bytes());
    value.extend_from_slice(&record.command);
    value
}

fn decode_raft_record(index: u64, value: &[u8]) -> Result<RaftLogRecord> {
    if value.len() < 8 {
        return Err(Error::Other("invalid raft log record".into()));
    }
    let term = u64::from_be_bytes(
        value[..8]
            .try_into()
            .map_err(|_| Error::Other("invalid raft log term".into()))?,
    );
    Ok(RaftLogRecord {
        index,
        term,
        command: value[8..].to_vec(),
    })
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

fn raft_log_prefix(tablet_id: i64) -> Vec<u8> {
    format!(
        "{}{:020}/",
        String::from_utf8_lossy(RAFT_LOG_PREFIX),
        tablet_id
    )
    .into_bytes()
}

fn log_key(tablet_id: i64, index: u64) -> Vec<u8> {
    let mut key = log_prefix(tablet_id);
    key.extend_from_slice(format!("{index:020}").as_bytes());
    key
}

fn raft_log_key(tablet_id: i64, index: u64) -> Vec<u8> {
    let mut key = raft_log_prefix(tablet_id);
    key.extend_from_slice(format!("{index:020}").as_bytes());
    key
}

fn raft_commit_key(tablet_id: i64) -> Vec<u8> {
    format!(
        "{}{:020}",
        String::from_utf8_lossy(RAFT_COMMIT_PREFIX),
        tablet_id
    )
    .into_bytes()
}

fn parse_log_index(key: &[u8], prefix: &[u8]) -> Option<u64> {
    let suffix = key.strip_prefix(prefix)?;
    std::str::from_utf8(suffix).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksKvStore;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

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

    #[derive(Default)]
    struct InMemoryRaftTransport {
        nodes: Mutex<HashMap<String, Arc<RaftQuorumReplicatedLog>>>,
        blocked: Mutex<HashSet<String>>,
    }

    impl InMemoryRaftTransport {
        fn insert(&self, node_id: &str, log: Arc<RaftQuorumReplicatedLog>) {
            self.nodes.lock().unwrap().insert(node_id.into(), log);
        }

        fn block(&self, node_id: &str) {
            self.blocked.lock().unwrap().insert(node_id.into());
        }

        fn unblock(&self, node_id: &str) {
            self.blocked.lock().unwrap().remove(node_id);
        }
    }

    impl RaftLogTransport for InMemoryRaftTransport {
        fn append_entries(
            &self,
            target_node_id: &str,
            request: RaftAppendEntriesRequest,
        ) -> Result<RaftAppendEntriesResponse> {
            if self.blocked.lock().unwrap().contains(target_node_id) {
                return Err(Error::Other(format!("node {target_node_id} unreachable")));
            }
            let node = self
                .nodes
                .lock()
                .unwrap()
                .get(target_node_id)
                .cloned()
                .ok_or_else(|| Error::Other(format!("node {target_node_id} not found")))?;
            node.receive_append_entries(request)
        }
    }

    fn raft_node(
        node_id: &str,
        voters: Vec<String>,
        transport: Arc<InMemoryRaftTransport>,
    ) -> Arc<RaftQuorumReplicatedLog> {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = Arc::new(
            RaftQuorumReplicatedLog::new(kv, node_id, voters, 1, transport.clone()).unwrap(),
        );
        // Keep the RocksDB directory alive for the lifetime of the test log.
        std::mem::forget(dir);
        log
    }

    #[test]
    fn raft_quorum_log_commits_after_majority_acknowledges() {
        let transport = Arc::new(InMemoryRaftTransport::default());
        let voters = vec!["n1".into(), "n2".into(), "n3".into()];
        let leader = raft_node("n1", voters.clone(), transport.clone());
        let follower_a = raft_node("n2", voters.clone(), transport.clone());
        let follower_b = raft_node("n3", voters, transport.clone());
        transport.insert("n1", leader.clone());
        transport.insert("n2", follower_a.clone());
        transport.insert("n3", follower_b.clone());

        let commit = leader.append(7, b"command".to_vec()).unwrap();

        assert_eq!(commit.index, 1);
        assert_eq!(
            leader.committed_entries(7, 0, 10).unwrap()[0].command,
            b"command"
        );
        assert_eq!(
            follower_a.committed_entries(7, 0, 10).unwrap()[0].command,
            b"command"
        );
        assert_eq!(
            follower_b.committed_entries(7, 0, 10).unwrap()[0].command,
            b"command"
        );
    }

    #[test]
    fn raft_quorum_log_commits_with_one_failed_follower() {
        let transport = Arc::new(InMemoryRaftTransport::default());
        let voters = vec!["n1".into(), "n2".into(), "n3".into()];
        let leader = raft_node("n1", voters.clone(), transport.clone());
        let follower = raft_node("n2", voters.clone(), transport.clone());
        transport.insert("n1", leader.clone());
        transport.insert("n2", follower.clone());
        transport.block("n3");

        let commit = leader.append(7, b"command".to_vec()).unwrap();

        assert_eq!(commit.index, 1);
        assert_eq!(leader.committed_entries(7, 0, 10).unwrap().len(), 1);
        assert_eq!(follower.committed_entries(7, 0, 10).unwrap().len(), 1);
    }

    #[test]
    fn raft_quorum_log_rejects_commit_without_majority() {
        let transport = Arc::new(InMemoryRaftTransport::default());
        let voters = vec!["n1".into(), "n2".into(), "n3".into()];
        let leader = raft_node("n1", voters, transport.clone());
        transport.insert("n1", leader.clone());
        transport.block("n2");
        transport.block("n3");

        let err = leader.append(7, b"command".to_vec()).unwrap_err();

        assert!(err.to_string().contains("raft quorum not reached"));
        assert!(leader.committed_entries(7, 0, 10).unwrap().is_empty());
    }

    #[test]
    fn raft_quorum_log_backfills_stale_follower_before_commit() {
        let transport = Arc::new(InMemoryRaftTransport::default());
        let voters = vec!["n1".into(), "n2".into(), "n3".into()];
        let leader = raft_node("n1", voters.clone(), transport.clone());
        let stale_follower = raft_node("n2", voters.clone(), transport.clone());
        let other_follower = raft_node("n3", voters, transport.clone());
        transport.insert("n1", leader.clone());
        transport.insert("n2", stale_follower.clone());
        transport.insert("n3", other_follower.clone());

        transport.block("n2");
        leader.append(7, b"first".to_vec()).unwrap();
        assert!(stale_follower
            .committed_entries(7, 0, 10)
            .unwrap()
            .is_empty());

        transport.unblock("n2");
        transport.block("n3");
        let commit = leader.append(7, b"second".to_vec()).unwrap();

        assert_eq!(commit.index, 2);
        let entries = stale_follower.committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].command, b"first");
        assert_eq!(entries[1].command, b"second");
    }

    #[test]
    fn raft_quorum_log_compacts_only_committed_entries() {
        let transport = Arc::new(InMemoryRaftTransport::default());
        let voters = vec!["n1".into()];
        let leader = raft_node("n1", voters, transport.clone());
        transport.insert("n1", leader.clone());

        leader.append(7, b"first".to_vec()).unwrap();
        leader.append(7, b"second".to_vec()).unwrap();

        assert_eq!(leader.compact(7, 1).unwrap(), 1);
        let entries = leader.committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 2);
    }
}
