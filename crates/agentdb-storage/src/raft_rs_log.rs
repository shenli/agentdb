//! `raft-rs` backed replicated log for tablet writes.
//!
//! This module uses TiKV's `raft` crate for consensus and TiKV's
//! `raft-engine` crate for the durable local Raft log. The transport in this
//! first implementation is an in-process shared group so tests and single
//! binary deployments can exercise real Raft quorum behavior without inventing
//! another consensus protocol. A production network transport can reuse the
//! same Ready/message processing loop.

use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use prost::Message as ProstMessage;
use raft::{
    eraftpb::{Entry, EntryType, HardState, Message},
    raw_node::RawNode,
    storage::MemStorage,
    Config, StateRole,
};
use raft_engine::{Config as RaftEngineConfig, Engine as RaftEngine, LogBatch};
use sha2::{Digest, Sha256};

use crate::{Error, LogCommit, LogEntry, ReplicatedLog, Result};

const HARD_STATE_KEY: &[u8] = b"hard_state";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftRsNodeConfig {
    pub node_id: u64,
    pub role: RaftRsNodeRole,
    pub raft_engine_dir: PathBuf,
}

impl RaftRsNodeConfig {
    pub fn from_control_plane_replica(
        tablet_id: i64,
        node_id: &str,
        role: &str,
        raft_engine_root: impl AsRef<Path>,
    ) -> Result<Self> {
        validate_raft_tablet_id(tablet_id)?;
        let raft_node_id = stable_raft_node_id(node_id)?;
        Ok(Self {
            node_id: raft_node_id,
            role: RaftRsNodeRole::from_replica_role(role)?,
            raft_engine_dir: raft_engine_root
                .as_ref()
                .join(format!("tablet-{tablet_id}"))
                .join(sanitize_path_component(node_id)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRsNodeRole {
    Voter,
    Learner,
}

impl RaftRsNodeRole {
    pub fn from_replica_role(role: &str) -> Result<Self> {
        match role {
            "voter" => Ok(Self::Voter),
            "learner" => Ok(Self::Learner),
            other => Err(Error::Other(format!(
                "tablet replica role `{other}` cannot be used as a raft-rs peer"
            ))),
        }
    }
}

pub fn stable_raft_node_id(node_id: &str) -> Result<u64> {
    if node_id.trim().is_empty() {
        return Err(Error::Other("raft-rs node_id cannot be empty".into()));
    }
    if let Ok(parsed) = node_id.parse::<u64>() {
        if parsed != 0 {
            return Ok(parsed);
        }
    }
    let digest = Sha256::digest(node_id.as_bytes());
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&digest[..8]);
    Ok(u64::from_be_bytes(raw).max(1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftRsReplicaStatus {
    pub node_id: u64,
    pub role: String,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
}

#[derive(Clone)]
pub struct RaftRsReplicatedLog {
    group: RaftRsSharedGroup,
    node_id: u64,
}

impl RaftRsReplicatedLog {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
}

impl ReplicatedLog for RaftRsReplicatedLog {
    fn append(&self, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        self.group.append(self.node_id, tablet_id, command)
    }

    fn committed_entries(
        &self,
        tablet_id: i64,
        after_index: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.group
            .committed_entries(self.node_id, tablet_id, after_index, limit)
    }

    fn compact(&self, tablet_id: i64, up_to_index: u64) -> Result<usize> {
        self.group.compact(self.node_id, tablet_id, up_to_index)
    }
}

#[derive(Clone)]
pub struct RaftRsSharedGroup {
    inner: Arc<Mutex<RaftRsGroup>>,
}

impl RaftRsSharedGroup {
    pub fn open(
        tablet_id: i64,
        configs: impl IntoIterator<Item = RaftRsNodeConfig>,
    ) -> Result<(Self, Vec<Arc<RaftRsReplicatedLog>>)> {
        validate_raft_tablet_id(tablet_id)?;
        let configs: Vec<_> = configs.into_iter().collect();
        if configs.is_empty() {
            return Err(Error::Other(
                "raft-rs group requires at least one node".into(),
            ));
        }
        let mut seen = BTreeMap::new();
        for config in &configs {
            if config.node_id == 0 {
                return Err(Error::Other("raft-rs node id must be positive".into()));
            }
            if seen.insert(config.node_id, ()).is_some() {
                return Err(Error::Other(format!(
                    "duplicate raft-rs node id {}",
                    config.node_id
                )));
            }
        }
        let voters: Vec<u64> = configs
            .iter()
            .filter(|config| config.role == RaftRsNodeRole::Voter)
            .map(|config| config.node_id)
            .collect();
        let learners: Vec<u64> = configs
            .iter()
            .filter(|config| config.role == RaftRsNodeRole::Learner)
            .map(|config| config.node_id)
            .collect();
        if voters.is_empty() {
            return Err(Error::Other(
                "raft-rs group requires at least one voter".into(),
            ));
        }
        let group = Self {
            inner: Arc::new(Mutex::new(RaftRsGroup::open(
                tablet_id, configs, voters, learners,
            )?)),
        };
        let handles = group
            .inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .node_ids()
            .into_iter()
            .map(|node_id| {
                Arc::new(RaftRsReplicatedLog {
                    group: group.clone(),
                    node_id,
                })
            })
            .collect();
        Ok((group, handles))
    }

    pub fn elect(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .elect(node_id)
    }

    pub fn status(&self) -> Vec<RaftRsReplicaStatus> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .status()
    }

    fn append(&self, node_id: u64, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .append(node_id, tablet_id, command)
    }

    fn committed_entries(
        &self,
        node_id: u64,
        tablet_id: i64,
        after_index: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .committed_entries(node_id, tablet_id, after_index, limit)
    }

    fn compact(&self, node_id: u64, tablet_id: i64, up_to_index: u64) -> Result<usize> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .compact(node_id, tablet_id, up_to_index)
    }
}

struct RaftRsGroup {
    tablet_id: i64,
    nodes: BTreeMap<u64, RaftRsNode>,
    messages: VecDeque<Message>,
}

impl RaftRsGroup {
    fn open(
        tablet_id: i64,
        configs: Vec<RaftRsNodeConfig>,
        voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Result<Self> {
        let raft_group_id = tablet_id as u64;
        let mut nodes = BTreeMap::new();
        for config in configs {
            let node = RaftRsNode::open(raft_group_id, &voters, &learners, config)?;
            nodes.insert(node.node_id, node);
        }
        let mut group = Self {
            tablet_id,
            nodes,
            messages: VecDeque::new(),
        };
        group.drive_until_idle()?;
        Ok(group)
    }

    fn node_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    fn elect(&mut self, node_id: u64) -> Result<()> {
        self.node_mut(node_id)?
            .raw
            .campaign()
            .map_err(map_raft_error)?;
        self.drive_until_idle()?;
        if self.node(node_id)?.raw.status().ss.raft_state != StateRole::Leader {
            return Err(Error::Other(format!(
                "raft-rs node {node_id} did not become leader"
            )));
        }
        Ok(())
    }

    fn append(&mut self, node_id: u64, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        if tablet_id != self.tablet_id {
            return Err(Error::Other(format!(
                "raft-rs log opened for tablet {}, got tablet {tablet_id}",
                self.tablet_id
            )));
        }
        if self.node(node_id)?.raw.status().ss.raft_state != StateRole::Leader {
            self.elect(node_id)?;
        }
        self.node_mut(node_id)?
            .raw
            .propose(Vec::new(), command)
            .map_err(map_raft_error)?;
        self.drive_until_idle()?;

        let leader = self.node(node_id)?;
        let commit_index = leader.last_applied_index;
        if commit_index == 0 {
            return Err(Error::Other(format!(
                "raft-rs command did not commit for tablet {tablet_id}"
            )));
        }
        let term = leader
            .committed
            .iter()
            .find(|entry| entry.index == commit_index)
            .map(|entry| entry.term)
            .unwrap_or_else(|| leader.raw.status().hs.term);
        Ok(LogCommit {
            tablet_id,
            index: commit_index,
            term,
        })
    }

    fn committed_entries(
        &self,
        node_id: u64,
        tablet_id: i64,
        after_index: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        if tablet_id != self.tablet_id {
            return Err(Error::Other(format!(
                "raft-rs log opened for tablet {}, got tablet {tablet_id}",
                self.tablet_id
            )));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut entries: Vec<_> = self
            .node(node_id)?
            .committed
            .iter()
            .filter(|entry| entry.index > after_index)
            .cloned()
            .collect();
        entries.truncate(limit);
        Ok(entries)
    }

    fn compact(&mut self, node_id: u64, tablet_id: i64, up_to_index: u64) -> Result<usize> {
        if tablet_id != self.tablet_id {
            return Err(Error::Other(format!(
                "raft-rs log opened for tablet {}, got tablet {tablet_id}",
                self.tablet_id
            )));
        }
        let node = self.node_mut(node_id)?;
        node.storage
            .wl()
            .compact(up_to_index.saturating_add(1))
            .map_err(map_raft_error)?;
        node.compact_engine_entries(up_to_index)
    }

    fn status(&self) -> Vec<RaftRsReplicaStatus> {
        self.nodes
            .values()
            .map(|node| {
                let status = node.raw.status();
                RaftRsReplicaStatus {
                    node_id: node.node_id,
                    role: format!("{:?}", status.ss.raft_state),
                    leader_id: status.ss.leader_id,
                    term: status.hs.term,
                    commit_index: status.hs.commit,
                    applied_index: node.last_applied_index,
                }
            })
            .collect()
    }

    fn drive_until_idle(&mut self) -> Result<()> {
        for _ in 0..10_000 {
            let mut progressed = false;
            let node_ids = self.node_ids();
            for node_id in node_ids {
                progressed |= self.process_ready(node_id)?;
            }
            while let Some(message) = self.messages.pop_front() {
                let to = message.to;
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.raw.step(message).map_err(map_raft_error)?;
                    progressed = true;
                }
            }
            if !progressed {
                return Ok(());
            }
        }
        Err(Error::Other("raft-rs group did not quiesce".into()))
    }

    fn process_ready(&mut self, node_id: u64) -> Result<bool> {
        let tablet_id = self.tablet_id;
        let node = self.node_mut(node_id)?;
        if !node.raw.has_ready() {
            return Ok(false);
        }

        let mut ready = node.raw.ready();
        let entries = ready.entries().clone();
        let hard_state = ready.hs().cloned();
        node.persist_ready(&entries, hard_state.as_ref(), ready.must_sync())?;

        {
            let mut storage = node.storage.wl();
            if let Some(hs) = hard_state {
                storage.set_hardstate(hs);
            }
            storage.append(&entries).map_err(map_raft_error)?;
        }

        let mut outbound = ready.take_messages();
        outbound.extend(ready.take_persisted_messages());
        node.apply_committed(ready.take_committed_entries(), tablet_id)?;
        let mut light = node.raw.advance(ready);
        outbound.extend(light.take_messages());
        node.apply_committed(light.take_committed_entries(), tablet_id)?;
        self.messages.extend(outbound);
        Ok(true)
    }

    fn node(&self, node_id: u64) -> Result<&RaftRsNode> {
        self.nodes
            .get(&node_id)
            .ok_or_else(|| Error::Other(format!("raft-rs node {node_id} not found")))
    }

    fn node_mut(&mut self, node_id: u64) -> Result<&mut RaftRsNode> {
        self.nodes
            .get_mut(&node_id)
            .ok_or_else(|| Error::Other(format!("raft-rs node {node_id} not found")))
    }
}

struct RaftRsNode {
    node_id: u64,
    raft_group_id: u64,
    raw: RawNode<MemStorage>,
    storage: MemStorage,
    engine: RaftEngine,
    committed: Vec<LogEntry>,
    last_applied_index: u64,
}

impl RaftRsNode {
    fn open(
        raft_group_id: u64,
        voters: &[u64],
        learners: &[u64],
        config: RaftRsNodeConfig,
    ) -> Result<Self> {
        std::fs::create_dir_all(&config.raft_engine_dir).map_err(Error::from)?;
        let engine = open_engine(&config.raft_engine_dir)?;
        let storage = MemStorage::new_with_conf_state((voters.to_vec(), learners.to_vec()));
        let replay = replay_raft_engine_to_storage(&engine, raft_group_id, &storage)?;
        let raft_config = Config {
            id: config.node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: u64::MAX,
            max_inflight_msgs: 256,
            check_quorum: true,
            pre_vote: true,
            ..Default::default()
        };
        raft_config.validate().map_err(map_raft_error)?;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&raft_config, storage.clone(), &logger).map_err(map_raft_error)?;
        Ok(Self {
            node_id: config.node_id,
            raft_group_id,
            raw,
            storage,
            engine,
            committed: replay.committed_entries,
            last_applied_index: replay.last_applied_index,
        })
    }

    fn persist_ready(
        &self,
        entries: &[Entry],
        hard_state: Option<&HardState>,
        sync: bool,
    ) -> Result<()> {
        if entries.is_empty() && hard_state.is_none() {
            return Ok(());
        }
        let mut batch = LogBatch::default();
        for entry in entries {
            batch
                .put(
                    self.raft_group_id,
                    raft_entry_key(entry.index),
                    entry.encode_to_vec(),
                )
                .map_err(map_raft_engine_error)?;
        }
        if let Some(hard_state) = hard_state {
            batch
                .put(
                    self.raft_group_id,
                    HARD_STATE_KEY.to_vec(),
                    hard_state.encode_to_vec(),
                )
                .map_err(map_raft_engine_error)?;
        }
        self.engine
            .write(&mut batch, sync)
            .map(|_| ())
            .map_err(map_raft_engine_error)
    }

    fn apply_committed(&mut self, entries: Vec<Entry>, tablet_id: i64) -> Result<()> {
        for entry in entries {
            self.last_applied_index = entry.index;
            if entry.entry_type != EntryType::EntryNormal as i32 || entry.data.is_empty() {
                continue;
            }
            if self
                .committed
                .iter()
                .any(|committed| committed.index == entry.index)
            {
                continue;
            }
            self.committed.push(LogEntry {
                tablet_id,
                index: entry.index,
                term: entry.term,
                command: entry.data.to_vec(),
            });
        }
        Ok(())
    }

    fn compact_engine_entries(&self, up_to_index: u64) -> Result<usize> {
        if up_to_index == 0 {
            return Ok(0);
        }
        let mut keys = Vec::new();
        self.engine
            .scan_raw_messages(
                self.raft_group_id,
                Some(b"entry/"),
                Some(b"entry0"),
                false,
                |key, _| {
                    if parse_raft_entry_key(key).is_some_and(|index| index <= up_to_index) {
                        keys.push(key.to_vec());
                    }
                    true
                },
            )
            .map_err(map_raft_engine_error)?;
        let deleted = keys.len();
        if deleted > 0 {
            let mut batch = LogBatch::default();
            for key in keys {
                batch.delete(self.raft_group_id, key);
            }
            self.engine
                .write(&mut batch, true)
                .map_err(map_raft_engine_error)?;
        }
        Ok(deleted)
    }
}

struct RaftEngineReplay {
    committed_entries: Vec<LogEntry>,
    last_applied_index: u64,
}

fn open_engine(path: &Path) -> Result<RaftEngine> {
    let mut config = RaftEngineConfig::default();
    config.dir = path.to_string_lossy().into_owned();
    RaftEngine::open(config).map_err(map_raft_engine_error)
}

fn replay_raft_engine_to_storage(
    engine: &RaftEngine,
    raft_group_id: u64,
    storage: &MemStorage,
) -> Result<RaftEngineReplay> {
    let hard_state = if let Some(bytes) = engine.get(raft_group_id, HARD_STATE_KEY) {
        let hard_state = HardState::decode(bytes.as_slice()).map_err(map_prost_error)?;
        storage.wl().set_hardstate(hard_state.clone());
        hard_state
    } else {
        HardState::default()
    };
    let mut entries_by_index = BTreeMap::new();
    engine
        .scan_raw_messages(
            raft_group_id,
            Some(b"entry/"),
            Some(b"entry0"),
            false,
            |_, raw| {
                if let Ok(entry) = Entry::decode(raw) {
                    entries_by_index.insert(entry.index, entry);
                }
                true
            },
        )
        .map_err(map_raft_engine_error)?;
    let entries: Vec<_> = entries_by_index.into_values().collect();
    if !entries.is_empty() {
        storage.wl().append(&entries).map_err(map_raft_error)?;
    }
    let mut committed_entries = Vec::new();
    for entry in &entries {
        if entry.index <= hard_state.commit
            && entry.entry_type == EntryType::EntryNormal as i32
            && !entry.data.is_empty()
        {
            committed_entries.push(LogEntry {
                tablet_id: raft_group_id as i64,
                index: entry.index,
                term: entry.term,
                command: entry.data.to_vec(),
            });
        }
    }
    Ok(RaftEngineReplay {
        committed_entries,
        last_applied_index: hard_state.commit,
    })
}

fn raft_entry_key(index: u64) -> Vec<u8> {
    format!("entry/{index:020}").into_bytes()
}

fn parse_raft_entry_key(key: &[u8]) -> Option<u64> {
    let suffix = key.strip_prefix(b"entry/")?;
    std::str::from_utf8(suffix).ok()?.parse().ok()
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn validate_raft_tablet_id(tablet_id: i64) -> Result<()> {
    if tablet_id <= 0 {
        return Err(Error::Other("raft-rs tablet_id must be positive".into()));
    }
    Ok(())
}

fn map_raft_error(err: raft::Error) -> Error {
    Error::Other(format!("raft-rs error: {err}"))
}

fn map_raft_engine_error(err: raft_engine::Error) -> Error {
    Error::Other(format!("raft-engine error: {err}"))
}

fn map_prost_error(err: prost::DecodeError) -> Error {
    Error::Other(format!("prost decode error: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configs(count: u64) -> (tempfile::TempDir, Vec<RaftRsNodeConfig>) {
        let dir = tempfile::tempdir().unwrap();
        let configs = (1..=count)
            .map(|node_id| RaftRsNodeConfig {
                node_id,
                role: RaftRsNodeRole::Voter,
                raft_engine_dir: dir.path().join(format!("n{node_id}")),
            })
            .collect();
        (dir, configs)
    }

    #[test]
    fn raft_rs_log_commits_after_real_raft_election_and_quorum() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();

        let commit = logs[0].append(7, b"command".to_vec()).unwrap();

        assert!(commit.index > 0);
        for log in &logs {
            let entries = log.committed_entries(7, 0, 10).unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].command, b"command");
        }
        assert!(group
            .status()
            .iter()
            .any(|status| status.node_id == 1 && status.role == "Leader"));
    }

    #[test]
    fn raft_rs_log_persists_entries_to_tikv_raft_engine() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        logs[0].append(7, b"persisted".to_vec()).unwrap();

        let guard = group.inner.lock().unwrap();
        let mut persisted_entries = 0;
        guard
            .node(1)
            .unwrap()
            .engine
            .scan_raw_messages(7, Some(b"entry/"), Some(b"entry0"), false, |_, _| {
                persisted_entries += 1;
                true
            })
            .unwrap();
        assert!(persisted_entries > 0);
    }

    #[test]
    fn raft_rs_log_rebuilds_committed_entries_after_restart() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs.clone()).unwrap();
        group.elect(1).unwrap();
        logs[0].append(7, b"before-restart".to_vec()).unwrap();
        drop(logs);
        drop(group);

        let (_reopened_group, reopened_logs) = RaftRsSharedGroup::open(7, configs).unwrap();

        let entries = reopened_logs[1].committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command, b"before-restart");
    }

    #[test]
    fn raft_rs_log_compacts_persisted_engine_entries() {
        let (_dir, configs) = configs(3);
        let (_group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        logs[0].append(7, b"first".to_vec()).unwrap();
        logs[0].append(7, b"second".to_vec()).unwrap();

        let deleted = logs[0].compact(7, 1).unwrap();

        assert!(deleted >= 1);
        let entries = logs[0].committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn raft_rs_log_accepts_control_plane_voter_and_learner_roles() {
        let dir = tempfile::tempdir().unwrap();
        let configs = vec![
            RaftRsNodeConfig {
                node_id: 1,
                role: RaftRsNodeRole::from_replica_role("voter").unwrap(),
                raft_engine_dir: dir.path().join("n1"),
            },
            RaftRsNodeConfig {
                node_id: 2,
                role: RaftRsNodeRole::from_replica_role("learner").unwrap(),
                raft_engine_dir: dir.path().join("n2"),
            },
        ];

        let (_group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();

        assert_eq!(logs.len(), 2);
        assert!(RaftRsNodeRole::from_replica_role("log_only").is_err());
    }

    #[test]
    fn raft_rs_node_config_maps_control_plane_replica_stably() {
        let dir = tempfile::tempdir().unwrap();

        let first =
            RaftRsNodeConfig::from_control_plane_replica(7, "node-a.us-west", "voter", dir.path())
                .unwrap();
        let second =
            RaftRsNodeConfig::from_control_plane_replica(7, "node-a.us-west", "voter", dir.path())
                .unwrap();

        assert_eq!(first.node_id, second.node_id);
        assert_eq!(first.role, RaftRsNodeRole::Voter);
        assert!(first.raft_engine_dir.ends_with("tablet-7/node-a.us-west"));
        assert_eq!(stable_raft_node_id("42").unwrap(), 42);
        assert!(stable_raft_node_id("").is_err());
    }
}
