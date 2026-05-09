//! `raft-rs` backed replicated log for tablet writes.
//!
//! This module uses TiKV's `raft` crate for consensus and TiKV's
//! `raft-engine` crate for the durable local Raft log. The transport in this
//! first implementation is an in-process shared group so tests and single
//! binary deployments can exercise real Raft quorum behavior without inventing
//! another consensus protocol. A production network transport can reuse the
//! same Ready/message processing loop.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use prost::Message as ProstMessage;
use raft::{
    eraftpb::{
        ConfChangeSingle, ConfChangeType, ConfChangeV2, Entry, EntryType, HardState, Message,
        Snapshot,
    },
    raw_node::RawNode,
    storage::MemStorage,
    Config, StateRole, Storage,
};
use raft_engine::{Config as RaftEngineConfig, Engine as RaftEngine, LogBatch};
use sha2::{Digest, Sha256};

use crate::{Error, LogCommit, LogEntry, ReplicatedLog, Result};

const HARD_STATE_KEY: &[u8] = b"hard_state";
const SNAPSHOT_KEY: &[u8] = b"snapshot";
const APPEND_WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const APPEND_WAIT_POLL: Duration = Duration::from_millis(10);

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
    pub pending_conf_change: bool,
    pub outbound_message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftRsWireMessage {
    pub tablet_id: i64,
    pub from: u64,
    pub to: u64,
    pub payload: Vec<u8>,
}

impl RaftRsWireMessage {
    pub fn from_raft_message(tablet_id: i64, message: &Message) -> Self {
        Self {
            tablet_id,
            from: message.from,
            to: message.to,
            payload: message.encode_to_vec(),
        }
    }

    pub fn into_raft_message(self) -> Result<Message> {
        Message::decode(self.payload.as_slice()).map_err(map_prost_error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftRsSnapshotTransfer {
    pub tablet_id: i64,
    pub from: u64,
    pub to: u64,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

impl RaftRsSnapshotTransfer {
    fn from_snapshot(tablet_id: i64, from: u64, to: u64, snapshot: &Snapshot) -> Self {
        let metadata = snapshot.get_metadata();
        Self {
            tablet_id,
            from,
            to,
            index: metadata.index,
            term: metadata.term,
            payload: snapshot.encode_to_vec(),
        }
    }

    fn into_snapshot(self) -> Result<Snapshot> {
        Snapshot::decode(self.payload.as_slice()).map_err(map_prost_error)
    }
}

pub trait RaftRsTransport: Send + Sync {
    fn send_message(&self, message: RaftRsWireMessage) -> Result<()>;

    fn send_snapshot(&self, snapshot: RaftRsSnapshotTransfer) -> Result<()>;
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

fn prepare_raft_rs_configs(
    tablet_id: i64,
    configs: impl IntoIterator<Item = RaftRsNodeConfig>,
) -> Result<(Vec<RaftRsNodeConfig>, Vec<u64>, Vec<u64>)> {
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
    Ok((configs, voters, learners))
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
        let (configs, voters, learners) = prepare_raft_rs_configs(tablet_id, configs)?;
        let group = Self::open_with_local_configs(tablet_id, configs, voters, learners)?;
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

    pub fn open_local(
        tablet_id: i64,
        configs: impl IntoIterator<Item = RaftRsNodeConfig>,
        local_node_id: u64,
    ) -> Result<(Self, Arc<RaftRsReplicatedLog>)> {
        if local_node_id == 0 {
            return Err(Error::Other(
                "raft-rs local node id must be positive".into(),
            ));
        }
        let (configs, voters, learners) = prepare_raft_rs_configs(tablet_id, configs)?;
        let Some(local_config) = configs
            .into_iter()
            .find(|config| config.node_id == local_node_id)
        else {
            return Err(Error::Other(format!(
                "raft-rs local node {local_node_id} is not a replica of tablet {tablet_id}"
            )));
        };
        let group = Self::open_with_local_configs(tablet_id, vec![local_config], voters, learners)?;
        let handle = Arc::new(RaftRsReplicatedLog {
            group: group.clone(),
            node_id: local_node_id,
        });
        Ok((group, handle))
    }

    fn open_with_local_configs(
        tablet_id: i64,
        configs: Vec<RaftRsNodeConfig>,
        voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Result<Self> {
        validate_raft_tablet_id(tablet_id)?;
        if configs.is_empty() {
            return Err(Error::Other(
                "raft-rs group requires at least one node".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(RaftRsGroup::open(
                tablet_id, configs, voters, learners,
            )?)),
        })
    }

    pub fn elect(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .elect(node_id)
    }

    pub fn campaign(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .campaign(node_id)
    }

    pub fn status(&self) -> Vec<RaftRsReplicaStatus> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .status()
    }

    pub fn pending_wire_message_count(&self) -> usize {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .pending_wire_message_count()
    }

    pub fn mark_unavailable(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .mark_unavailable(node_id)
    }

    pub fn mark_available(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .mark_available(node_id)
    }

    pub fn take_wire_messages(&self) -> Vec<RaftRsWireMessage> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .take_wire_messages()
    }

    pub fn restore_wire_messages(&self, messages: Vec<RaftRsWireMessage>) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .restore_wire_messages(messages)
    }

    pub fn step_wire_message(&self, message: RaftRsWireMessage) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .step_wire_message(message)
    }

    pub fn tick(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .tick(node_id)
    }

    pub fn flush_wire_messages_to(&self, transport: &dyn RaftRsTransport) -> Result<usize> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .flush_wire_messages_to(transport)
    }

    pub fn send_snapshot_to(
        &self,
        transport: &dyn RaftRsTransport,
        snapshot: RaftRsSnapshotTransfer,
    ) -> Result<()> {
        transport.send_snapshot(snapshot)
    }

    pub fn promote_to_voter(&self, leader_id: u64, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .propose_membership_change(leader_id, node_id, ConfChangeType::AddNode)
    }

    pub fn remove_node(&self, leader_id: u64, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .propose_membership_change(leader_id, node_id, ConfChangeType::RemoveNode)
    }

    pub fn build_snapshot_transfer(
        &self,
        from: u64,
        to: u64,
        application_snapshot: Vec<u8>,
    ) -> Result<RaftRsSnapshotTransfer> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .build_snapshot_transfer(from, to, application_snapshot)
    }

    pub fn install_snapshot_transfer(
        &self,
        to: u64,
        transfer: RaftRsSnapshotTransfer,
    ) -> Result<Vec<u8>> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .install_snapshot_transfer(to, transfer)
    }

    fn append(&self, node_id: u64, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        {
            let mut group = self.inner.lock().expect("raft-rs group lock poisoned");
            if group.node_count() > 1 {
                return group.append(node_id, tablet_id, command);
            }
        }
        let started_at = Instant::now();
        let mut campaign_started = false;
        let mut proposed_after = None;
        loop {
            let maybe_commit = {
                let mut group = self.inner.lock().expect("raft-rs group lock poisoned");
                group.validate_append_target(node_id, tablet_id)?;
                if proposed_after.is_none() {
                    if !group.is_leader(node_id)? {
                        if !campaign_started {
                            group
                                .node_mut(node_id)?
                                .raw
                                .campaign()
                                .map_err(map_raft_error)?;
                            campaign_started = true;
                        }
                        group.drive_until_idle()?;
                        None
                    } else {
                        let previous_applied_index = group.node(node_id)?.last_applied_index;
                        group
                            .node_mut(node_id)?
                            .raw
                            .propose(Vec::new(), command.clone())
                            .map_err(map_raft_error)?;
                        proposed_after = Some(previous_applied_index);
                        group.drive_until_idle()?;
                        group.find_committed_after(node_id, tablet_id, previous_applied_index)?
                    }
                } else {
                    group.drive_until_idle()?;
                    group.find_committed_after(
                        node_id,
                        tablet_id,
                        proposed_after.expect("proposal index set"),
                    )?
                }
            };
            if let Some(commit) = maybe_commit {
                return Ok(commit);
            }
            if started_at.elapsed() >= APPEND_WAIT_TIMEOUT {
                let reason = if proposed_after.is_some() {
                    "command did not commit"
                } else {
                    "node did not become leader"
                };
                return Err(Error::Other(format!(
                    "raft-rs {reason} for tablet {tablet_id} before timeout"
                )));
            }
            thread::sleep(APPEND_WAIT_POLL);
        }
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
    unavailable_nodes: BTreeSet<u64>,
    messages: VecDeque<Message>,
    wire_messages: VecDeque<RaftRsWireMessage>,
    pending_conf_change: bool,
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
            unavailable_nodes: BTreeSet::new(),
            messages: VecDeque::new(),
            wire_messages: VecDeque::new(),
            pending_conf_change: false,
        };
        group.drive_until_idle()?;
        Ok(group)
    }

    fn node_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn mark_unavailable(&mut self, node_id: u64) -> Result<()> {
        self.node(node_id)?;
        self.unavailable_nodes.insert(node_id);
        Ok(())
    }

    fn mark_available(&mut self, node_id: u64) -> Result<()> {
        self.node(node_id)?;
        self.unavailable_nodes.remove(&node_id);
        self.drive_until_idle()
    }

    fn validate_append_target(&self, node_id: u64, tablet_id: i64) -> Result<()> {
        if tablet_id != self.tablet_id {
            return Err(Error::Other(format!(
                "raft-rs log opened for tablet {}, got tablet {tablet_id}",
                self.tablet_id
            )));
        }
        self.node(node_id)?;
        Ok(())
    }

    fn is_leader(&self, node_id: u64) -> Result<bool> {
        Ok(self.node(node_id)?.raw.status().ss.raft_state == StateRole::Leader)
    }

    fn find_committed_after(
        &self,
        node_id: u64,
        tablet_id: i64,
        previous_applied_index: u64,
    ) -> Result<Option<LogCommit>> {
        Ok(self
            .node(node_id)?
            .committed
            .iter()
            .find(|entry| entry.index > previous_applied_index)
            .map(|entry| LogCommit {
                tablet_id,
                index: entry.index,
                term: entry.term,
            }))
    }

    fn take_wire_messages(&mut self) -> Vec<RaftRsWireMessage> {
        self.wire_messages.drain(..).collect()
    }

    fn pending_wire_message_count(&self) -> usize {
        self.wire_messages.len()
    }

    fn restore_wire_messages(&mut self, messages: Vec<RaftRsWireMessage>) -> Result<()> {
        for message in &messages {
            if message.tablet_id != self.tablet_id {
                return Err(Error::Other(format!(
                    "raft-rs wire message targets tablet {}, local group is tablet {}",
                    message.tablet_id, self.tablet_id
                )));
            }
        }
        for message in messages.into_iter().rev() {
            self.wire_messages.push_front(message);
        }
        Ok(())
    }

    fn flush_wire_messages_to(&mut self, transport: &dyn RaftRsTransport) -> Result<usize> {
        let mut sent = 0;
        while let Some(message) = self.wire_messages.pop_front() {
            match transport.send_message(message.clone()) {
                Ok(()) => sent += 1,
                Err(err) => {
                    self.wire_messages.push_front(message);
                    return Err(err);
                }
            }
        }
        Ok(sent)
    }

    fn step_wire_message(&mut self, message: RaftRsWireMessage) -> Result<()> {
        if message.tablet_id != self.tablet_id {
            return Err(Error::Other(format!(
                "raft-rs wire message targets tablet {}, local group is tablet {}",
                message.tablet_id, self.tablet_id
            )));
        }
        if self.unavailable_nodes.contains(&message.to) {
            self.wire_messages.push_back(message);
            return Ok(());
        }
        let to = message.to;
        let raft_message = message.into_raft_message()?;
        self.node_mut(to)?
            .raw
            .step(raft_message)
            .map_err(map_raft_error)?;
        self.drive_until_idle()
    }

    fn tick(&mut self, node_id: u64) -> Result<()> {
        self.node_mut(node_id)?.raw.tick();
        self.drive_until_idle()
    }

    fn propose_membership_change(
        &mut self,
        leader_id: u64,
        node_id: u64,
        change_type: ConfChangeType,
    ) -> Result<()> {
        if self.pending_conf_change {
            return Err(Error::Other(format!(
                "raft-rs tablet {} already has a pending membership change",
                self.tablet_id
            )));
        }
        if self.node(leader_id)?.raw.status().ss.raft_state != StateRole::Leader {
            self.elect(leader_id)?;
        }
        let cc = conf_change(node_id, change_type);
        self.node_mut(leader_id)?
            .raw
            .propose_conf_change(Vec::new(), cc)
            .map_err(map_raft_error)?;
        self.pending_conf_change = true;
        self.drive_until_idle()
    }

    fn build_snapshot_transfer(
        &self,
        from: u64,
        to: u64,
        application_snapshot: Vec<u8>,
    ) -> Result<RaftRsSnapshotTransfer> {
        if to == 0 {
            return Err(Error::Other(
                "raft-rs snapshot target must be positive".into(),
            ));
        }
        let node = self.node(from)?;
        let mut snapshot = node
            .storage
            .snapshot(node.last_applied_index, to)
            .map_err(map_raft_error)?;
        snapshot.set_data(application_snapshot.into());
        Ok(RaftRsSnapshotTransfer::from_snapshot(
            self.tablet_id,
            from,
            to,
            &snapshot,
        ))
    }

    fn install_snapshot_transfer(
        &mut self,
        to: u64,
        transfer: RaftRsSnapshotTransfer,
    ) -> Result<Vec<u8>> {
        if transfer.tablet_id != self.tablet_id {
            return Err(Error::Other(format!(
                "raft-rs snapshot targets tablet {}, local group is tablet {}",
                transfer.tablet_id, self.tablet_id
            )));
        }
        if transfer.to != to {
            return Err(Error::Other(format!(
                "raft-rs snapshot targets node {}, install requested for node {to}",
                transfer.to
            )));
        }
        let node = self.node_mut(to)?;
        let snapshot = transfer.into_snapshot()?;
        let metadata = snapshot.get_metadata();
        if metadata.index < node.last_applied_index {
            return Err(Error::Other(format!(
                "stale raft-rs snapshot at index {}; node {to} is already applied through {}",
                metadata.index, node.last_applied_index
            )));
        }
        let application_snapshot = snapshot.get_data().to_vec();
        node.install_snapshot(snapshot)?;
        Ok(application_snapshot)
    }

    fn elect(&mut self, node_id: u64) -> Result<()> {
        self.campaign(node_id)?;
        if self.node(node_id)?.raw.status().ss.raft_state != StateRole::Leader {
            return Err(Error::Other(format!(
                "raft-rs node {node_id} did not become leader"
            )));
        }
        Ok(())
    }

    fn campaign(&mut self, node_id: u64) -> Result<()> {
        self.node_mut(node_id)?
            .raw
            .campaign()
            .map_err(map_raft_error)?;
        self.drive_until_idle()
    }

    fn append(&mut self, node_id: u64, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        self.validate_append_target(node_id, tablet_id)?;
        if self.node(node_id)?.raw.status().ss.raft_state != StateRole::Leader {
            self.elect(node_id)?;
        }
        let previous_applied_index = self.node(node_id)?.last_applied_index;
        self.node_mut(node_id)?
            .raw
            .propose(Vec::new(), command)
            .map_err(map_raft_error)?;
        self.drive_until_idle()?;

        let Some(committed) =
            self.find_committed_after(node_id, tablet_id, previous_applied_index)?
        else {
            return Err(Error::Other(format!(
                "raft-rs command did not commit for tablet {tablet_id}"
            )));
        };
        Ok(committed)
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
        let snapshot = node
            .storage
            .snapshot(up_to_index, node_id)
            .map_err(map_raft_error)?;
        node.persist_snapshot(&snapshot)?;
        node.storage
            .wl()
            .compact(up_to_index.saturating_add(1))
            .map_err(map_raft_error)?;
        let persisted_deleted = node.compact_engine_entries(up_to_index)?;
        let retained_before = node.committed.len();
        node.committed.retain(|entry| entry.index > up_to_index);
        let retained_deleted = retained_before.saturating_sub(node.committed.len());
        Ok(persisted_deleted.max(retained_deleted))
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
                    pending_conf_change: self.pending_conf_change,
                    outbound_message_count: self.wire_messages.len(),
                }
            })
            .collect()
    }

    fn drive_until_idle(&mut self) -> Result<()> {
        for _ in 0..10_000 {
            let mut progressed = false;
            let node_ids = self.node_ids();
            for node_id in node_ids {
                if self.unavailable_nodes.contains(&node_id) {
                    continue;
                }
                progressed |= self.process_ready(node_id)?;
            }
            while let Some(message) = self.messages.pop_front() {
                let to = message.to;
                if self.unavailable_nodes.contains(&message.from)
                    || self.unavailable_nodes.contains(&to)
                {
                    self.wire_messages
                        .push_back(RaftRsWireMessage::from_raft_message(
                            self.tablet_id,
                            &message,
                        ));
                    continue;
                }
                if let Some(node) = self.nodes.get_mut(&to) {
                    match node.raw.step(message) {
                        Ok(()) => {
                            progressed = true;
                        }
                        Err(err) if err.to_string().contains("peer not found") => {
                            tracing::debug!(
                                node_id = to,
                                ?err,
                                "ignored message for removed raft peer"
                            );
                        }
                        Err(err) => return Err(map_raft_error(err)),
                    }
                } else {
                    self.wire_messages
                        .push_back(RaftRsWireMessage::from_raft_message(
                            self.tablet_id,
                            &message,
                        ));
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
        let mut applied_conf_change =
            node.apply_committed(ready.take_committed_entries(), tablet_id)?;
        let mut light = node.raw.advance(ready);
        outbound.extend(light.take_messages());
        applied_conf_change |= node.apply_committed(light.take_committed_entries(), tablet_id)?;
        if applied_conf_change {
            self.pending_conf_change = false;
        }
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

    fn apply_committed(&mut self, entries: Vec<Entry>, tablet_id: i64) -> Result<bool> {
        let mut applied_conf_change = false;
        for entry in entries {
            self.last_applied_index = entry.index;
            if entry.entry_type == EntryType::EntryConfChangeV2 as i32 {
                let cc = ConfChangeV2::decode(entry.data.as_slice()).map_err(map_prost_error)?;
                self.raw.apply_conf_change(&cc).map_err(map_raft_error)?;
                applied_conf_change = true;
                continue;
            }
            if entry.entry_type == EntryType::EntryNormal as i32 && !entry.data.is_empty() {
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
        }
        Ok(applied_conf_change)
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

    fn install_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        self.storage
            .wl()
            .apply_snapshot(snapshot.clone())
            .map_err(map_raft_error)?;
        self.persist_snapshot(&snapshot)?;
        self.last_applied_index = index;
        self.committed.retain(|entry| entry.index > index);
        self.compact_engine_entries(index)?;
        Ok(())
    }

    fn persist_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let hard_state = self
            .storage
            .initial_state()
            .map_err(map_raft_error)?
            .hard_state;
        let mut batch = LogBatch::default();
        batch
            .put(
                self.raft_group_id,
                SNAPSHOT_KEY.to_vec(),
                snapshot.encode_to_vec(),
            )
            .map_err(map_raft_engine_error)?;
        batch
            .put(
                self.raft_group_id,
                HARD_STATE_KEY.to_vec(),
                hard_state.encode_to_vec(),
            )
            .map_err(map_raft_engine_error)?;
        self.engine
            .write(&mut batch, true)
            .map(|_| ())
            .map_err(map_raft_engine_error)
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
    let snapshot_index = if let Some(bytes) = engine.get(raft_group_id, SNAPSHOT_KEY) {
        let snapshot = Snapshot::decode(bytes.as_slice()).map_err(map_prost_error)?;
        let index = snapshot.get_metadata().index;
        storage
            .wl()
            .apply_snapshot(snapshot)
            .map_err(map_raft_error)?;
        index
    } else {
        0
    };
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
        last_applied_index: hard_state.commit.max(snapshot_index),
    })
}

fn raft_entry_key(index: u64) -> Vec<u8> {
    format!("entry/{index:020}").into_bytes()
}

fn conf_change(node_id: u64, change_type: ConfChangeType) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            node_id,
            change_type: change_type as i32,
        }],
        ..Default::default()
    }
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

    #[derive(Default)]
    struct RecordingRaftRsTransport {
        messages: Mutex<Vec<RaftRsWireMessage>>,
        snapshots: Mutex<Vec<RaftRsSnapshotTransfer>>,
        fail_after: Mutex<Option<usize>>,
    }

    impl RecordingRaftRsTransport {
        fn fail_after(count: usize) -> Self {
            Self {
                fail_after: Mutex::new(Some(count)),
                ..Default::default()
            }
        }
    }

    impl RaftRsTransport for RecordingRaftRsTransport {
        fn send_message(&self, message: RaftRsWireMessage) -> Result<()> {
            let mut fail_after = self.fail_after.lock().unwrap();
            if let Some(remaining) = fail_after.as_mut() {
                if *remaining == 0 {
                    return Err(Error::Other("transport unavailable".into()));
                }
                *remaining -= 1;
            }
            self.messages.lock().unwrap().push(message);
            Ok(())
        }

        fn send_snapshot(&self, snapshot: RaftRsSnapshotTransfer) -> Result<()> {
            self.snapshots.lock().unwrap().push(snapshot);
            Ok(())
        }
    }

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

        let before = logs[0].committed_entries(7, 0, 10).unwrap();
        assert_eq!(before.len(), 2);
        let deleted = logs[0].compact(7, before[0].index).unwrap();

        assert!(deleted >= 1);
        let entries = logs[0].committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command, b"second");
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

    #[test]
    fn raft_rs_log_commits_with_one_unavailable_follower() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(3).unwrap();

        let commit = logs[0].append(7, b"majority".to_vec()).unwrap();

        assert!(commit.index > 0);
        assert_eq!(
            logs[1].committed_entries(7, 0, 10).unwrap()[0].command,
            b"majority"
        );
        assert!(logs[2].committed_entries(7, 0, 10).unwrap().is_empty());
    }

    #[test]
    fn raft_rs_log_fails_without_quorum_and_recovers_after_follower_returns() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(2).unwrap();
        group.mark_unavailable(3).unwrap();

        let err = logs[0].append(7, b"no-quorum".to_vec()).unwrap_err();
        assert!(err.to_string().contains("did not commit"));

        group.mark_available(2).unwrap();
        let commit = logs[0].append(7, b"quorum-restored".to_vec()).unwrap();

        assert!(commit.index > 0);
        let entries = logs[1].committed_entries(7, 0, 10).unwrap();
        assert!(entries
            .iter()
            .any(|entry| entry.command == b"quorum-restored"));
    }

    #[test]
    fn raft_rs_log_exposes_wire_messages_for_unavailable_nodes() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(3).unwrap();

        logs[0].append(7, b"wire".to_vec()).unwrap();
        let messages = group.take_wire_messages();

        assert!(messages.iter().any(|message| {
            message.tablet_id == 7
                && message.from == 1
                && message.to == 3
                && !message.payload.is_empty()
        }));
    }

    #[test]
    fn raft_rs_log_steps_wire_message_after_node_returns() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(3).unwrap();
        logs[0].append(7, b"wire-delivery".to_vec()).unwrap();
        let messages = group.take_wire_messages();

        group.mark_available(3).unwrap();
        for message in messages {
            if message.to == 3 {
                group.step_wire_message(message).unwrap();
            }
        }

        let entries = logs[2].committed_entries(7, 0, 10).unwrap();
        assert!(entries
            .iter()
            .any(|entry| entry.command == b"wire-delivery"));
    }

    #[test]
    fn raft_rs_log_promotes_learner_to_voter() {
        let dir = tempfile::tempdir().unwrap();
        let configs = vec![
            RaftRsNodeConfig {
                node_id: 1,
                role: RaftRsNodeRole::Voter,
                raft_engine_dir: dir.path().join("n1"),
            },
            RaftRsNodeConfig {
                node_id: 2,
                role: RaftRsNodeRole::Voter,
                raft_engine_dir: dir.path().join("n2"),
            },
            RaftRsNodeConfig {
                node_id: 3,
                role: RaftRsNodeRole::Learner,
                raft_engine_dir: dir.path().join("n3"),
            },
        ];
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();

        group.promote_to_voter(1, 3).unwrap();
        group.mark_unavailable(2).unwrap();
        let commit = logs[0].append(7, b"promoted-voter".to_vec()).unwrap();

        assert!(commit.index > 0);
        assert!(logs[2]
            .committed_entries(7, 0, 10)
            .unwrap()
            .iter()
            .any(|entry| entry.command == b"promoted-voter"));
    }

    #[test]
    fn raft_rs_membership_changes_are_serialized_until_committed() {
        let dir = tempfile::tempdir().unwrap();
        let configs = vec![
            RaftRsNodeConfig {
                node_id: 1,
                role: RaftRsNodeRole::Voter,
                raft_engine_dir: dir.path().join("n1"),
            },
            RaftRsNodeConfig {
                node_id: 2,
                role: RaftRsNodeRole::Voter,
                raft_engine_dir: dir.path().join("n2"),
            },
            RaftRsNodeConfig {
                node_id: 3,
                role: RaftRsNodeRole::Learner,
                raft_engine_dir: dir.path().join("n3"),
            },
        ];
        let (group, _logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(2).unwrap();
        group.mark_unavailable(3).unwrap();

        group.promote_to_voter(1, 3).unwrap();
        let err = group.remove_node(1, 2).unwrap_err();

        assert!(err.to_string().contains("pending membership change"));
        assert!(group
            .status()
            .iter()
            .all(|status| status.pending_conf_change));
        assert!(group.pending_wire_message_count() > 0);
    }

    #[test]
    fn raft_rs_log_removes_voter_from_quorum() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();

        group.remove_node(1, 3).unwrap();
        group.mark_unavailable(2).unwrap();
        let err = logs[0].append(7, b"after-remove".to_vec()).unwrap_err();

        assert!(err.to_string().contains("did not commit"));
    }

    #[test]
    fn raft_rs_snapshot_transfer_round_trips_payload_and_advances_target() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        let commit = logs[0].append(7, b"before-snapshot".to_vec()).unwrap();

        let transfer = group
            .build_snapshot_transfer(1, 3, b"tablet-state-snapshot".to_vec())
            .unwrap();
        assert_eq!(transfer.tablet_id, 7);
        assert_eq!(transfer.from, 1);
        assert_eq!(transfer.to, 3);
        assert_eq!(transfer.index, commit.index);

        let payload = group.install_snapshot_transfer(3, transfer).unwrap();

        assert_eq!(payload, b"tablet-state-snapshot");
        let status = group
            .status()
            .into_iter()
            .find(|status| status.node_id == 3)
            .unwrap();
        assert_eq!(status.applied_index, commit.index);
        assert!(logs[2].committed_entries(7, 0, 10).unwrap().is_empty());
    }

    #[test]
    fn raft_rs_snapshot_transfer_rejects_wrong_target_and_tablet() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        logs[0].append(7, b"before-snapshot".to_vec()).unwrap();

        let transfer = group
            .build_snapshot_transfer(1, 3, b"tablet-state-snapshot".to_vec())
            .unwrap();
        let wrong_target = group
            .install_snapshot_transfer(2, transfer.clone())
            .unwrap_err();
        assert!(wrong_target.to_string().contains("targets node 3"));

        let wrong_tablet = RaftRsSnapshotTransfer {
            tablet_id: 8,
            ..transfer
        };
        let err = group
            .install_snapshot_transfer(3, wrong_tablet)
            .unwrap_err();
        assert!(err.to_string().contains("targets tablet 8"));
    }

    #[test]
    fn raft_rs_snapshot_transfer_rejects_stale_snapshot() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        logs[0].append(7, b"first".to_vec()).unwrap();
        let stale = group
            .build_snapshot_transfer(1, 3, b"stale-snapshot".to_vec())
            .unwrap();
        logs[0].append(7, b"second".to_vec()).unwrap();
        let fresh = group
            .build_snapshot_transfer(1, 3, b"fresh-snapshot".to_vec())
            .unwrap();
        group.install_snapshot_transfer(3, fresh).unwrap();

        let err = group.install_snapshot_transfer(3, stale).unwrap_err();

        assert!(err.to_string().contains("stale raft-rs snapshot"));
    }

    #[test]
    fn raft_rs_snapshot_transfer_persists_across_restart() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs.clone()).unwrap();
        group.elect(1).unwrap();
        let commit = logs[0].append(7, b"before-snapshot".to_vec()).unwrap();
        let transfer = group
            .build_snapshot_transfer(1, 3, b"tablet-state-snapshot".to_vec())
            .unwrap();
        group.install_snapshot_transfer(3, transfer).unwrap();
        drop(logs);
        drop(group);

        let (_reopened_group, reopened_logs) = RaftRsSharedGroup::open(7, configs).unwrap();

        assert!(reopened_logs[2]
            .committed_entries(7, 0, 10)
            .unwrap()
            .is_empty());
        assert_eq!(reopened_logs[2].compact(7, commit.index).unwrap(), 0);
    }

    #[test]
    fn raft_rs_transport_flushes_wire_message_outbox() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(3).unwrap();
        logs[0].append(7, b"wire-outbox".to_vec()).unwrap();
        let transport = RecordingRaftRsTransport::default();

        let sent = group.flush_wire_messages_to(&transport).unwrap();

        assert!(sent > 0);
        assert_eq!(group.take_wire_messages().len(), 0);
        assert!(transport
            .messages
            .lock()
            .unwrap()
            .iter()
            .any(|message| message.to == 3));
    }

    #[test]
    fn raft_rs_transport_keeps_unsent_wire_messages_on_failure() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        group.mark_unavailable(3).unwrap();
        logs[0].append(7, b"wire-outbox".to_vec()).unwrap();
        let transport = RecordingRaftRsTransport::fail_after(0);

        let err = group.flush_wire_messages_to(&transport).unwrap_err();

        assert!(err.to_string().contains("transport unavailable"));
        assert!(!group.take_wire_messages().is_empty());
    }

    #[test]
    fn raft_rs_transport_sends_snapshot_transfer() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        logs[0].append(7, b"before-snapshot".to_vec()).unwrap();
        let snapshot = group
            .build_snapshot_transfer(1, 3, b"tablet-state-snapshot".to_vec())
            .unwrap();
        let transport = RecordingRaftRsTransport::default();

        group
            .send_snapshot_to(&transport, snapshot.clone())
            .unwrap();

        assert_eq!(transport.snapshots.lock().unwrap().as_slice(), &[snapshot]);
    }

    #[test]
    fn raft_rs_open_local_hosts_only_local_node_with_full_conf_state() {
        let (_dir, configs) = configs(3);
        let (group, local_log) = RaftRsSharedGroup::open_local(7, configs, 2).unwrap();

        assert_eq!(local_log.node_id(), 2);
        assert_eq!(group.status().len(), 1);
        assert_eq!(group.status()[0].node_id, 2);

        let err = group.elect(2).unwrap_err();
        assert!(err.to_string().contains("did not become leader"));
        let outbound = group.take_wire_messages();
        assert!(
            outbound.iter().any(|message| message.to == 1)
                && outbound.iter().any(|message| message.to == 3)
        );
    }

    #[test]
    fn raft_rs_open_local_builds_snapshot_for_remote_peer() {
        let (_dir, configs) = configs(2);
        let (group, logs) = RaftRsSharedGroup::open(7, configs.clone()).unwrap();
        group.elect(1).unwrap();
        let commit = logs[0].append(7, b"snapshot-source".to_vec()).unwrap();
        drop(group);
        drop(logs);

        let (local_group, _local_log) = RaftRsSharedGroup::open_local(7, configs, 1).unwrap();
        let transfer = local_group
            .build_snapshot_transfer(1, 2, b"application-state".to_vec())
            .unwrap();

        assert_eq!(transfer.tablet_id, 7);
        assert_eq!(transfer.from, 1);
        assert_eq!(transfer.to, 2);
        assert_eq!(transfer.index, commit.index);
        assert!(!transfer.payload.is_empty());
    }

    #[test]
    fn raft_rs_open_local_installs_snapshot_from_remote_source() {
        let (_dir, configs) = configs(2);
        let (group, logs) = RaftRsSharedGroup::open(7, configs.clone()).unwrap();
        group.elect(1).unwrap();
        let commit = logs[0].append(7, b"snapshot-source".to_vec()).unwrap();
        let transfer = group
            .build_snapshot_transfer(1, 2, b"application-state".to_vec())
            .unwrap();
        drop(group);
        drop(logs);

        let (target_group, _target_log) = RaftRsSharedGroup::open_local(7, configs, 2).unwrap();
        let payload = target_group.install_snapshot_transfer(2, transfer).unwrap();

        assert_eq!(payload, b"application-state");
        assert_eq!(target_group.status()[0].applied_index, commit.index);
    }

    #[test]
    fn raft_rs_open_local_commits_through_delivered_wire_messages() {
        let (_dir, configs) = configs(3);
        let (group_1, log_1) = RaftRsSharedGroup::open_local(7, configs.clone(), 1).unwrap();
        let (group_2, log_2) = RaftRsSharedGroup::open_local(7, configs.clone(), 2).unwrap();
        let (group_3, log_3) = RaftRsSharedGroup::open_local(7, configs, 3).unwrap();
        let groups = BTreeMap::from([(1, group_1.clone()), (2, group_2), (3, group_3)]);

        let append = std::thread::spawn(move || log_1.append(7, b"network-quorum".to_vec()));
        for _ in 0..1_000 {
            let mut messages = Vec::new();
            for group in groups.values() {
                messages.extend(group.take_wire_messages());
            }
            for message in messages {
                groups
                    .get(&message.to)
                    .expect("target raft group")
                    .step_wire_message(message)
                    .unwrap();
            }
            if append.is_finished() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        let commit = append.join().unwrap().unwrap();
        assert!(commit.index > 0);
        for _ in 0..10 {
            let mut messages = Vec::new();
            for group in groups.values() {
                messages.extend(group.take_wire_messages());
            }
            if messages.is_empty() {
                break;
            }
            for message in messages {
                groups
                    .get(&message.to)
                    .expect("target raft group")
                    .step_wire_message(message)
                    .unwrap();
            }
        }
        assert_eq!(
            log_2.committed_entries(7, 0, 10).unwrap()[0].command,
            b"network-quorum"
        );
        assert_eq!(
            log_3.committed_entries(7, 0, 10).unwrap()[0].command,
            b"network-quorum"
        );
        assert!(group_1
            .status()
            .iter()
            .any(|status| status.node_id == 1 && status.role == "Leader"));
    }
}
