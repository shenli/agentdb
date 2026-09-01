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
        MessageType, Snapshot,
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
const APPEND_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
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
    Ok((u64::from_be_bytes(raw) & i64::MAX as u64).max(1))
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

    pub fn transfer_leader(&self, leader_id: u64, transferee_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .transfer_leader(leader_id, transferee_id)
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

    pub fn expire_leader_lease(&self, node_id: u64) -> Result<()> {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .expire_leader_lease(node_id)
    }

    /// Linearizable read barrier (ReadIndex). Confirms this node still
    /// holds leadership against a quorum and waits until the local state
    /// machine has applied at least the confirmed read index, then returns
    /// that index. A partitioned or stepped-down former leader never gets
    /// quorum confirmation, so this returns a typed timeout error rather
    /// than ever falling back to a local read.
    ///
    /// The commit-side note in `append` applies here too: the wait can
    /// block a tokio worker, so we hand off with `block_in_place` on the
    /// multi-thread runtime.
    pub fn read_barrier(&self, node_id: u64, timeout: Duration) -> Result<u64> {
        let run = || -> Result<u64> {
            let ctx = {
                let mut group = self.inner.lock().expect("raft-rs group lock poisoned");
                let ctx = group.submit_read_index(node_id)?;
                group.drive_until_idle()?;
                ctx
            };
            let started_at = Instant::now();
            loop {
                let satisfied = {
                    let mut group = self.inner.lock().expect("raft-rs group lock poisoned");
                    group.drive_until_idle()?;
                    group.take_satisfied_read_index(node_id, &ctx)?
                };
                if let Some(index) = satisfied {
                    return Ok(index);
                }
                if started_at.elapsed() >= timeout {
                    // Drop the parked read state so a slow-but-alive barrier
                    // does not leak entries into pending_reads. Read the
                    // tablet id from the held guard — self.tablet_id() would
                    // re-lock this non-reentrant Mutex and deadlock.
                    let mut group = self.inner.lock().expect("raft-rs group lock poisoned");
                    let tablet_id = group.tablet_id;
                    if let Ok(node) = group.node_mut(node_id) {
                        node.pending_reads.remove(&ctx);
                    }
                    return Err(Error::Other(format!(
                        "raft-rs read barrier for tablet {tablet_id} did not confirm leadership \
                         within timeout (node may be partitioned or no longer leader)"
                    )));
                }
                thread::sleep(APPEND_WAIT_POLL);
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(run)
            }
            _ => run(),
        }
    }

    pub fn tablet_id(&self) -> i64 {
        self.inner
            .lock()
            .expect("raft-rs group lock poisoned")
            .tablet_id
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
            group.validate_append_target(node_id, tablet_id)?;
            if !group.is_leader(node_id)? {
                return Err(Error::Other(format!(
                    "raft-rs local node {node_id} is not leader for tablet {tablet_id}"
                )));
            }
        }
        // The commit wait below blocks the calling thread for up to
        // APPEND_WAIT_TIMEOUT. When that thread is a tokio worker, tell the
        // runtime so it can shift queued tasks (ingress handlers, the
        // message outbox, the accept loop) to other workers — a handful of
        // concurrent replicated writes must not freeze the async runtime
        // that raft message delivery itself depends on.
        let wait_for_commit = || -> Result<LogCommit> {
            let started_at = Instant::now();
            let mut proposed_after = None;
            loop {
                let maybe_commit = {
                    let mut group = self.inner.lock().expect("raft-rs group lock poisoned");
                    group.validate_append_target(node_id, tablet_id)?;
                    let previous_applied_index = match proposed_after {
                        Some(index) => index,
                        None => {
                            let previous_applied_index = group.node(node_id)?.last_applied_index;
                            group
                                .node_mut(node_id)?
                                .raw
                                .propose(Vec::new(), command.clone())
                                .map_err(map_raft_error)?;
                            proposed_after = Some(previous_applied_index);
                            previous_applied_index
                        }
                    };
                    group.drive_until_idle()?;
                    group.find_committed_after(node_id, tablet_id, previous_applied_index)?
                };
                if let Some(commit) = maybe_commit {
                    return Ok(commit);
                }
                if started_at.elapsed() >= APPEND_WAIT_TIMEOUT {
                    return Err(Error::Other(format!(
                        "raft-rs command did not commit for tablet {tablet_id} before timeout"
                    )));
                }
                thread::sleep(APPEND_WAIT_POLL);
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(wait_for_commit)
            }
            _ => wait_for_commit(),
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
    /// Monotonic source for unique ReadIndex request contexts. A plain
    /// counter (not a clock/random) keeps the barrier deterministic under
    /// test replay.
    read_counter: u64,
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
            read_counter: 0,
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

    /// Submit a ReadIndex request and return the context bytes it can be
    /// collected under. The caller must be leader — raft-rs `Safe`
    /// ReadIndex confirms leadership against a quorum before yielding a
    /// read state, which is the whole point: an isolated former leader
    /// never gets the quorum confirmation, so its read state never arrives.
    fn submit_read_index(&mut self, node_id: u64) -> Result<Vec<u8>> {
        if !self.is_leader(node_id)? {
            return Err(Error::Other(format!(
                "raft-rs local node {node_id} is not leader for tablet {}; \
                 linearizable read requires the leader",
                self.tablet_id
            )));
        }
        self.read_counter += 1;
        let mut ctx = Vec::with_capacity(16);
        ctx.extend_from_slice(&self.tablet_id.to_be_bytes());
        ctx.extend_from_slice(&self.read_counter.to_be_bytes());
        self.node_mut(node_id)?.raw.read_index(ctx.clone());
        Ok(ctx)
    }

    /// Collect a previously submitted read index if its quorum confirmation
    /// has landed AND the local state machine has applied up to it. Returns
    /// `Ok(Some(index))` when the barrier is satisfied, `Ok(None)` while
    /// still waiting. Re-parks the read state if the index is confirmed but
    /// not yet applied.
    fn take_satisfied_read_index(&mut self, node_id: u64, ctx: &[u8]) -> Result<Option<u64>> {
        let last_applied = self.node(node_id)?.last_applied_index;
        let node = self.node_mut(node_id)?;
        match node.pending_reads.remove(ctx) {
            Some(index) if last_applied >= index => Ok(Some(index)),
            Some(index) => {
                node.pending_reads.insert(ctx.to_vec(), index);
                Ok(None)
            }
            None => Ok(None),
        }
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
        let mut raft_message = message.into_raft_message()?;
        self.clamp_out_of_order_heartbeat_commit(to, &mut raft_message)?;
        self.node_mut(to)?
            .raw
            .step(raft_message)
            .map_err(map_raft_error)?;
        self.drive_until_idle()
    }

    fn clamp_out_of_order_heartbeat_commit(&self, to: u64, message: &mut Message) -> Result<()> {
        if message.get_msg_type() != MessageType::MsgHeartbeat {
            return Ok(());
        }
        let last_index = self.node(to)?.raw.raft.raft_log.last_index();
        if message.commit > last_index {
            tracing::debug!(
                tablet_id = self.tablet_id,
                to,
                from = message.from,
                leader_commit = message.commit,
                last_index,
                "clamped out-of-order raft heartbeat commit to local last index"
            );
            message.commit = last_index;
        }
        Ok(())
    }

    fn tick(&mut self, node_id: u64) -> Result<()> {
        self.node_mut(node_id)?.raw.tick();
        self.drive_until_idle()
    }

    fn expire_leader_lease(&mut self, node_id: u64) -> Result<()> {
        let node = self.node_mut(node_id)?;
        if node.raw.status().ss.raft_state == StateRole::Follower {
            // Let this follower grant pre-votes after an externally detected
            // owner failure without ticking it into its own campaign.
            node.raw.raft.election_elapsed = usize::MAX / 4;
        }
        Ok(())
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
        snapshot.set_data(application_snapshot);
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
        let applied_index = node.last_applied_index.max(node.raw.raft.raft_log.applied);
        if metadata.index < applied_index {
            return Err(Error::Other(format!(
                "stale raft-rs snapshot at index {}; node {to} is already applied through {}",
                metadata.index, applied_index
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

    fn transfer_leader(&mut self, leader_id: u64, transferee_id: u64) -> Result<()> {
        if leader_id == transferee_id {
            return Ok(());
        }
        if self.node(leader_id)?.raw.status().ss.raft_state != StateRole::Leader {
            return Err(Error::Other(format!(
                "raft-rs node {leader_id} is not leader and cannot transfer leadership to {transferee_id}"
            )));
        }
        self.node_mut(leader_id)?.raw.transfer_leader(transferee_id);
        self.drive_until_idle()
    }

    fn append(&mut self, node_id: u64, tablet_id: i64, command: Vec<u8>) -> Result<LogCommit> {
        self.validate_append_target(node_id, tablet_id)?;
        if self.node(node_id)?.raw.status().ss.raft_state != StateRole::Leader {
            return Err(Error::Other(format!(
                "raft-rs node {node_id} is not leader for tablet {tablet_id}"
            )));
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
        let applied_index = node.last_applied_index.min(node.raw.raft.raft_log.applied);
        let requested_up_to_index = up_to_index.min(applied_index);
        if requested_up_to_index == 0 {
            return Ok(0);
        }
        let storage_up_to_index = if requested_up_to_index >= applied_index {
            applied_index.saturating_sub(1)
        } else {
            requested_up_to_index
        };
        let persisted_deleted = if storage_up_to_index == 0 {
            0
        } else {
            let snapshot = node
                .storage
                .snapshot(storage_up_to_index, node_id)
                .map_err(map_raft_error)?;
            node.persist_snapshot(&snapshot)?;
            node.storage
                .wl()
                .compact(storage_up_to_index)
                .map_err(map_raft_error)?;
            node.compact_engine_entries(storage_up_to_index)?
        };
        let retained_before = node.committed.len();
        node.committed
            .retain(|entry| entry.index > requested_up_to_index);
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
        // Capture ReadIndex results before advancing: read_barrier waits on
        // these, and a dropped read state hangs the barrier forever.
        let read_states: Vec<(Vec<u8>, u64)> = ready
            .read_states()
            .iter()
            .map(|state| (state.request_ctx.clone(), state.index))
            .collect();
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
        let mut light = node.raw.advance_append(ready);
        outbound.extend(light.take_messages());
        applied_conf_change |= node.apply_committed(light.take_committed_entries(), tablet_id)?;
        let raft_applied_index = node.raw.raft.raft_log.applied;
        let committed_index = node.raw.raft.raft_log.committed;
        if committed_index >= raft_applied_index {
            node.last_applied_index = node
                .last_applied_index
                .clamp(raft_applied_index, committed_index);
            node.raw.advance_apply_to(node.last_applied_index);
        }
        for (ctx, index) in read_states {
            node.pending_reads.insert(ctx, index);
        }
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
    /// ReadIndex results keyed by the request context we submitted:
    /// `ctx -> committed read index`. Populated when `process_ready`
    /// observes a `ReadState`; drained by `read_barrier`. Without this the
    /// read states raft-rs produces would be dropped and every barrier
    /// would hang (see event-driven-state-stuck footgun).
    pending_reads: std::collections::HashMap<Vec<u8>, u64>,
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
        let raft_config = raft_node_config(config.node_id, replay.last_applied_index);
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
            pending_reads: std::collections::HashMap::new(),
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
            if entry.index <= self.last_applied_index {
                continue;
            }
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
        let mut hard_state = self
            .storage
            .initial_state()
            .map_err(map_raft_error)?
            .hard_state;
        hard_state.commit = hard_state.commit.max(index);
        self.storage.wl().set_hardstate(hard_state);
        self.persist_snapshot(&snapshot)?;
        self.rebuild_raw_node(index)?;
        self.last_applied_index = self.last_applied_index.max(index);
        self.committed.retain(|entry| entry.index > index);
        self.compact_engine_entries(index)?;
        Ok(())
    }

    fn rebuild_raw_node(&mut self, applied: u64) -> Result<()> {
        let raft_config = raft_node_config(self.node_id, applied);
        raft_config.validate().map_err(map_raft_error)?;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        self.raw =
            RawNode::new(&raft_config, self.storage.clone(), &logger).map_err(map_raft_error)?;
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

fn raft_node_config(node_id: u64, applied: u64) -> Config {
    Config {
        id: node_id,
        election_tick: 10,
        heartbeat_tick: 3,
        applied,
        max_size_per_msg: u64::MAX,
        max_inflight_msgs: 256,
        // check_quorum is required for correct linearizable ReadIndex.
        // Without it raft-rs never resets per-peer `recent_active`, so an
        // isolated leader believes its peers are still reachable forever
        // and keeps confirming ReadIndex against a quorum that no longer
        // exists (it serves stale reads). With check_quorum the leader
        // clears recent_active each election timeout and steps down when it
        // cannot re-confirm a quorum, after which the read barrier fails
        // closed. Paired with pre_vote (already on) so a stepped-down or
        // isolated node cannot win an election without a reachable quorum.
        check_quorum: true,
        pre_vote: true,
        ..Default::default()
    }
}

struct RaftEngineReplay {
    committed_entries: Vec<LogEntry>,
    last_applied_index: u64,
}

fn open_engine(path: &Path) -> Result<RaftEngine> {
    let dir = path.to_string_lossy().into_owned();
    let mut last_error = None;
    for attempt in 0..20 {
        let config = RaftEngineConfig {
            dir: dir.clone(),
            ..Default::default()
        };
        match RaftEngine::open(config) {
            Ok(engine) => return Ok(engine),
            Err(err) if raft_engine_lock_error(&err) => {
                last_error = Some(err);
                thread::sleep(Duration::from_millis(5 * (attempt + 1)));
            }
            Err(err) => return Err(map_raft_engine_error(err)),
        }
    }
    Err(map_raft_engine_error(
        last_error.expect("raft-engine open retry loop records lock errors"),
    ))
}

fn raft_engine_lock_error(err: &raft_engine::Error) -> bool {
    err.to_string().contains("Failed to lock file")
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
    let mut hard_state = if let Some(bytes) = engine.get(raft_group_id, HARD_STATE_KEY) {
        HardState::decode(bytes.as_slice()).map_err(map_prost_error)?
    } else {
        HardState::default()
    };
    // The unapplied tail must be materialized for raft-rs MemStorage, so
    // memory here is proportional to retained-log backlog. Healthy operation
    // bounds that via snapshot/compaction; this budget turns the pathological
    // case (compaction stalled for a long time) into an actionable startup
    // error instead of an OOM. The backlog length is the blast radius.
    let replay_max_bytes = std::env::var("AGENTDB_RAFT_REPLAY_MAX_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(512 * 1024 * 1024);
    let mut replayed_bytes: u64 = 0;
    let mut budget_exceeded = false;
    let mut entries_by_index = BTreeMap::new();
    engine
        .scan_raw_messages(
            raft_group_id,
            Some(b"entry/"),
            Some(b"entry0"),
            false,
            |_, raw| {
                replayed_bytes = replayed_bytes.saturating_add(raw.len() as u64);
                if replayed_bytes > replay_max_bytes {
                    budget_exceeded = true;
                    return false;
                }
                if let Ok(entry) = Entry::decode(raw) {
                    entries_by_index.insert(entry.index, entry);
                }
                true
            },
        )
        .map_err(map_raft_engine_error)?;
    if budget_exceeded {
        return Err(Error::Other(format!(
            "raft group {raft_group_id} retained log exceeds              AGENTDB_RAFT_REPLAY_MAX_BYTES={replay_max_bytes}; compact the log              (snapshot catch-up) or raise the budget before restarting"
        )));
    }
    let entries: Vec<_> = entries_by_index
        .into_values()
        .filter(|entry| entry.index > snapshot_index)
        .collect();
    if !entries.is_empty() {
        storage.wl().append(&entries).map_err(map_raft_error)?;
    }
    let last_index = entries
        .last()
        .map(|entry| entry.index)
        .unwrap_or(snapshot_index);
    if hard_state.commit > last_index {
        hard_state.set_commit(last_index);
    }
    storage.wl().set_hardstate(hard_state.clone());
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
        group.elect(1).unwrap();
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
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
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
    fn raft_rs_log_can_append_after_compacting_through_current_commit() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        logs[0].append(7, b"before-compaction".to_vec()).unwrap();
        let current = logs[0].append(7, b"compact-through-me".to_vec()).unwrap();

        logs[0].compact(7, current.index).unwrap();
        let after = logs[0].append(7, b"after-compaction".to_vec()).unwrap();

        assert!(after.index > current.index);
        let entries = logs[0].committed_entries(7, current.index, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command, b"after-compaction");
    }

    #[test]
    fn raft_rs_log_can_campaign_after_compacting_replicated_entry() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        let current = logs[0]
            .append(7, b"replicated-before-campaign".to_vec())
            .unwrap();
        assert!(logs[2]
            .committed_entries(7, 0, 10)
            .unwrap()
            .iter()
            .any(|entry| entry.index == current.index));

        logs[2].compact(7, current.index).unwrap();

        group.campaign(3).unwrap();
    }

    #[test]
    fn read_barrier_confirms_on_healthy_leader_and_times_out_when_isolated() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        // Commit something so applied advances past the empty log.
        logs[0].append(7, b"before-barrier".to_vec()).unwrap();

        // Healthy leader: the barrier confirms and returns an index at or
        // above the committed entry.
        let index = group
            .read_barrier(1, Duration::from_secs(5))
            .expect("healthy leader confirms read barrier");
        assert!(index >= 1, "read index {index} should cover the commit");

        // A non-leader cannot serve a linearizable read.
        assert!(group.read_barrier(2, Duration::from_millis(200)).is_err());

        // Isolate the leader from both followers: ReadIndex can no longer
        // confirm leadership against a quorum, so the barrier must time out
        // rather than fall back to a local read.
        group.mark_unavailable(2).unwrap();
        group.mark_unavailable(3).unwrap();
        let started = Instant::now();
        let result = group.read_barrier(1, Duration::from_millis(500));
        assert!(
            result.is_err(),
            "isolated leader must not confirm a read barrier"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(500),
            "barrier should wait out its timeout before failing"
        );
    }

    #[test]
    fn raft_rs_transfer_leader_moves_leadership_to_transferee() {
        let (_dir, configs) = configs(3);
        let (group, _logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();

        group.transfer_leader(1, 2).unwrap();

        let statuses = group.status();
        assert!(statuses
            .iter()
            .any(|status| status.node_id == 2 && status.role == "Leader"));
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
        assert!(stable_raft_node_id("node-a").unwrap() <= i64::MAX as u64);
        assert!(stable_raft_node_id("node-b").unwrap() <= i64::MAX as u64);
        assert!(stable_raft_node_id("node-c").unwrap() <= i64::MAX as u64);
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
    fn raft_rs_log_rejects_append_on_non_leader() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();

        let err = logs[1]
            .append(7, b"stale-owner-write".to_vec())
            .unwrap_err();

        assert!(err.to_string().contains("is not leader"));
        let leader_status = group
            .status()
            .into_iter()
            .find(|status| status.node_id == 1)
            .unwrap();
        assert_eq!(leader_status.role, "Leader");
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
    fn raft_rs_snapshot_transfer_rejects_snapshot_behind_raw_applied() {
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
        {
            let mut guard = group.inner.lock().expect("raft-rs group lock poisoned");
            let node = guard.node_mut(3).unwrap();
            node.last_applied_index = stale.index;
        }

        let err = group.install_snapshot_transfer(3, stale).unwrap_err();

        assert!(err.to_string().contains("stale raft-rs snapshot"));
    }

    #[test]
    fn raft_rs_apply_committed_ignores_entries_below_applied_watermark() {
        let (_dir, configs) = configs(3);
        let (group, _logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        let mut guard = group.inner.lock().expect("raft-rs group lock poisoned");
        let node = guard.node_mut(1).unwrap();
        node.last_applied_index = 10;
        let stale = Entry {
            index: 9,
            term: 1,
            entry_type: EntryType::EntryNormal as i32,
            data: b"stale-entry".to_vec(),
            ..Entry::default()
        };

        node.apply_committed(vec![stale], 7).unwrap();

        assert_eq!(node.last_applied_index, 10);
        assert!(node.committed.is_empty());
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
    fn raft_rs_snapshot_transfer_advances_persisted_watermark() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        let commit = logs[0].append(7, b"before-snapshot".to_vec()).unwrap();
        let transfer = group
            .build_snapshot_transfer(1, 3, b"tablet-state-snapshot".to_vec())
            .unwrap();

        group.install_snapshot_transfer(3, transfer).unwrap();

        let guard = group.inner.lock().expect("raft-rs group lock poisoned");
        let node = guard.node(3).unwrap();
        assert!(node.raw.raft.raft_log.persisted >= commit.index);
        assert!(node.raw.raft.raft_log.applied >= commit.index);
    }

    #[test]
    fn raft_rs_snapshot_transfer_keeps_raw_node_apply_cursor_consistent() {
        let (_dir, configs) = configs(3);
        let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
        group.elect(1).unwrap();
        let snapshot_commit = logs[0].append(7, b"before-snapshot".to_vec()).unwrap();
        let transfer = group
            .build_snapshot_transfer(1, 3, b"tablet-state-snapshot".to_vec())
            .unwrap();
        group.install_snapshot_transfer(3, transfer).unwrap();

        let after_snapshot = logs[0].append(7, b"after-snapshot".to_vec()).unwrap();

        assert!(after_snapshot.index > snapshot_commit.index);
        assert!(logs[2]
            .committed_entries(7, snapshot_commit.index, 10)
            .unwrap()
            .iter()
            .any(|entry| entry.command == b"after-snapshot"));
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

        group_1.campaign(1).unwrap();
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
            if group_1
                .status()
                .iter()
                .any(|status| status.node_id == 1 && status.role == "Leader")
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

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

    #[test]
    fn raft_rs_wire_heartbeat_clamps_commit_ahead_of_local_log() {
        let (_dir, configs) = configs(3);
        let (group_2, _log_2) = RaftRsSharedGroup::open_local(7, configs, 2).unwrap();

        let mut heartbeat = Message::default();
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        heartbeat.from = 1;
        heartbeat.to = 2;
        heartbeat.term = 1;
        heartbeat.commit = 12;

        group_2
            .step_wire_message(RaftRsWireMessage::from_raft_message(7, &heartbeat))
            .unwrap();

        let status = group_2.status();
        assert!(status[0].commit_index < 12);
    }
}
