use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use base64::Engine as _;
use reqwest::StatusCode;
use serde::Serialize;

use agentdb_storage::{RaftRsSharedGroup, RaftRsSnapshotTransfer, RaftRsWireMessage};

use crate::raft_ingress::RaftRsGroupRegistry;

const INTERNAL_AUTH_HEADER: &str = "X-AgentDB-Internal-Token";

#[derive(Clone)]
pub struct RaftRsHttpClient {
    client: reqwest::Client,
    token: Arc<str>,
}

impl RaftRsHttpClient {
    pub fn new(token: impl Into<Arc<str>>) -> Self {
        Self {
            client: reqwest::Client::new(),
            token: token.into(),
        }
    }

    pub fn with_client(client: reqwest::Client, token: impl Into<Arc<str>>) -> Self {
        Self {
            client,
            token: token.into(),
        }
    }

    pub async fn send_message(
        &self,
        base_url: &str,
        message: &RaftRsWireMessage,
    ) -> Result<(), RaftRsHttpError> {
        let body = RaftMessageBody {
            tablet_id: message.tablet_id,
            from: message.from,
            to: message.to,
            payload_base64: encode_payload(&message.payload),
        };
        self.post(base_url, "/api/internal/raft/messages", &body)
            .await
    }

    pub async fn send_snapshot(
        &self,
        base_url: &str,
        snapshot: &RaftRsSnapshotTransfer,
    ) -> Result<(), RaftRsHttpError> {
        let body = RaftSnapshotBody {
            tablet_id: snapshot.tablet_id,
            from: snapshot.from,
            to: snapshot.to,
            index: snapshot.index,
            term: snapshot.term,
            payload_base64: encode_payload(&snapshot.payload),
        };
        self.post(base_url, "/api/internal/raft/snapshots", &body)
            .await
    }

    async fn post<T: Serialize + ?Sized>(
        &self,
        base_url: &str,
        path: &str,
        body: &T,
    ) -> Result<(), RaftRsHttpError> {
        let url = format!("{}{}", base_url.trim_end_matches('/'), path);
        let response = self
            .client
            .post(url)
            .header(INTERNAL_AUTH_HEADER, self.token.as_ref())
            .json(body)
            .send()
            .await
            .map_err(RaftRsHttpError::Request)?;
        let status = response.status();
        if status == StatusCode::ACCEPTED {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(RaftRsHttpError::Rejected { status, body })
        }
    }
}

#[derive(Clone)]
pub struct RaftRsHttpOutbox {
    client: RaftRsHttpClient,
    peer_urls: Arc<RwLock<BTreeMap<u64, String>>>,
}

impl RaftRsHttpOutbox {
    pub fn new(token: impl Into<Arc<str>>) -> Self {
        Self {
            client: RaftRsHttpClient::new(token),
            peer_urls: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn with_client(client: RaftRsHttpClient) -> Self {
        Self {
            client,
            peer_urls: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn set_peer_url(&self, node_id: u64, base_url: impl Into<String>) {
        self.peer_urls
            .write()
            .expect("raft-rs HTTP peer map lock poisoned")
            .insert(node_id, base_url.into());
    }

    pub fn remove_peer_url(&self, node_id: u64) -> Option<String> {
        self.peer_urls
            .write()
            .expect("raft-rs HTTP peer map lock poisoned")
            .remove(&node_id)
    }

    pub fn replace_peer_urls(&self, peer_urls: BTreeMap<u64, String>) {
        *self
            .peer_urls
            .write()
            .expect("raft-rs HTTP peer map lock poisoned") = peer_urls;
    }

    pub async fn flush_group_once(
        &self,
        group: &RaftRsSharedGroup,
    ) -> Result<usize, RaftRsHttpError> {
        let messages = group.take_wire_messages();
        let mut sent = 0;
        for (index, message) in messages.iter().cloned().enumerate() {
            let Some(base_url) = self.peer_url(message.to) else {
                group
                    .restore_wire_messages(messages[index..].to_vec())
                    .map_err(RaftRsHttpError::Storage)?;
                return Err(RaftRsHttpError::MissingPeerUrl);
            };
            match self.client.send_message(&base_url, &message).await {
                Ok(()) => sent += 1,
                Err(err) => {
                    group
                        .restore_wire_messages(messages[index..].to_vec())
                        .map_err(RaftRsHttpError::Storage)?;
                    return Err(err);
                }
            }
        }
        Ok(sent)
    }

    pub async fn flush_registry_once(
        &self,
        registry: &RaftRsGroupRegistry,
    ) -> Result<usize, RaftRsHttpError> {
        let mut sent = 0;
        for (_, group) in registry.groups_snapshot() {
            sent += self.flush_group_once(&group).await?;
        }
        Ok(sent)
    }

    pub async fn send_snapshot_to_peer(
        &self,
        snapshot: &RaftRsSnapshotTransfer,
    ) -> Result<(), RaftRsHttpError> {
        let base_url = self
            .peer_url(snapshot.to)
            .ok_or(RaftRsHttpError::MissingPeerUrl)?;
        self.client.send_snapshot(&base_url, snapshot).await
    }

    pub async fn send_message_to_peer(
        &self,
        message: &RaftRsWireMessage,
    ) -> Result<(), RaftRsHttpError> {
        let base_url = self
            .peer_url(message.to)
            .ok_or(RaftRsHttpError::MissingPeerUrl)?;
        self.client.send_message(&base_url, message).await
    }

    fn peer_url(&self, node_id: u64) -> Option<String> {
        self.peer_urls
            .read()
            .expect("raft-rs HTTP peer map lock poisoned")
            .get(&node_id)
            .cloned()
    }
}

pub fn spawn_raft_rs_outbox_worker(
    registry: Arc<RaftRsGroupRegistry>,
    outbox: RaftRsHttpOutbox,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let interval = interval.max(Duration::from_millis(100));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let _ = outbox.flush_registry_once(&registry).await;
        }
    })
}

#[derive(Debug)]
pub enum RaftRsHttpError {
    Request(reqwest::Error),
    Rejected { status: StatusCode, body: String },
    MissingPeerUrl,
    Storage(agentdb_storage::Error),
}

impl std::fmt::Display for RaftRsHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(err) => write!(f, "raft-rs HTTP request failed: {err}"),
            Self::Rejected { status, body } => {
                write!(f, "raft-rs HTTP request rejected with {status}: {body}")
            }
            Self::MissingPeerUrl => write!(f, "raft-rs HTTP peer URL is not configured"),
            Self::Storage(err) => write!(f, "raft-rs storage error: {err}"),
        }
    }
}

impl std::error::Error for RaftRsHttpError {}

#[derive(Serialize)]
struct RaftMessageBody {
    tablet_id: i64,
    from: u64,
    to: u64,
    payload_base64: String,
}

#[derive(Serialize)]
struct RaftSnapshotBody {
    tablet_id: i64,
    from: u64,
    to: u64,
    index: u64,
    term: u64,
    payload_base64: String,
}

fn encode_payload(payload: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(payload)
}
