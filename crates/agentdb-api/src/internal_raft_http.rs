use std::sync::Arc;

use base64::Engine as _;
use reqwest::StatusCode;
use serde::Serialize;

use agentdb_storage::{RaftRsSnapshotTransfer, RaftRsWireMessage};

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

#[derive(Debug)]
pub enum RaftRsHttpError {
    Request(reqwest::Error),
    Rejected { status: StatusCode, body: String },
}

impl std::fmt::Display for RaftRsHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(err) => write!(f, "raft-rs HTTP request failed: {err}"),
            Self::Rejected { status, body } => {
                write!(f, "raft-rs HTTP request rejected with {status}: {body}")
            }
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
