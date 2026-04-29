//! Wire DTOs for the V2.0 HTTP routes.
//!
//! These are deliberately separate from the runtime types: HTTP path params
//! already carry `tenant_id`, `agent_id`, `session_id`, so the request bodies
//! avoid repeating them. Responses serialize the runtime types directly (no
//! shape change needed there) — only the request shapes need adapting.

use serde::Deserialize;

use agentdb_core::{ArtifactRef, SourceRef};

#[derive(Debug, Deserialize)]
pub struct CreateTenantBody {
    pub external_key: String,
    #[serde(default = "default_policy")]
    pub policy: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateAgentBody {
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default = "default_policy")]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionBody {
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AppendBody {
    pub agent_id: String,
    pub kind: String,
    #[serde(default = "default_policy")]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub source_refs: Vec<SourceRef>,
}

#[derive(Debug, Deserialize)]
pub struct AppendAndFetchBody {
    pub agent_id: String,
    pub kind: String,
    #[serde(default = "default_policy")]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub source_refs: Vec<SourceRef>,
    #[serde(default = "default_last")]
    pub last: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
}

#[derive(Debug, Deserialize)]
pub struct FetchQuery {
    #[serde(default)]
    pub from_seq: u64,
    #[serde(default = "default_fetch_limit")]
    pub limit: usize,
}

fn default_policy() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

fn default_last() -> usize {
    20
}

fn default_max_bytes() -> usize {
    1_000_000
}

fn default_fetch_limit() -> usize {
    100
}
