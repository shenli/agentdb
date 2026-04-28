use serde::{Deserialize, Serialize};

#[derive(Deserialize, utoipa::ToSchema)]
pub struct AppendAndFetchRequest {
    pub event: serde_json::Value,
    #[serde(default)]
    pub fetch_last_n: usize,
    /// Optional agent identifier to attribute this event to. See
    /// `docs/agent-db-v2.md` §2.3 for the agent-as-metadata stance —
    /// free-form string, no pre-registration, no separate resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct AppendAndFetchResponse {
    pub seq: u64,
    pub recent: Vec<EventDto>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct EventDto {
    pub seq: u64,
    pub ts: u64,
    /// Agent attribution, if any. Mirrors `Event::actor_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    pub payload: serde_json::Value,
}

impl From<agentdb_core::Event> for EventDto {
    fn from(e: agentdb_core::Event) -> Self {
        Self {
            seq: e.seq,
            ts: e.ts,
            actor_id: e.actor_id,
            payload: e.payload,
        }
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct FetchRecentRequest {
    pub n: usize,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct FetchRecentResponse {
    pub recent: Vec<EventDto>,
}
