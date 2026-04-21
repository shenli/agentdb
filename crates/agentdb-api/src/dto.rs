use serde::{Deserialize, Serialize};

#[derive(Deserialize, utoipa::ToSchema)]
pub struct AppendAndFetchRequest {
    pub event: serde_json::Value,
    #[serde(default)]
    pub fetch_last_n: usize,
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
    pub payload: serde_json::Value,
}

impl From<agentdb_core::Event> for EventDto {
    fn from(e: agentdb_core::Event) -> Self {
        Self {
            seq: e.seq,
            ts: e.ts,
            payload: e.payload,
        }
    }
}
