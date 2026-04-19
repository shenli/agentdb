use serde::{Deserialize, Serialize};

use agentdb_storage::{BoxId, SessionId, TenantId};

/// A schema-free document. Backed by serde_json for M2 simplicity;
/// will move to BSON when the Mongo wire protocol lands in M3.
pub type Document = serde_json::Value;

/// An append-only record inside a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    /// Unix epoch millis at write time.
    pub ts: u64,
    pub payload: Document,
}

/// Lifecycle tier of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Warm,
    Cold,
    Archive,
}

/// Persisted tenant record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    pub internal_id: TenantId,
    pub external_id: String,
    pub box_id: BoxId,
    pub created_at: u64,
}

/// Persisted session record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub internal_id: SessionId,
    pub external_id: String,
    pub tenant_internal_id: TenantId,
    pub box_id: BoxId,
    pub created_at: u64,
    pub last_active_at: u64,
    pub tier: Tier,
    pub next_seq: u64,
    /// When `tier == Cold`, the `object_store` path where events live. None for warm sessions.
    #[serde(default)]
    pub segment_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_roundtrips_through_json() {
        let original = Event {
            seq: 42,
            ts: 1_700_000_000_000,
            payload: json!({ "kind": "tool_call", "name": "search" }),
        };
        let text = serde_json::to_string(&original).unwrap();
        let back: Event = serde_json::from_str(&text).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn document_accepts_arbitrary_nesting() {
        let d: Document = json!({ "a": { "b": [1, 2, { "c": true }] } });
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"c\":true"));
    }

    #[test]
    fn session_roundtrips_through_json() {
        let s = Session {
            internal_id: 17,
            external_id: "s-9".into(),
            tenant_internal_id: 3,
            box_id: 1,
            created_at: 100,
            last_active_at: 150,
            tier: Tier::Warm,
            next_seq: 5,
            segment_path: None,
        };
        let text = serde_json::to_string(&s).unwrap();
        let back: Session = serde_json::from_str(&text).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn tier_serializes_as_lowercase_string() {
        let t = Tier::Archive;
        let s = serde_json::to_string(&t).unwrap();
        assert_eq!(s, "\"archive\"");
    }

    #[test]
    fn tenant_roundtrips_through_json() {
        let t = Tenant {
            internal_id: 100,
            external_id: "t-42".into(),
            box_id: 2,
            created_at: 1,
        };
        let text = serde_json::to_string(&t).unwrap();
        let back: Tenant = serde_json::from_str(&text).unwrap();
        assert_eq!(back, t);
    }
}
