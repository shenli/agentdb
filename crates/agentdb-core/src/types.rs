use serde::{Deserialize, Serialize};

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
}
