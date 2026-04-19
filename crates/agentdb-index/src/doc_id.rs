use serde::{Deserialize, Serialize};

/// Internal document identifier scoped within a box.
///
/// Composed of `(session_internal_id, seq)` so the planner can filter the
/// candidate set to a specific session via cheap field comparison, and
/// so the caller can resolve a hit back to an `Event` through
/// `agentdb-core::primitives::scan_session_events` or a prefix lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocId {
    pub session_internal_id: u64,
    pub seq: u64,
}

impl DocId {
    pub fn new(session_internal_id: u64, seq: u64) -> Self {
        Self {
            session_internal_id,
            seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_lexicographic_over_session_then_seq() {
        let a = DocId::new(10, 1);
        let b = DocId::new(10, 2);
        let c = DocId::new(11, 0);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn roundtrips_through_serde_json() {
        let d = DocId::new(42, 7);
        let s = serde_json::to_string(&d).unwrap();
        let back: DocId = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }
}
