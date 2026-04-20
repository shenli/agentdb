use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Tracks segment-file object-store paths currently pinned by open
/// `ReadCtx` instances for one box. Multiple `ReadCtx`s may pin the
/// same path; the registry refcounts.
#[derive(Debug, Default)]
pub struct SegmentPinRegistry {
    counts: Mutex<HashMap<String, usize>>,
}

impl SegmentPinRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one pin for `path`. Idempotent per ReadCtx via the caller's
    /// own local set — see `ReadCtx::pinned_segments`.
    pub fn pin(&self, path: &str) {
        let mut m = self.counts.lock();
        *m.entry(path.to_string()).or_insert(0) += 1;
    }

    /// Release one pin per entry in `paths`. Entries whose count reaches
    /// zero are removed from the map.
    pub fn release(&self, paths: BTreeSet<String>) {
        let mut m = self.counts.lock();
        for p in paths {
            if let Some(c) = m.get_mut(&p) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    m.remove(&p);
                }
            }
        }
    }

    /// Set of currently pinned paths. Used by `SegmentGc`.
    pub fn pinned_paths(&self) -> HashSet<String> {
        self.counts.lock().keys().cloned().collect()
    }

    /// Number of distinct paths currently pinned. Used for observability.
    pub fn pinned_len(&self) -> u64 {
        self.counts.lock().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_and_release_single_path() {
        let r = SegmentPinRegistry::new();
        r.pin("a");
        assert!(r.pinned_paths().contains("a"));
        r.release([String::from("a")].into_iter().collect());
        assert!(!r.pinned_paths().contains("a"));
        assert_eq!(r.pinned_len(), 0);
    }

    #[test]
    fn refcount_multiple_pins_same_path() {
        let r = SegmentPinRegistry::new();
        r.pin("a");
        r.pin("a");
        r.release([String::from("a")].into_iter().collect());
        assert!(r.pinned_paths().contains("a"), "still pinned: count was 2");
        r.release([String::from("a")].into_iter().collect());
        assert!(!r.pinned_paths().contains("a"));
    }

    #[test]
    fn release_unknown_path_is_noop() {
        let r = SegmentPinRegistry::new();
        r.release([String::from("nope")].into_iter().collect());
        assert_eq!(r.pinned_len(), 0);
    }

    #[test]
    fn pinned_len_counts_distinct_paths_only() {
        let r = SegmentPinRegistry::new();
        r.pin("a");
        r.pin("a");
        r.pin("b");
        assert_eq!(r.pinned_len(), 2);
    }
}
