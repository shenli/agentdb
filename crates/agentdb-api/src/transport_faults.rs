//! Deterministic transport fault injection for the internal raft path.
//!
//! Test-only link controls keyed by `(tablet_id, source, target)`, sitting
//! at the PRODUCTION transport boundary: the outbox consults the table
//! before a send, the ingress handler consults it before stepping a
//! received message. Tests therefore exercise the same serialization,
//! authentication, routing, outbox, and dispatch path as a real message —
//! a parallel test transport that bypassed production framing would not
//! provide the same evidence.
//!
//! The table is inert unless the server was started with
//! `AGENTDB_TRANSPORT_FAULTS=1`; without the gate no table exists and the
//! hot path pays a single `Option` check.
//!
//! Actions (see `docs/design/distributed-safety-hardening.md` §2):
//! - `drop`: the message vanishes, like network loss. On the send side it
//!   is treated as sent; on the receive side it is acknowledged (202) but
//!   never stepped.
//! - `delay_ms`: the message is delivered after a bounded pause.
//! - `reject`: a typed transport error — the sender sees a send failure
//!   and restores the message; a receiver answers 503.
//! - `hold`: the message is parked under the rule's key and re-injected
//!   in FIFO order when the key is released — deterministic reordering.
//!
//! Every rule match increments a per-rule counter so a scenario can prove
//! the fault actually fired (testing-evidence contract: a mutation
//! experiment confirms the mutation landed).

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, RwLock,
    },
};

use agentdb_storage::RaftRsWireMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultDirection {
    Send,
    Recv,
    Both,
}

impl FaultDirection {
    fn matches(self, send_side: bool) -> bool {
        match self {
            FaultDirection::Both => true,
            FaultDirection::Send => send_side,
            FaultDirection::Recv => !send_side,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum FaultAction {
    Drop,
    Delay { ms: u64 },
    Reject,
    Hold,
}

/// Delays are bounded so a typo cannot wedge an outbox flush for minutes.
pub const MAX_DELAY_MS: u64 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultRule {
    /// Unique key: names the rule in counters and in `release`.
    pub key: String,
    /// `None` matches every tablet.
    #[serde(default)]
    pub tablet_id: Option<i64>,
    /// Raft node id of the message source; `None` matches all.
    #[serde(default)]
    pub from: Option<u64>,
    /// Raft node id of the message target; `None` matches all.
    #[serde(default)]
    pub to: Option<u64>,
    pub direction: FaultDirection,
    #[serde(flatten)]
    pub action: FaultAction,
}

impl FaultRule {
    fn matches(&self, tablet_id: i64, from: u64, to: u64, send_side: bool) -> bool {
        self.direction.matches(send_side)
            && self.tablet_id.is_none_or(|rule| rule == tablet_id)
            && self.from.is_none_or(|rule| rule == from)
            && self.to.is_none_or(|rule| rule == to)
    }
}

/// A message parked by a `hold` rule, tagged with the side it was
/// intercepted on so release can re-inject it correctly (send-held
/// messages go back to the group outbox; recv-held messages are stepped).
#[derive(Debug, Clone)]
pub struct HeldMessage {
    pub send_side: bool,
    pub message: RaftRsWireMessage,
}

#[derive(Debug, Default)]
struct RuleCounter {
    matched: AtomicU64,
}

#[derive(Debug, Default)]
pub struct FaultTable {
    rules: RwLock<Vec<FaultRule>>,
    counters: RwLock<BTreeMap<String, RuleCounter>>,
    held: Mutex<BTreeMap<String, Vec<HeldMessage>>>,
}

/// The action the transport layer must take for one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultVerdict {
    Pass,
    Drop,
    Delay {
        ms: u64,
    },
    Reject,
    /// The message was consumed into the held store under this key.
    Held {
        key: String,
    },
}

impl FaultTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Evaluate one message against the table. First matching rule wins —
    /// rule order is the deterministic schedule. `Hold` consumes the
    /// message into the held store.
    pub fn evaluate(
        &self,
        tablet_id: i64,
        from: u64,
        to: u64,
        send_side: bool,
        message: &RaftRsWireMessage,
    ) -> FaultVerdict {
        let rules = self.rules.read().expect("fault rules lock poisoned");
        for rule in rules.iter() {
            if !rule.matches(tablet_id, from, to, send_side) {
                continue;
            }
            self.bump(&rule.key);
            return match &rule.action {
                FaultAction::Drop => FaultVerdict::Drop,
                FaultAction::Delay { ms } => FaultVerdict::Delay {
                    ms: (*ms).min(MAX_DELAY_MS),
                },
                FaultAction::Reject => FaultVerdict::Reject,
                FaultAction::Hold => {
                    self.held
                        .lock()
                        .expect("fault held lock poisoned")
                        .entry(rule.key.clone())
                        .or_default()
                        .push(HeldMessage {
                            send_side,
                            message: message.clone(),
                        });
                    FaultVerdict::Held {
                        key: rule.key.clone(),
                    }
                }
            };
        }
        FaultVerdict::Pass
    }

    fn bump(&self, key: &str) {
        if let Some(counter) = self
            .counters
            .read()
            .expect("fault counters lock poisoned")
            .get(key)
        {
            counter.matched.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.counters
            .write()
            .expect("fault counters lock poisoned")
            .entry(key.to_string())
            .or_default()
            .matched
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Replace the whole rule set. Held messages for keys that no longer
    /// exist stay parked until released or cleared — releasing them is the
    /// scenario driver's responsibility, not a side effect.
    pub fn replace(&self, rules: Vec<FaultRule>) {
        *self.rules.write().expect("fault rules lock poisoned") = rules;
    }

    /// Drop all rules AND all held messages. Returns how many held
    /// messages were discarded (a scenario tearing down mid-hold should
    /// know it destroyed messages).
    pub fn clear(&self) -> usize {
        self.rules
            .write()
            .expect("fault rules lock poisoned")
            .clear();
        let discarded: usize = {
            let mut held = self.held.lock().expect("fault held lock poisoned");
            let count = held.values().map(Vec::len).sum();
            held.clear();
            count
        };
        self.counters
            .write()
            .expect("fault counters lock poisoned")
            .clear();
        discarded
    }

    /// Take every message parked under `key`, in arrival (FIFO) order.
    pub fn release(&self, key: &str) -> Vec<HeldMessage> {
        self.held
            .lock()
            .expect("fault held lock poisoned")
            .remove(key)
            .unwrap_or_default()
    }

    pub fn snapshot(&self) -> FaultTableSnapshot {
        let rules = self
            .rules
            .read()
            .expect("fault rules lock poisoned")
            .clone();
        let counters = self
            .counters
            .read()
            .expect("fault counters lock poisoned")
            .iter()
            .map(|(key, counter)| (key.clone(), counter.matched.load(Ordering::Relaxed)))
            .collect();
        let held = self
            .held
            .lock()
            .expect("fault held lock poisoned")
            .iter()
            .map(|(key, messages)| (key.clone(), messages.len()))
            .collect();
        FaultTableSnapshot {
            rules,
            matched: counters,
            held,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultTableSnapshot {
    pub rules: Vec<FaultRule>,
    /// Rule key -> how many messages matched it.
    pub matched: BTreeMap<String, u64>,
    /// Rule key -> how many messages are currently parked.
    pub held: BTreeMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(tablet_id: i64, from: u64, to: u64) -> RaftRsWireMessage {
        RaftRsWireMessage {
            tablet_id,
            from,
            to,
            payload: vec![1, 2, 3],
        }
    }

    #[test]
    fn first_matching_rule_wins_and_counts() {
        let table = FaultTable::new();
        table.replace(vec![
            FaultRule {
                key: "drop-a-to-b".into(),
                tablet_id: Some(1),
                from: Some(10),
                to: Some(20),
                direction: FaultDirection::Send,
                action: FaultAction::Drop,
            },
            FaultRule {
                key: "reject-all".into(),
                tablet_id: None,
                from: None,
                to: None,
                direction: FaultDirection::Both,
                action: FaultAction::Reject,
            },
        ]);
        let msg = message(1, 10, 20);
        assert_eq!(table.evaluate(1, 10, 20, true, &msg), FaultVerdict::Drop);
        // Recv side skips the send-only rule and falls through to reject.
        assert_eq!(table.evaluate(1, 10, 20, false, &msg), FaultVerdict::Reject);
        // Unmatched tablet on send side also falls through to reject.
        assert_eq!(table.evaluate(2, 10, 20, true, &msg), FaultVerdict::Reject);
        let snapshot = table.snapshot();
        assert_eq!(snapshot.matched["drop-a-to-b"], 1);
        assert_eq!(snapshot.matched["reject-all"], 2);
    }

    #[test]
    fn hold_parks_fifo_and_release_drains() {
        let table = FaultTable::new();
        table.replace(vec![FaultRule {
            key: "hold-b".into(),
            tablet_id: None,
            from: None,
            to: Some(20),
            direction: FaultDirection::Send,
            action: FaultAction::Hold,
        }]);
        for seq in 0..3u64 {
            let msg = message(1, seq, 20);
            assert_eq!(
                table.evaluate(1, seq, 20, true, &msg),
                FaultVerdict::Held {
                    key: "hold-b".into()
                }
            );
        }
        let held = table.release("hold-b");
        assert_eq!(
            held.iter().map(|h| h.message.from).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "release preserves arrival order"
        );
        assert!(table.release("hold-b").is_empty());
    }

    #[test]
    fn delay_is_clamped_and_clear_reports_discards() {
        let table = FaultTable::new();
        table.replace(vec![FaultRule {
            key: "slow".into(),
            tablet_id: None,
            from: None,
            to: None,
            direction: FaultDirection::Both,
            action: FaultAction::Delay { ms: 999_999 },
        }]);
        let msg = message(1, 1, 2);
        assert_eq!(
            table.evaluate(1, 1, 2, true, &msg),
            FaultVerdict::Delay { ms: MAX_DELAY_MS }
        );
        table.replace(vec![FaultRule {
            key: "park".into(),
            tablet_id: None,
            from: None,
            to: None,
            direction: FaultDirection::Both,
            action: FaultAction::Hold,
        }]);
        table.evaluate(1, 1, 2, true, &msg);
        assert_eq!(table.clear(), 1);
        assert_eq!(table.evaluate(1, 1, 2, true, &msg), FaultVerdict::Pass);
    }
}
