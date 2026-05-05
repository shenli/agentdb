//! Tablet rebalance planning and application.
//!
//! This module is deliberately metadata-only. It does not move bytes or
//! runtime-local state; it writes the same ownership/handoff metadata that
//! the admin API exposes. The scheduler and HTTP admin route both call this
//! module so operator-triggered and automatic convergence share one policy.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sqlx::AnyPool;

use crate::{nodes, tablets, MetaResult};

#[derive(Debug, Clone, Deserialize)]
pub struct RebalanceRequest {
    #[serde(default)]
    pub dry_run: bool,
    pub stale_threshold_ms: i64,
    pub max_movements: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RebalanceAction {
    pub action: &'static str,
    pub tablet_id: i64,
    pub source_node_id: Option<String>,
    pub target_node_id: Option<String>,
    pub changed: bool,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RebalanceReport {
    pub dry_run: bool,
    pub stale_threshold_ms: i64,
    pub eligible_node_count: usize,
    pub tablet_count: usize,
    pub owner_counts_before: HashMap<String, usize>,
    pub owner_counts_after: HashMap<String, usize>,
    pub actions: Vec<RebalanceAction>,
    pub skipped: Vec<RebalanceAction>,
}

impl RebalanceReport {
    pub fn changed_count(&self) -> usize {
        self.actions.iter().filter(|action| action.changed).count()
    }
}

pub async fn run(
    pool: &AnyPool,
    request: RebalanceRequest,
    now_ms: i64,
) -> MetaResult<RebalanceReport> {
    let nodes = nodes::list_all(pool).await?;
    let tablets = tablets::list_all(pool).await?;

    let fresh_alive_nodes: HashSet<String> = nodes
        .iter()
        .filter(|node| {
            node.state == "alive"
                && now_ms.saturating_sub(node.last_heartbeat_ms) <= request.stale_threshold_ms
        })
        .map(|node| node.node_id.clone())
        .collect();
    let node_state: HashMap<String, (String, bool)> = nodes
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                (
                    node.state.clone(),
                    now_ms.saturating_sub(node.last_heartbeat_ms) > request.stale_threshold_ms,
                ),
            )
        })
        .collect();

    let mut counts_before = owner_counts(&tablets);
    for node_id in &fresh_alive_nodes {
        counts_before.entry(node_id.clone()).or_insert(0);
    }
    let mut counts_after: HashMap<String, usize> = fresh_alive_nodes
        .iter()
        .map(|node_id| {
            let count = tablets
                .iter()
                .filter(|tablet| tablet.owner_node_id.as_ref() == Some(node_id))
                .count();
            (node_id.clone(), count)
        })
        .collect();
    let mut actions = Vec::new();
    let mut skipped = Vec::new();

    if fresh_alive_nodes.is_empty() {
        for tablet in tablets
            .iter()
            .filter(|tablet| tablet.owner_node_id.is_none())
        {
            skipped.push(RebalanceAction {
                action: "assign",
                tablet_id: tablet.tablet_id,
                source_node_id: None,
                target_node_id: None,
                changed: false,
                reason: "no_eligible_target",
            });
        }
        return Ok(RebalanceReport {
            dry_run: request.dry_run,
            stale_threshold_ms: request.stale_threshold_ms,
            eligible_node_count: 0,
            tablet_count: tablets.len(),
            owner_counts_before: counts_before,
            owner_counts_after: counts_after,
            actions,
            skipped,
        });
    }

    for tablet in tablets
        .iter()
        .filter(|tablet| tablet.owner_node_id.is_none())
    {
        if actions.len() >= request.max_movements {
            break;
        }
        let Some(target) = least_loaded_node(&counts_after) else {
            break;
        };
        let changed = if request.dry_run {
            true
        } else {
            tablets::assign_to(pool, tablet.tablet_id, &target).await?
        };
        if changed {
            *counts_after.entry(target.clone()).or_insert(0) += 1;
        }
        actions.push(RebalanceAction {
            action: "assign",
            tablet_id: tablet.tablet_id,
            source_node_id: None,
            target_node_id: Some(target),
            changed,
            reason: "unowned",
        });
    }

    for tablet in tablets.iter().filter(|tablet| {
        tablet
            .owner_node_id
            .as_ref()
            .and_then(|owner| node_state.get(owner).map(|(state, stale)| (state, *stale)))
            .is_some_and(|(state, stale)| state == "draining" && !stale)
    }) {
        if actions.len() >= request.max_movements {
            break;
        }
        let source = tablet.owner_node_id.clone();
        let Some(target) = least_loaded_node(&counts_after) else {
            break;
        };
        if source.as_deref() == Some(target.as_str()) {
            continue;
        }
        let changed = if request.dry_run {
            true
        } else {
            tablets::mark_for_handoff(pool, tablet.tablet_id, &target).await?
        };
        if changed {
            *counts_after.entry(target.clone()).or_insert(0) += 1;
        }
        actions.push(RebalanceAction {
            action: "handoff",
            tablet_id: tablet.tablet_id,
            source_node_id: source,
            target_node_id: Some(target),
            changed,
            reason: "source_draining",
        });
    }

    let mut balance_candidates: Vec<tablets::Tablet> = tablets
        .iter()
        .filter(|tablet| tablet.handoff_target_node_id.is_none())
        .filter(|tablet| {
            tablet
                .owner_node_id
                .as_ref()
                .is_some_and(|owner| fresh_alive_nodes.contains(owner))
        })
        .cloned()
        .collect();
    balance_candidates.sort_by_key(|tablet| tablet.tablet_id);
    while actions.len() < request.max_movements {
        let Some((source, source_count)) = most_loaded_node(&counts_after) else {
            break;
        };
        let Some((target, target_count)) = least_loaded_node_with_count(&counts_after) else {
            break;
        };
        if source_count <= target_count + 1 {
            break;
        }
        let Some(candidate_index) = balance_candidates
            .iter()
            .position(|tablet| tablet.owner_node_id.as_deref() == Some(source.as_str()))
        else {
            break;
        };
        let tablet = balance_candidates.remove(candidate_index);
        let changed = if request.dry_run {
            true
        } else {
            tablets::mark_for_handoff(pool, tablet.tablet_id, &target).await?
        };
        if changed {
            *counts_after.entry(source.clone()).or_insert(0) -= 1;
            *counts_after.entry(target.clone()).or_insert(0) += 1;
        }
        actions.push(RebalanceAction {
            action: "handoff",
            tablet_id: tablet.tablet_id,
            source_node_id: Some(source),
            target_node_id: Some(target),
            changed,
            reason: "load_balance",
        });
    }

    for tablet in tablets.iter().filter(|tablet| {
        tablet.owner_node_id.as_ref().is_some_and(|owner| {
            node_state
                .get(owner)
                .is_none_or(|(state, stale)| *stale || state == "dead")
        })
    }) {
        skipped.push(RebalanceAction {
            action: "handoff",
            tablet_id: tablet.tablet_id,
            source_node_id: tablet.owner_node_id.clone(),
            target_node_id: least_loaded_node(&counts_after),
            changed: false,
            reason: "source_not_cooperative",
        });
    }

    Ok(RebalanceReport {
        dry_run: request.dry_run,
        stale_threshold_ms: request.stale_threshold_ms,
        eligible_node_count: fresh_alive_nodes.len(),
        tablet_count: tablets.len(),
        owner_counts_before: counts_before,
        owner_counts_after: counts_after,
        actions,
        skipped,
    })
}

fn owner_counts(tablets: &[tablets::Tablet]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for tablet in tablets {
        if let Some(owner) = &tablet.owner_node_id {
            *counts.entry(owner.clone()).or_insert(0) += 1;
        }
    }
    counts
}

fn least_loaded_node(counts: &HashMap<String, usize>) -> Option<String> {
    least_loaded_node_with_count(counts).map(|(node, _)| node)
}

fn least_loaded_node_with_count(counts: &HashMap<String, usize>) -> Option<(String, usize)> {
    counts
        .iter()
        .min_by(|(left_node, left_count), (right_node, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| left_node.cmp(right_node))
        })
        .map(|(node, count)| (node.clone(), *count))
}

fn most_loaded_node(counts: &HashMap<String, usize>) -> Option<(String, usize)> {
    counts
        .iter()
        .max_by(|(left_node, left_count), (right_node, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_node.cmp(left_node))
        })
        .map(|(node, count)| (node.clone(), *count))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> sqlx::AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn assigns_unowned_and_marks_cooperative_handoffs() {
        let p = pool().await;
        nodes::upsert(&p, "node-a", "a", None, "alive", 1_000, None, None)
            .await
            .unwrap();
        nodes::upsert(&p, "node-b", "b", None, "alive", 1_000, None, None)
            .await
            .unwrap();
        nodes::upsert(&p, "node-c", "c", None, "draining", 1_000, None, None)
            .await
            .unwrap();
        for id in 1..=4 {
            tablets::get_or_create(&p, id, 100).await.unwrap();
        }
        tablets::assign_to(&p, 1, "node-a").await.unwrap();
        tablets::assign_to(&p, 2, "node-a").await.unwrap();
        tablets::assign_to(&p, 3, "node-c").await.unwrap();

        let report = run(
            &p,
            RebalanceRequest {
                dry_run: false,
                stale_threshold_ms: 15_000,
                max_movements: 8,
            },
            2_000,
        )
        .await
        .unwrap();

        assert!(report
            .actions
            .iter()
            .any(|action| action.action == "assign" && action.tablet_id == 4));
        assert!(report
            .actions
            .iter()
            .any(|action| action.action == "handoff"
                && action.source_node_id.as_deref() == Some("node-c")));
        assert!(report.changed_count() >= 2);
    }
}
