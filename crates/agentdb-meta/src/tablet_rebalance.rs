//! Tablet rebalance planning and application.
//!
//! This module is deliberately metadata-only. It does not move bytes or
//! runtime-local state; it writes the same ownership/handoff metadata that
//! the admin API exposes. The scheduler and HTTP admin route both call this
//! module so operator-triggered and automatic convergence share one policy.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sqlx::AnyPool;

use crate::{nodes, tablet_replicas, tablets, MetaResult};

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
    pub membership_operations: Vec<RebalanceMembershipOperation>,
}

impl RebalanceReport {
    pub fn changed_count(&self) -> usize {
        self.actions.iter().filter(|action| action.changed).count()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RebalanceMembershipOperation {
    pub tablet_id: i64,
    pub operation: &'static str,
    pub node_id: String,
    pub raft_node_id: Option<u64>,
    pub status: &'static str,
    pub error: Option<String>,
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
            membership_operations: Vec::new(),
        });
    }

    for tablet in tablets
        .iter()
        .filter(|tablet| tablet.owner_node_id.is_some() && tablet.handoff_target_node_id.is_some())
    {
        if actions.len() >= request.max_movements {
            break;
        }
        let step = complete_ready_replacement_intent(pool, tablet, request.dry_run, now_ms).await?;
        if let Some(skipped_action) = step.skipped {
            skipped.push(skipped_action);
        }
        let Some(action) = step.action else {
            continue;
        };
        if action.changed {
            if let Some((source, target)) = &step.ownership_move {
                if let Some(count) = counts_after.get_mut(source) {
                    *count = count.saturating_sub(1);
                }
                *counts_after.entry(target.clone()).or_insert(0) += 1;
            }
        }
        actions.push(action);
    }

    for tablet in tablets
        .iter()
        .filter(|tablet| tablet.owner_node_id.is_none())
    {
        if actions.len() >= request.max_movements {
            break;
        }
        let Some(target) =
            select_unowned_tablet_target(pool, tablet.tablet_id, &counts_after, &fresh_alive_nodes)
                .await?
        else {
            break;
        };
        let changed = if request.dry_run {
            true
        } else {
            let assigned = tablets::assign_to(pool, tablet.tablet_id, &target).await?;
            if assigned {
                let target_replica = tablet_replicas::list_by_tablet(pool, tablet.tablet_id)
                    .await?
                    .into_iter()
                    .find(|replica| replica.node_id == target);
                let lease_expires_at_ms = assignment_lease_expires_at_ms(&request, now_ms);
                tablets::assume_ownership(
                    pool,
                    tablet.tablet_id,
                    tablet.epoch,
                    &target,
                    lease_expires_at_ms,
                )
                .await?;
                tablet_replicas::upsert(
                    pool,
                    tablet.tablet_id,
                    &target,
                    "voter",
                    "healthy",
                    target_replica
                        .as_ref()
                        .map(|replica| replica.applied_index)
                        .unwrap_or(0),
                    target_replica
                        .as_ref()
                        .map(|replica| replica.match_index)
                        .unwrap_or(0),
                    now_ms,
                )
                .await?;
            }
            assigned
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
        let changed = prepare_replacement_intent(
            pool,
            tablet.tablet_id,
            source.as_deref(),
            &target,
            request.dry_run,
            now_ms,
        )
        .await?;
        if changed {
            *counts_after.entry(target.clone()).or_insert(0) += 1;
        }
        actions.push(RebalanceAction {
            action: "prepare_replacement",
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
        let changed = prepare_replacement_intent(
            pool,
            tablet.tablet_id,
            Some(&source),
            &target,
            request.dry_run,
            now_ms,
        )
        .await?;
        if changed {
            *counts_after.entry(source.clone()).or_insert(0) -= 1;
            *counts_after.entry(target.clone()).or_insert(0) += 1;
        }
        actions.push(RebalanceAction {
            action: "prepare_replacement",
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
        membership_operations: Vec::new(),
    })
}

fn assignment_lease_expires_at_ms(request: &RebalanceRequest, now_ms: i64) -> i64 {
    let lease_ms = request
        .stale_threshold_ms
        .max(1)
        .saturating_mul(10)
        .max(30_000);
    now_ms.saturating_add(lease_ms)
}

struct RebalanceStep {
    action: Option<RebalanceAction>,
    skipped: Option<RebalanceAction>,
    ownership_move: Option<(String, String)>,
}

async fn complete_ready_replacement_intent(
    pool: &AnyPool,
    tablet: &tablets::Tablet,
    dry_run: bool,
    now_ms: i64,
) -> MetaResult<RebalanceStep> {
    let Some(source_node_id) = tablet.owner_node_id.as_ref() else {
        return Ok(RebalanceStep {
            action: None,
            skipped: None,
            ownership_move: None,
        });
    };
    let Some(target_node_id) = tablet.handoff_target_node_id.as_ref() else {
        return Ok(RebalanceStep {
            action: None,
            skipped: None,
            ownership_move: None,
        });
    };
    let replicas = tablet_replicas::list_by_tablet(pool, tablet.tablet_id).await?;
    let source_replica = replicas
        .iter()
        .find(|replica| replica.node_id == *source_node_id);
    let Some(target_replica) = replicas
        .iter()
        .find(|replica| replica.node_id == *target_node_id)
    else {
        return Ok(RebalanceStep {
            action: None,
            skipped: Some(RebalanceAction {
                action: "complete_replacement",
                tablet_id: tablet.tablet_id,
                source_node_id: Some(source_node_id.clone()),
                target_node_id: Some(target_node_id.clone()),
                changed: false,
                reason: "target_replica_missing",
            }),
            ownership_move: None,
        });
    };
    let required_applied_index = source_replica
        .map(|replica| replica.applied_index)
        .unwrap_or(target_replica.applied_index);
    if target_replica.health != "healthy" {
        return Ok(RebalanceStep {
            action: None,
            skipped: Some(RebalanceAction {
                action: "complete_replacement",
                tablet_id: tablet.tablet_id,
                source_node_id: Some(source_node_id.clone()),
                target_node_id: Some(target_node_id.clone()),
                changed: false,
                reason: "target_not_healthy",
            }),
            ownership_move: None,
        });
    }
    if target_replica.applied_index < required_applied_index {
        return Ok(RebalanceStep {
            action: None,
            skipped: Some(RebalanceAction {
                action: "complete_replacement",
                tablet_id: tablet.tablet_id,
                source_node_id: Some(source_node_id.clone()),
                target_node_id: Some(target_node_id.clone()),
                changed: false,
                reason: "target_lagging",
            }),
            ownership_move: None,
        });
    }
    let changed = if dry_run {
        true
    } else {
        tablet_replicas::upsert(
            pool,
            tablet.tablet_id,
            target_node_id,
            "voter",
            &target_replica.health,
            target_replica.applied_index,
            target_replica.match_index,
            now_ms,
        )
        .await?;
        let changed = tablets::complete_handoff(pool, tablet.tablet_id, source_node_id)
            .await?
            .is_some();
        if changed {
            let _ = tablet_replicas::set_health(
                pool,
                tablet.tablet_id,
                source_node_id,
                "draining",
                now_ms,
            )
            .await?;
        }
        changed
    };
    Ok(RebalanceStep {
        action: Some(RebalanceAction {
            action: "complete_replacement",
            tablet_id: tablet.tablet_id,
            source_node_id: Some(source_node_id.clone()),
            target_node_id: Some(target_node_id.clone()),
            changed,
            reason: "target_caught_up",
        }),
        skipped: None,
        ownership_move: changed.then(|| (source_node_id.clone(), target_node_id.clone())),
    })
}

async fn prepare_replacement_intent(
    pool: &AnyPool,
    tablet_id: i64,
    source_node_id: Option<&str>,
    target_node_id: &str,
    dry_run: bool,
    now_ms: i64,
) -> MetaResult<bool> {
    if dry_run {
        return Ok(true);
    }
    let replicas = tablet_replicas::list_by_tablet(pool, tablet_id).await?;
    let source_replica =
        source_node_id.and_then(|source| replicas.iter().find(|replica| replica.node_id == source));
    let mut changed = false;
    if let Some(source_node_id) = source_node_id {
        if source_replica
            .map(|replica| replica.role != "voter")
            .unwrap_or(true)
        {
            tablet_replicas::upsert(
                pool,
                tablet_id,
                source_node_id,
                "voter",
                source_replica
                    .map(|replica| replica.health.as_str())
                    .unwrap_or("healthy"),
                source_replica
                    .map(|replica| replica.applied_index)
                    .unwrap_or(0),
                source_replica
                    .map(|replica| replica.match_index)
                    .unwrap_or(0),
                now_ms,
            )
            .await?;
            changed = true;
        }
    }
    let target_replica = replicas
        .iter()
        .find(|replica| replica.node_id == target_node_id);
    if target_replica
        .map(|replica| replica.role != "learner")
        .unwrap_or(true)
    {
        tablet_replicas::upsert(
            pool,
            tablet_id,
            target_node_id,
            "learner",
            target_replica
                .map(|replica| replica.health.as_str())
                .unwrap_or("lagging"),
            target_replica
                .map(|replica| replica.applied_index)
                .unwrap_or(0),
            target_replica
                .map(|replica| replica.match_index)
                .unwrap_or(0),
            now_ms,
        )
        .await?;
        changed = true;
    }
    let handoff_changed = tablets::mark_for_handoff(pool, tablet_id, target_node_id).await?;
    Ok(changed || handoff_changed)
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

async fn select_unowned_tablet_target(
    pool: &AnyPool,
    tablet_id: i64,
    counts: &HashMap<String, usize>,
    fresh_alive_nodes: &HashSet<String>,
) -> MetaResult<Option<String>> {
    let replicas = tablet_replicas::list_by_tablet(pool, tablet_id).await?;
    let best_ready_index = replicas
        .iter()
        .filter(|replica| {
            fresh_alive_nodes.contains(&replica.node_id)
                && replica.role == "voter"
                && replica.health == "healthy"
        })
        .map(|replica| replica.applied_index)
        .max();
    let Some(best_ready_index) = best_ready_index else {
        return Ok(least_loaded_node(counts));
    };
    let candidates: HashSet<String> = replicas
        .into_iter()
        .filter(|replica| {
            fresh_alive_nodes.contains(&replica.node_id)
                && replica.role == "voter"
                && replica.health == "healthy"
                && replica.applied_index == best_ready_index
        })
        .map(|replica| replica.node_id)
        .collect();
    Ok(counts
        .iter()
        .map(|(node_id, count)| (node_id.clone(), *count))
        .filter(|(node_id, _)| candidates.contains(node_id))
        .min_by(|(left_node, left_count), (right_node, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| left_node.cmp(right_node))
        })
        .map(|(node_id, _)| node_id))
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
            .any(|action| action.action == "prepare_replacement"
                && action.source_node_id.as_deref() == Some("node-c")));
        assert!(report.changed_count() >= 2);
        let replicas = tablet_replicas::list_by_tablet(&p, 3).await.unwrap();
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id == "node-c" && replica.role == "voter"));
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id != "node-c" && replica.role == "learner"));
        let assigned_replicas = tablet_replicas::list_by_tablet(&p, 4).await.unwrap();
        assert!(assigned_replicas
            .iter()
            .any(|replica| replica.role == "voter" && replica.health == "healthy"));
        let assigned = tablets::get(&p, 4).await.unwrap().unwrap();
        assert!(assigned.lease_expires_at_ms > 2_000);
    }

    #[tokio::test]
    async fn assigns_unowned_tablet_to_most_caught_up_healthy_voter() {
        let p = pool().await;
        nodes::upsert(&p, "node-a", "a", None, "alive", 1_000, None, None)
            .await
            .unwrap();
        nodes::upsert(&p, "node-b", "b", None, "alive", 1_000, None, None)
            .await
            .unwrap();
        tablets::get_or_create(&p, 7, 100).await.unwrap();
        tablet_replicas::upsert(&p, 7, "node-a", "voter", "healthy", 100, 100, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(&p, 7, "node-b", "voter", "healthy", 90, 90, 100)
            .await
            .unwrap();

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

        assert!(report.actions.iter().any(|action| {
            action.action == "assign"
                && action.tablet_id == 7
                && action.target_node_id.as_deref() == Some("node-a")
        }));
        let tablet = tablets::get(&p, 7).await.unwrap().unwrap();
        assert_eq!(tablet.owner_node_id.as_deref(), Some("node-a"));
        let owner_replica = tablet_replicas::list_by_tablet(&p, 7)
            .await
            .unwrap()
            .into_iter()
            .find(|replica| replica.node_id == "node-a")
            .unwrap();
        assert_eq!(owner_replica.applied_index, 100);
        assert_eq!(owner_replica.match_index, 100);
    }

    #[tokio::test]
    async fn completes_ready_replacements_and_skips_lagging_targets() {
        let p = pool().await;
        nodes::upsert(&p, "node-a", "a", None, "alive", 1_000, None, None)
            .await
            .unwrap();
        nodes::upsert(&p, "node-b", "b", None, "alive", 1_000, None, None)
            .await
            .unwrap();
        for id in 7..=8 {
            tablets::get_or_create(&p, id, 100).await.unwrap();
            tablets::assign_to(&p, id, "node-a").await.unwrap();
            tablets::mark_for_handoff(&p, id, "node-b").await.unwrap();
            tablet_replicas::upsert(&p, id, "node-a", "voter", "healthy", 10, 10, 100)
                .await
                .unwrap();
        }
        tablet_replicas::upsert(&p, 7, "node-b", "learner", "healthy", 10, 10, 100)
            .await
            .unwrap();
        tablet_replicas::upsert(&p, 8, "node-b", "learner", "healthy", 9, 9, 100)
            .await
            .unwrap();

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

        assert!(report.actions.iter().any(|action| {
            action.action == "complete_replacement"
                && action.tablet_id == 7
                && action.reason == "target_caught_up"
        }));
        assert!(report.skipped.iter().any(|action| {
            action.action == "complete_replacement"
                && action.tablet_id == 8
                && action.reason == "target_lagging"
        }));
        let completed = tablets::get(&p, 7).await.unwrap().unwrap();
        assert_eq!(completed.owner_node_id.as_deref(), Some("node-b"));
        assert_eq!(completed.handoff_target_node_id, None);
        let target = tablet_replicas::list_by_tablet(&p, 7)
            .await
            .unwrap()
            .into_iter()
            .find(|replica| replica.node_id == "node-b")
            .unwrap();
        assert_eq!(target.role, "voter");
        let source = tablet_replicas::list_by_tablet(&p, 7)
            .await
            .unwrap()
            .into_iter()
            .find(|replica| replica.node_id == "node-a")
            .unwrap();
        assert_eq!(source.health, "draining");
        let lagging = tablets::get(&p, 8).await.unwrap().unwrap();
        assert_eq!(lagging.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(lagging.handoff_target_node_id.as_deref(), Some("node-b"));
    }
}
