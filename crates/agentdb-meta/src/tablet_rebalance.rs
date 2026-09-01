//! Tablet rebalance planning and application.
//!
//! This module is deliberately metadata-only. It does not move bytes or
//! runtime-local state; it writes the same ownership/handoff metadata that
//! the admin API exposes. The scheduler and HTTP admin route both call this
//! module so operator-triggered and automatic convergence share one policy.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sqlx::AnyPool;

use crate::{
    cells::{self, Cell, DEFAULT_CELL_ID},
    node_operational,
    nodes::{self, Node},
    placement_scopes::{self, PlacementScope},
    tablet_replicas::{self, TabletReplica},
    tablets,
    tenant_policies::{self, TenantPolicy},
    MetaResult,
};

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
    let cells = cells::list_all(pool).await?;
    let placement_scopes = placement_scopes::list_all(pool).await?;
    let tenant_policies = tenant_policies::list_all(pool).await?;
    let placement = PlacementContext::new(&nodes, &cells, &placement_scopes, &tenant_policies);

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
    let fresh_alive_node_refs: HashMap<String, &Node> = nodes
        .iter()
        .filter(|node| {
            node.state == "alive"
                && now_ms.saturating_sub(node.last_heartbeat_ms) <= request.stale_threshold_ms
        })
        .map(|node| (node.node_id.clone(), node))
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
        let step = complete_ready_replacement_intent(
            pool,
            tablet,
            request.dry_run,
            now_ms,
            assignment_lease_expires_at_ms(&request, now_ms),
        )
        .await?;
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
        let Some(target) = select_unowned_tablet_target(
            pool,
            tablet,
            &counts_after,
            &fresh_alive_node_refs,
            &placement,
            now_ms,
        )
        .await?
        else {
            skipped.push(RebalanceAction {
                action: "assign",
                tablet_id: tablet.tablet_id,
                source_node_id: None,
                target_node_id: None,
                changed: false,
                reason: "no_eligible_target",
            });
            continue;
        };
        let changed = if request.dry_run {
            true
        } else {
            let lease_expires_at_ms = assignment_lease_expires_at_ms(&request, now_ms);
            let assigned =
                tablets::assign_to_with_lease(pool, tablet.tablet_id, &target, lease_expires_at_ms)
                    .await?;
            if assigned {
                tablet_replicas::upsert_intent(pool, tablet.tablet_id, &target, "voter", now_ms)
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
        let Some(target) = select_replacement_target(
            pool,
            tablet,
            source.as_deref(),
            &counts_after,
            &fresh_alive_node_refs,
            &placement,
            now_ms,
        )
        .await?
        else {
            skipped.push(RebalanceAction {
                action: "prepare_replacement",
                tablet_id: tablet.tablet_id,
                source_node_id: source,
                target_node_id: None,
                changed: false,
                reason: "no_eligible_target",
            });
            continue;
        };
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
        let Some((candidate_index, source, target)) = select_load_balance_move(
            pool,
            &balance_candidates,
            &counts_after,
            &fresh_alive_node_refs,
            &placement,
            now_ms,
        )
        .await?
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
        tablet
            .owner_node_id
            .as_ref()
            .is_some_and(|owner| match node_state.get(owner) {
                Some((state, stale)) => *stale || state == "dead",
                None => true,
            })
    }) {
        skipped.push(RebalanceAction {
            action: "handoff",
            tablet_id: tablet.tablet_id,
            source_node_id: tablet.owner_node_id.clone(),
            target_node_id: None,
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
    lease_expires_at_ms: i64,
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
    let replicas =
        node_operational::merged_replica_view_by_tablet(pool, tablet.tablet_id, cutoff_ms(now_ms))
            .await?;
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
        tablet_replicas::upsert_intent(pool, tablet.tablet_id, target_node_id, "voter", now_ms)
            .await?;
        let changed = tablets::complete_handoff_with_lease(
            pool,
            tablet.tablet_id,
            source_node_id,
            lease_expires_at_ms,
        )
        .await?
        .is_some();
        if changed {
            let _ = tablet_replicas::set_state(
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
    let intent = tablet_replicas::list_intent_by_tablet(pool, tablet_id).await?;
    let source_role = source_node_id.and_then(|source| {
        intent
            .iter()
            .find(|row| row.node_id == source)
            .map(|row| row.role.clone())
    });
    let mut changed = false;
    if let Some(source_node_id) = source_node_id {
        if source_role.as_deref() != Some("voter") {
            tablet_replicas::upsert_intent(pool, tablet_id, source_node_id, "voter", now_ms)
                .await?;
            changed = true;
        }
    }
    let target_role = intent
        .iter()
        .find(|row| row.node_id == target_node_id)
        .map(|row| row.role.clone());
    if target_role.as_deref() != Some("learner") {
        tablet_replicas::upsert_intent(pool, tablet_id, target_node_id, "learner", now_ms).await?;
        changed = true;
    }
    let handoff_changed = tablets::mark_for_handoff(pool, tablet_id, target_node_id).await?;
    Ok(changed || handoff_changed)
}

fn cutoff_ms(now_ms: i64) -> i64 {
    now_ms.saturating_sub(node_operational::DEFAULT_STALENESS_MS)
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

async fn select_unowned_tablet_target(
    pool: &AnyPool,
    tablet: &tablets::Tablet,
    counts: &HashMap<String, usize>,
    fresh_alive_nodes: &HashMap<String, &Node>,
    placement: &PlacementContext<'_>,
    now_ms: i64,
) -> MetaResult<Option<String>> {
    let replicas =
        node_operational::merged_replica_view_by_tablet(pool, tablet.tablet_id, cutoff_ms(now_ms))
            .await?;
    let eligible = eligible_targets(tablet, &replicas, None, false, fresh_alive_nodes, placement);
    if eligible.is_empty() {
        return Ok(None);
    }
    let best_ready_index = replicas
        .iter()
        .filter(|replica| {
            eligible.contains(&replica.node_id)
                && replica.role == "voter"
                && replica.health == "healthy"
        })
        .map(|replica| replica.applied_index)
        .max();
    let Some(best_ready_index) = best_ready_index else {
        return Ok(select_best_target(
            tablet,
            &replicas,
            eligible,
            counts,
            fresh_alive_nodes,
            placement,
        ));
    };
    let candidates: HashSet<String> = replicas
        .into_iter()
        .filter(|replica| {
            eligible.contains(&replica.node_id)
                && replica.role == "voter"
                && replica.health == "healthy"
                && replica.applied_index == best_ready_index
        })
        .map(|replica| replica.node_id)
        .collect();
    Ok(select_best_target(
        tablet,
        &[],
        candidates,
        counts,
        fresh_alive_nodes,
        placement,
    ))
}

async fn select_replacement_target(
    pool: &AnyPool,
    tablet: &tablets::Tablet,
    source_node_id: Option<&str>,
    counts: &HashMap<String, usize>,
    fresh_alive_nodes: &HashMap<String, &Node>,
    placement: &PlacementContext<'_>,
    now_ms: i64,
) -> MetaResult<Option<String>> {
    let replicas =
        node_operational::merged_replica_view_by_tablet(pool, tablet.tablet_id, cutoff_ms(now_ms))
            .await?;
    let eligible = eligible_targets(
        tablet,
        &replicas,
        source_node_id,
        true,
        fresh_alive_nodes,
        placement,
    );
    Ok(select_best_target(
        tablet,
        &replicas,
        eligible,
        counts,
        fresh_alive_nodes,
        placement,
    ))
}

async fn select_load_balance_move(
    pool: &AnyPool,
    candidates: &[tablets::Tablet],
    counts: &HashMap<String, usize>,
    fresh_alive_nodes: &HashMap<String, &Node>,
    placement: &PlacementContext<'_>,
    now_ms: i64,
) -> MetaResult<Option<(usize, String, String)>> {
    let mut order: Vec<_> = candidates.iter().enumerate().collect();
    order.sort_by(|(_, left), (_, right)| {
        let left_count = left
            .owner_node_id
            .as_ref()
            .and_then(|node| counts.get(node))
            .copied()
            .unwrap_or(0);
        let right_count = right
            .owner_node_id
            .as_ref()
            .and_then(|node| counts.get(node))
            .copied()
            .unwrap_or(0);
        right_count
            .cmp(&left_count)
            .then_with(|| left.tablet_id.cmp(&right.tablet_id))
    });
    for (index, tablet) in order {
        let Some(source) = tablet.owner_node_id.as_deref() else {
            continue;
        };
        let source_count = counts.get(source).copied().unwrap_or(0);
        let Some(target) = select_replacement_target(
            pool,
            tablet,
            Some(source),
            counts,
            fresh_alive_nodes,
            placement,
            now_ms,
        )
        .await?
        else {
            continue;
        };
        let target_count = counts.get(&target).copied().unwrap_or(0);
        if source_count > target_count + 1 {
            return Ok(Some((index, source.to_string(), target)));
        }
    }
    Ok(None)
}

struct PlacementContext<'a> {
    nodes_by_id: HashMap<&'a str, &'a Node>,
    cells_by_id: HashMap<&'a str, &'a Cell>,
    scopes_by_id: HashMap<&'a str, &'a PlacementScope>,
    policies_by_scope: HashMap<(&'a str, &'a str), Vec<&'a TenantPolicy>>,
}

impl<'a> PlacementContext<'a> {
    fn new(
        nodes: &'a [Node],
        cells: &'a [Cell],
        scopes: &'a [PlacementScope],
        policies: &'a [TenantPolicy],
    ) -> Self {
        let mut policies_by_scope: HashMap<(&'a str, &'a str), Vec<&'a TenantPolicy>> =
            HashMap::new();
        for policy in policies {
            policies_by_scope
                .entry((policy.cell_id.as_str(), policy.placement_scope_id.as_str()))
                .or_default()
                .push(policy);
        }
        Self {
            nodes_by_id: nodes
                .iter()
                .map(|node| (node.node_id.as_str(), node))
                .collect(),
            cells_by_id: cells
                .iter()
                .map(|cell| (cell.cell_id.as_str(), cell))
                .collect(),
            scopes_by_id: scopes
                .iter()
                .map(|scope| (scope.placement_scope_id.as_str(), scope))
                .collect(),
            policies_by_scope,
        }
    }

    fn tablet_cell_id(&self, tablet: &tablets::Tablet) -> &str {
        self.scopes_by_id
            .get(tablet.placement_scope_id.as_str())
            .map(|scope| scope.cell_id.as_str())
            .unwrap_or(DEFAULT_CELL_ID)
    }

    fn cell_for_tablet(&self, tablet: &tablets::Tablet) -> Cell {
        let cell_id = self.tablet_cell_id(tablet);
        self.cells_by_id
            .get(cell_id)
            .copied()
            .cloned()
            .unwrap_or_else(|| {
                let mut default = Cell::default_cell();
                default.cell_id = cell_id.to_string();
                default
            })
    }

    fn effective_tier(&self, tablet: &tablets::Tablet) -> &str {
        let cell_id = self.tablet_cell_id(tablet);
        self.policies_by_scope
            .get(&(cell_id, tablet.placement_scope_id.as_str()))
            .and_then(|policies| policies.iter().max_by_key(|policy| tier_rank(&policy.tier)))
            .map(|policy| policy.tier.as_str())
            .unwrap_or("free")
    }
}

fn eligible_targets(
    tablet: &tablets::Tablet,
    replicas: &[TabletReplica],
    source_node_id: Option<&str>,
    exclude_existing_replicas: bool,
    fresh_alive_nodes: &HashMap<String, &Node>,
    placement: &PlacementContext<'_>,
) -> HashSet<String> {
    let cell = placement.cell_for_tablet(tablet);
    let tier = placement.effective_tier(tablet);
    if !cells::tier_allowed_in_domain(tier, &cell.domain_class, &cell.capacity_class) {
        return HashSet::new();
    }
    let existing_replicas: HashSet<&str> = replicas
        .iter()
        .filter(|replica| Some(replica.node_id.as_str()) != source_node_id)
        .map(|replica| replica.node_id.as_str())
        .collect();
    fresh_alive_nodes
        .iter()
        .filter_map(|(node_id, node)| {
            if node.cell_id != cell.cell_id {
                return None;
            }
            if Some(node_id.as_str()) == source_node_id {
                return None;
            }
            if exclude_existing_replicas && existing_replicas.contains(node_id.as_str()) {
                return None;
            }
            if !cells::tier_allowed_in_domain(tier, &cell.domain_class, &node.capacity_class) {
                return None;
            }
            Some(node_id.clone())
        })
        .collect()
}

fn select_best_target(
    tablet: &tablets::Tablet,
    replicas: &[TabletReplica],
    candidates: HashSet<String>,
    counts: &HashMap<String, usize>,
    fresh_alive_nodes: &HashMap<String, &Node>,
    placement: &PlacementContext<'_>,
) -> Option<String> {
    let cell = placement.cell_for_tablet(tablet);
    let existing_voter_azs: HashSet<&str> = replicas
        .iter()
        .filter(|replica| replica.role == "voter")
        .filter_map(|replica| {
            placement
                .nodes_by_id
                .get(replica.node_id.as_str())
                .and_then(|node| node.availability_zone.as_deref())
        })
        .collect();
    let cell_az_count = fresh_alive_nodes
        .values()
        .filter(|node| node.cell_id == cell.cell_id)
        .filter_map(|node| node.availability_zone.as_deref())
        .collect::<HashSet<_>>()
        .len();
    candidates
        .into_iter()
        .filter_map(|node_id| {
            let node = fresh_alive_nodes.get(&node_id)?;
            let az_score = deployment_az_score(
                &cell.deployment_profile,
                node.availability_zone.as_deref(),
                &existing_voter_azs,
                cell_az_count,
            );
            Some((
                node_id,
                az_score,
                counts.get(node.node_id.as_str()).copied().unwrap_or(0),
            ))
        })
        .min_by(
            |(left_node, left_az_score, left_count), (right_node, right_az_score, right_count)| {
                left_az_score
                    .cmp(right_az_score)
                    .then_with(|| left_count.cmp(right_count))
                    .then_with(|| left_node.cmp(right_node))
            },
        )
        .map(|(node_id, _, _)| node_id)
}

fn deployment_az_score(
    deployment_profile: &str,
    candidate_az: Option<&str>,
    existing_voter_azs: &HashSet<&str>,
    cell_az_count: usize,
) -> usize {
    match deployment_profile {
        "multi_az_standard" | "enterprise" if cell_az_count > existing_voter_azs.len() => {
            if candidate_az.is_some_and(|az| !existing_voter_azs.contains(az)) {
                0
            } else {
                1
            }
        }
        "single_az_cluster" if existing_voter_azs.len() == 1 => {
            if candidate_az.is_some_and(|az| existing_voter_azs.contains(az)) {
                0
            } else {
                1
            }
        }
        _ => 0,
    }
}

fn tier_rank(tier: &str) -> usize {
    match tier {
        "regulated" => 4,
        "enterprise" => 3,
        "paid" => 2,
        "developer" => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> sqlx::AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    /// Seed both placement intent and an operational observation, the way
    /// the pre-stage-1 `tablet_replicas::upsert` used to in one row.
    #[allow(clippy::too_many_arguments)]
    async fn seed_replica(
        p: &sqlx::AnyPool,
        tablet_id: i64,
        node_id: &str,
        role: &str,
        health: &str,
        applied: i64,
        matched: i64,
        now_ms: i64,
    ) {
        tablet_replicas::upsert_intent(p, tablet_id, node_id, role, now_ms)
            .await
            .unwrap();
        let mut summary = node_operational::list_all(p)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.node_id == node_id)
            .map(|r| r.summary)
            .unwrap_or_default();
        summary
            .replicas
            .retain(|o| !(o.tablet_id == tablet_id && o.node_id == node_id));
        summary.replicas.push(node_operational::ReplicaObservation {
            tablet_id,
            node_id: node_id.into(),
            role: role.into(),
            health: health.into(),
            applied_index: applied,
            match_index: matched,
        });
        node_operational::report(p, node_id, now_ms, &summary)
            .await
            .unwrap();
    }

    async fn merged_by_tablet(p: &sqlx::AnyPool, tablet_id: i64) -> Vec<TabletReplica> {
        node_operational::merged_replica_view_by_tablet(p, tablet_id, 0)
            .await
            .unwrap()
    }

    async fn cell(
        p: &sqlx::AnyPool,
        cell_id: &str,
        domain_class: &str,
        deployment_profile: &str,
        capacity_class: &str,
    ) {
        cells::upsert(
            p,
            cell_id,
            Some("us-west-2"),
            domain_class,
            deployment_profile,
            capacity_class,
            "shared_logical",
            "serving",
            Some(cell_id),
            Some(cell_id),
            0,
            100,
        )
        .await
        .unwrap();
    }

    async fn scoped_tablet(p: &sqlx::AnyPool, tablet_id: i64, scope_id: &str, cell_id: &str) {
        placement_scopes::upsert_with_cell(
            p,
            scope_id,
            "dedicated_tenant",
            16,
            None,
            "serving",
            cell_id,
            100,
        )
        .await
        .unwrap();
        tablets::create_with_placement(p, tablet_id, scope_id, "serving", None, 0, 100)
            .await
            .unwrap();
    }

    async fn node_with_cell(
        p: &sqlx::AnyPool,
        node_id: &str,
        state: &str,
        cell_id: &str,
        az: &str,
        capacity_class: &str,
    ) {
        nodes::upsert_with_cell(
            p,
            node_id,
            node_id,
            None,
            state,
            1_000,
            Some(az),
            Some("us-west-2"),
            cell_id,
            capacity_class,
        )
        .await
        .unwrap();
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
        let replicas = merged_by_tablet(&p, 3).await;
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id == "node-c" && replica.role == "voter"));
        assert!(replicas
            .iter()
            .any(|replica| replica.node_id != "node-c" && replica.role == "learner"));
        let assigned_replicas = merged_by_tablet(&p, 4).await;
        // Assignment writes placement INTENT; observed health arrives later
        // through node reports, so a fresh assignment reads as "unknown".
        assert!(assigned_replicas
            .iter()
            .any(|replica| replica.role == "voter"));
        let assigned = tablets::get(&p, 4).await.unwrap().unwrap();
        assert!(assigned.lease_expires_at_ms > 2_000);
        assert_eq!(assigned.epoch, 1);
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
        seed_replica(&p, 7, "node-a", "voter", "healthy", 100, 100, 100).await;
        seed_replica(&p, 7, "node-b", "voter", "healthy", 90, 90, 100).await;

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
        assert!(tablet.lease_expires_at_ms > 2_000);
        assert_eq!(tablet.epoch, 1);
        let owner_replica = merged_by_tablet(&p, 7)
            .await
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
            seed_replica(&p, id, "node-a", "voter", "healthy", 10, 10, 100).await;
        }
        seed_replica(&p, 7, "node-b", "learner", "healthy", 10, 10, 100).await;
        seed_replica(&p, 8, "node-b", "learner", "healthy", 9, 9, 100).await;

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
        let target = merged_by_tablet(&p, 7)
            .await
            .into_iter()
            .find(|replica| replica.node_id == "node-b")
            .unwrap();
        assert_eq!(target.role, "voter");
        let source = merged_by_tablet(&p, 7)
            .await
            .into_iter()
            .find(|replica| replica.node_id == "node-a")
            .unwrap();
        assert_eq!(source.health, "draining");
        let lagging = tablets::get(&p, 8).await.unwrap().unwrap();
        assert_eq!(lagging.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(lagging.handoff_target_node_id.as_deref(), Some("node-b"));
    }

    #[tokio::test]
    async fn refuses_cross_cell_assignment() {
        let p = pool().await;
        cell(&p, "cell-a", "standard", "single_az_cluster", "stable").await;
        cell(&p, "cell-b", "standard", "single_az_cluster", "stable").await;
        node_with_cell(&p, "node-b", "alive", "cell-b", "us-west-2a", "stable").await;
        scoped_tablet(&p, 50, "tenant/a", "cell-a").await;

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

        assert!(report.skipped.iter().any(|action| {
            action.action == "assign"
                && action.tablet_id == 50
                && action.reason == "no_eligible_target"
        }));
        let tablet = tablets::get(&p, 50).await.unwrap().unwrap();
        assert_eq!(tablet.owner_node_id, None);
    }

    #[tokio::test]
    async fn single_az_profile_prefers_replacement_in_existing_voter_az() {
        let p = pool().await;
        cell(&p, "cell-a", "standard", "single_az_cluster", "stable").await;
        node_with_cell(&p, "node-a", "draining", "cell-a", "us-west-2a", "stable").await;
        node_with_cell(&p, "node-b", "alive", "cell-a", "us-west-2a", "stable").await;
        node_with_cell(&p, "node-c", "alive", "cell-a", "us-west-2b", "stable").await;
        scoped_tablet(&p, 60, "tenant/a", "cell-a").await;
        tablets::assign_to(&p, 60, "node-a").await.unwrap();
        seed_replica(&p, 60, "node-a", "voter", "healthy", 10, 10, 100).await;

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
            action.action == "prepare_replacement"
                && action.tablet_id == 60
                && action.target_node_id.as_deref() == Some("node-b")
        }));
    }

    #[tokio::test]
    async fn multi_az_profile_prefers_replacement_in_distinct_az() {
        let p = pool().await;
        cell(&p, "cell-a", "standard", "multi_az_standard", "stable").await;
        node_with_cell(&p, "node-a", "draining", "cell-a", "us-west-2a", "stable").await;
        node_with_cell(&p, "node-b", "alive", "cell-a", "us-west-2b", "stable").await;
        node_with_cell(&p, "node-c", "alive", "cell-a", "us-west-2a", "stable").await;
        scoped_tablet(&p, 61, "tenant/a", "cell-a").await;
        tablets::assign_to(&p, 61, "node-a").await.unwrap();
        seed_replica(&p, 61, "node-a", "voter", "healthy", 10, 10, 100).await;

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
            action.action == "prepare_replacement"
                && action.tablet_id == 61
                && action.target_node_id.as_deref() == Some("node-b")
        }));
    }

    #[tokio::test]
    async fn paid_tenant_is_not_automatically_placed_in_economy_domain() {
        let p = pool().await;
        cell(
            &p,
            "cell-econ",
            "economy",
            "single_az_cluster",
            "preemptible",
        )
        .await;
        node_with_cell(
            &p,
            "node-econ",
            "alive",
            "cell-econ",
            "us-west-2a",
            "stable",
        )
        .await;
        scoped_tablet(&p, 70, "tenant/paid", "cell-econ").await;
        tenant_policies::upsert_with_cell(
            &p,
            "paid-tenant",
            "dedicated",
            "tenant/paid",
            "tenant",
            None,
            0,
            1,
            "cell-econ",
            "paid",
            10,
            100,
        )
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

        assert!(report.skipped.iter().any(|action| {
            action.action == "assign"
                && action.tablet_id == 70
                && action.reason == "no_eligible_target"
        }));
        assert_eq!(
            tablets::get(&p, 70).await.unwrap().unwrap().owner_node_id,
            None
        );
    }

    #[tokio::test]
    async fn free_tenant_can_land_in_economy_domain() {
        let p = pool().await;
        cell(
            &p,
            "cell-econ",
            "economy",
            "single_az_cluster",
            "preemptible",
        )
        .await;
        node_with_cell(
            &p,
            "node-econ",
            "alive",
            "cell-econ",
            "us-west-2a",
            "preemptible",
        )
        .await;
        scoped_tablet(&p, 71, "shared/free", "cell-econ").await;
        tenant_policies::upsert_with_cell(
            &p,
            "free-tenant",
            "shared",
            "shared/free",
            "pool",
            None,
            0,
            1,
            "cell-econ",
            "free",
            0,
            100,
        )
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
                && action.tablet_id == 71
                && action.target_node_id.as_deref() == Some("node-econ")
        }));
        assert_eq!(
            tablets::get(&p, 71)
                .await
                .unwrap()
                .unwrap()
                .owner_node_id
                .as_deref(),
            Some("node-econ")
        );
    }
}
