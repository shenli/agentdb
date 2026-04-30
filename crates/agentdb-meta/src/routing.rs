//! Multi-node routing planner.
//!
//! This module is deliberately pure: it consumes snapshots from the existing
//! `nodes`, `tablets`, `shards`, and `session_locators` tables and returns the
//! routing action the data plane should take. Keeping this layer side-effect
//! free makes the scale-out contract easy to test before the HTTP runtime starts
//! forwarding requests between nodes.

use std::collections::BTreeMap;

use crate::nodes::Node;
use crate::session_locators::SessionLocator;
use crate::shards::ShardRow;
use crate::tablets::Tablet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidencyPolicy {
    /// Empty means "no placement restriction". Values are compared to the
    /// owning node's durable region. Nodes without region metadata fall back
    /// to `availability_zone` for compatibility with older deployments that
    /// used AZ as the prototype location label.
    pub allowed_locations: Vec<String>,
    /// When false, a request carrying a location that differs from the owning
    /// node's location is rejected instead of forwarded cross-location.
    pub allow_cross_location: bool,
}

impl Default for ResidencyPolicy {
    fn default() -> Self {
        Self {
            allowed_locations: Vec::new(),
            allow_cross_location: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRequest<'a> {
    pub tenant_id: &'a str,
    pub session_locator: Option<&'a SessionLocator>,
    pub request_location: Option<&'a str>,
    pub local_node_id: Option<&'a str>,
    pub shard_count: i64,
    pub now_ms: i64,
    pub policy: ResidencyPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    pub action: RouteAction,
    pub shard_id: i64,
    pub tablet_id: i64,
    pub owner_node_id: Option<String>,
    pub owner_address: Option<String>,
    pub owner_location: Option<String>,
    pub epoch: Option<i64>,
    pub reason: Option<RouteReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteAction {
    /// The local node owns the tablet and can serve the request.
    Local,
    /// Another node owns the tablet and the caller may forward to it.
    Forward,
    /// The request must fail without forwarding.
    Reject,
    /// No live leased owner exists yet; placement or failover must run first.
    Unassigned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteReason {
    MissingTablet,
    MissingOwner,
    ExpiredLease,
    MissingNode,
    NodeUnavailable,
    ResidencyDenied,
    WrongLocation,
    KeyUnavailable,
}

pub fn plan_route(
    request: RouteRequest<'_>,
    shards: &[ShardRow],
    tablets: &[Tablet],
    nodes: &[Node],
) -> RouteDecision {
    assert!(request.shard_count > 0, "shard_count must be positive");

    let shard_id = request
        .session_locator
        .map(|locator| i64::from(locator.shard_id))
        .unwrap_or_else(|| stable_shard(request.tenant_id, request.shard_count));
    let tablet_id = request
        .session_locator
        .and_then(|locator| locator.tablet_id)
        .or_else(|| shard_override(shards, shard_id))
        .unwrap_or(shard_id);

    let tablets_by_id: BTreeMap<_, _> = tablets
        .iter()
        .map(|tablet| (tablet.tablet_id, tablet))
        .collect();
    let nodes_by_id: BTreeMap<_, _> = nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node))
        .collect();

    let Some(tablet) = tablets_by_id.get(&tablet_id) else {
        return unresolved(shard_id, tablet_id, RouteReason::MissingTablet);
    };
    let Some(owner_node_id) = tablet.owner_node_id.as_deref() else {
        return unresolved(shard_id, tablet_id, RouteReason::MissingOwner);
    };
    if tablet.lease_expires_at_ms <= request.now_ms {
        return unresolved(shard_id, tablet_id, RouteReason::ExpiredLease);
    }
    let Some(owner) = nodes_by_id.get(owner_node_id) else {
        return unresolved(shard_id, tablet_id, RouteReason::MissingNode);
    };
    if owner.state != "alive" {
        return unresolved(shard_id, tablet_id, RouteReason::NodeUnavailable);
    }

    let owner_location = owner_route_location(owner).map(str::to_string);
    if !request.policy.allowed_locations.is_empty()
        && !owner_location.as_deref().is_some_and(|loc| {
            request
                .policy
                .allowed_locations
                .iter()
                .any(|allowed| allowed == loc)
        })
    {
        return reject(shard_id, tablet, owner, RouteReason::ResidencyDenied);
    }
    if !request.policy.allow_cross_location {
        if let (Some(request_location), Some(owner_location)) =
            (request.request_location, owner_location.as_deref())
        {
            if request_location != owner_location {
                return reject(shard_id, tablet, owner, RouteReason::WrongLocation);
            }
        }
    }

    let action = if request.local_node_id == Some(owner.node_id.as_str()) {
        RouteAction::Local
    } else {
        RouteAction::Forward
    };
    RouteDecision {
        action,
        shard_id,
        tablet_id,
        owner_node_id: Some(owner.node_id.clone()),
        owner_address: Some(owner.address.clone()),
        owner_location,
        epoch: Some(tablet.epoch),
        reason: None,
    }
}

pub fn stable_shard(tenant_id: &str, shard_count: i64) -> i64 {
    assert!(shard_count > 0, "shard_count must be positive");
    (fnv1a64(tenant_id.as_bytes()) % shard_count as u64) as i64
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn shard_override(shards: &[ShardRow], shard_id: i64) -> Option<i64> {
    shards
        .iter()
        .find(|row| row.shard_id == shard_id)
        .and_then(|row| row.tablet_id)
}

fn unresolved(shard_id: i64, tablet_id: i64, reason: RouteReason) -> RouteDecision {
    RouteDecision {
        action: RouteAction::Unassigned,
        shard_id,
        tablet_id,
        owner_node_id: None,
        owner_address: None,
        owner_location: None,
        epoch: None,
        reason: Some(reason),
    }
}

fn reject(shard_id: i64, tablet: &Tablet, owner: &Node, reason: RouteReason) -> RouteDecision {
    RouteDecision {
        action: RouteAction::Reject,
        shard_id,
        tablet_id: tablet.tablet_id,
        owner_node_id: Some(owner.node_id.clone()),
        owner_address: Some(owner.address.clone()),
        owner_location: owner_route_location(owner).map(str::to_string),
        epoch: Some(tablet.epoch),
        reason: Some(reason),
    }
}

fn owner_route_location(owner: &Node) -> Option<&str> {
    owner
        .region
        .as_deref()
        .or(owner.availability_zone.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, location: &str) -> Node {
        Node {
            node_id: id.into(),
            address: format!("http://{id}:8080"),
            mongo_address: None,
            state: "alive".into(),
            last_heartbeat_ms: 100,
            joined_at_ms: 100,
            availability_zone: Some(format!("{location}a")),
            region: Some(location.into()),
        }
    }

    fn tablet(id: i64, owner: Option<&str>, lease_expires_at_ms: i64) -> Tablet {
        Tablet {
            tablet_id: id,
            owner_node_id: owner.map(str::to_string),
            epoch: 7,
            lease_expires_at_ms,
            created_at_ms: 0,
            handoff_target_node_id: None,
        }
    }

    fn locator(session_id: &str, shard_id: i32, tablet_id: Option<i64>) -> SessionLocator {
        SessionLocator {
            session_id: session_id.into(),
            tenant_internal_id: 1,
            tenant_external_id: Some("ten_1".into()),
            tablet_id,
            shard_id,
            created_at_ms: 0,
        }
    }

    #[test]
    fn local_route_uses_session_locator_tablet_before_shard_override() {
        let request = RouteRequest {
            tenant_id: "ten_1",
            session_locator: Some(&locator("ses_1", 3, Some(99))),
            request_location: Some("us-west-2a"),
            local_node_id: Some("node-a"),
            shard_count: 16,
            now_ms: 1_000,
            policy: ResidencyPolicy::default(),
        };
        let decision = plan_route(
            request,
            &[ShardRow {
                shard_id: 3,
                tablet_id: Some(3),
                updated_at_ms: 0,
            }],
            &[tablet(99, Some("node-a"), 2_000)],
            &[node("node-a", "us-west-2a")],
        );

        assert_eq!(decision.action, RouteAction::Local);
        assert_eq!(decision.shard_id, 3);
        assert_eq!(decision.tablet_id, 99);
        assert_eq!(
            decision.owner_address.as_deref(),
            Some("http://node-a:8080")
        );
        assert_eq!(decision.epoch, Some(7));
    }

    #[test]
    fn forward_route_uses_shard_override_when_no_locator_exists() {
        let request = RouteRequest {
            tenant_id: "tenant-a",
            session_locator: None,
            request_location: Some("us-west-2a"),
            local_node_id: Some("node-a"),
            shard_count: 16,
            now_ms: 1_000,
            policy: ResidencyPolicy::default(),
        };
        let shard_id = stable_shard("tenant-a", 16);
        let decision = plan_route(
            request,
            &[ShardRow {
                shard_id,
                tablet_id: Some(42),
                updated_at_ms: 0,
            }],
            &[tablet(42, Some("node-b"), 2_000)],
            &[node("node-b", "us-west-2a")],
        );

        assert_eq!(decision.action, RouteAction::Forward);
        assert_eq!(decision.tablet_id, 42);
        assert_eq!(decision.owner_node_id.as_deref(), Some("node-b"));
    }

    #[test]
    fn expired_lease_is_unassigned_not_forwarded() {
        let request = RouteRequest {
            tenant_id: "ten_1",
            session_locator: Some(&locator("ses_1", 1, Some(1))),
            request_location: None,
            local_node_id: Some("node-a"),
            shard_count: 16,
            now_ms: 2_000,
            policy: ResidencyPolicy::default(),
        };
        let decision = plan_route(
            request,
            &[],
            &[tablet(1, Some("node-a"), 2_000)],
            &[node("node-a", "us-west-2a")],
        );

        assert_eq!(decision.action, RouteAction::Unassigned);
        assert_eq!(decision.reason, Some(RouteReason::ExpiredLease));
    }

    #[test]
    fn residency_policy_rejects_owner_outside_allowed_locations() {
        let request = RouteRequest {
            tenant_id: "ten_1",
            session_locator: Some(&locator("ses_1", 1, Some(1))),
            request_location: Some("eu-central-1a"),
            local_node_id: Some("node-local"),
            shard_count: 16,
            now_ms: 1_000,
            policy: ResidencyPolicy {
                allowed_locations: vec!["eu-central-1a".into()],
                allow_cross_location: false,
            },
        };
        let decision = plan_route(
            request,
            &[],
            &[tablet(1, Some("node-a"), 2_000)],
            &[node("node-a", "us-west-2a")],
        );

        assert_eq!(decision.action, RouteAction::Reject);
        assert_eq!(decision.reason, Some(RouteReason::ResidencyDenied));
        assert_eq!(decision.owner_location.as_deref(), Some("us-west-2a"));
    }

    #[test]
    fn residency_policy_prefers_region_over_availability_zone() {
        let mut owner = node("node-a", "us-west-2");
        owner.availability_zone = Some("us-west-2a".into());
        let request = RouteRequest {
            tenant_id: "ten_1",
            session_locator: Some(&locator("ses_1", 1, Some(1))),
            request_location: Some("us-west-2"),
            local_node_id: Some("node-local"),
            shard_count: 16,
            now_ms: 1_000,
            policy: ResidencyPolicy {
                allowed_locations: vec!["us-west-2".into()],
                allow_cross_location: false,
            },
        };
        let decision = plan_route(request, &[], &[tablet(1, Some("node-a"), 2_000)], &[owner]);

        assert_eq!(decision.action, RouteAction::Forward);
        assert_eq!(decision.owner_location.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn residency_policy_falls_back_to_availability_zone_without_region() {
        let mut owner = node("node-a", "us-west-2a");
        owner.region = None;
        owner.availability_zone = Some("us-west-2a".into());
        let request = RouteRequest {
            tenant_id: "ten_1",
            session_locator: Some(&locator("ses_1", 1, Some(1))),
            request_location: Some("us-west-2a"),
            local_node_id: Some("node-local"),
            shard_count: 16,
            now_ms: 1_000,
            policy: ResidencyPolicy {
                allowed_locations: vec!["us-west-2a".into()],
                allow_cross_location: false,
            },
        };
        let decision = plan_route(request, &[], &[tablet(1, Some("node-a"), 2_000)], &[owner]);

        assert_eq!(decision.action, RouteAction::Forward);
        assert_eq!(decision.owner_location.as_deref(), Some("us-west-2a"));
    }

    #[test]
    fn wrong_request_location_is_rejected_when_cross_location_is_disabled() {
        let request = RouteRequest {
            tenant_id: "ten_1",
            session_locator: Some(&locator("ses_1", 1, Some(1))),
            request_location: Some("eu-central-1a"),
            local_node_id: Some("node-local"),
            shard_count: 16,
            now_ms: 1_000,
            policy: ResidencyPolicy {
                allowed_locations: vec!["us-west-2a".into(), "eu-central-1a".into()],
                allow_cross_location: false,
            },
        };
        let decision = plan_route(
            request,
            &[],
            &[tablet(1, Some("node-a"), 2_000)],
            &[node("node-a", "us-west-2a")],
        );

        assert_eq!(decision.action, RouteAction::Reject);
        assert_eq!(decision.reason, Some(RouteReason::WrongLocation));
    }
}
