# Plan: Cluster Deployment Gap Closure

Date: 2026-05-04
Scope: close the product, API, automation, and backup/restore gaps between the
current single-node AgentDB deployment and a future supported multi-node
cluster deployment.

## Purpose

This plan defines the work needed to make AgentDB cluster deployment a real
operator path rather than a design direction.

It also locks down the deployment support matrix so implementation effort stays
focused.

## Support Matrix

### Single node

#### Primary supported deployment

- `RocksDB + local filesystem`

This is the default, boring, operator-friendly deployment for:

- local development with durable state
- self-hosted single-node runtime
- Docker / Compose / Helm single-replica evaluation

#### Testing and evaluation only

- `SlateDB + S3`

This mode exists to:

- validate the object-storage-first architecture
- test tiered session storage on real S3
- exercise the same storage model the future cluster will use
- support AWS validation and integration testing

It is **not** the primary recommended single-node production deployment.

### Cluster

#### Target supported deployment

- `SlateDB + S3`

Cluster mode should not support `RocksDB` as the cluster durable truth.

## Why This Matrix

### Why single-node should stay RocksDB-first

Single-node users usually want:

- simple install
- local durability
- low operational complexity
- straightforward backup/restore
- no dependency on cloud object storage

That points to `RocksDB + local filesystem`.

### Why single-node SlateDB + S3 still matters

It is useful as a validation path for:

- object-store-backed runtime metadata
- cold/archive/object-store behavior
- the future clustered storage model
- AWS smoke tests and disaster-recovery drills

### Why cluster should be SlateDB + S3 only

A real cluster should have:

- shared durable truth
- clean node replacement
- clean ownership handoff
- simpler backup/restore
- no dependency on one node's local RocksDB files being authoritative

Clustered `RocksDB` would make:

- failover harder
- replacement harder
- backup/restore uglier
- ownership movement more confusing

So it should be out of scope.

## Gap Categories

The cluster gap has four parts:

1. cluster control-plane features and APIs
2. cluster automation
3. backup/restore
4. operator visibility and day-2 workflows

## Phase 1: Freeze The Product Contract

Write the explicit cluster support contract before adding more APIs.

Decisions to freeze:

- cluster mode requires `SlateDB + S3`
- single-node production path is `RocksDB + local filesystem`
- single-node `SlateDB + S3` is testing/evaluation only
- cluster ownership moves by tablet/placement, not by tenant pinning
- drain is the standard pre-stop workflow
- object storage is the retained source of truth for cold/archive/artifacts

Deliverables:

- `docs/operations/DEPLOYMENT.md`
- `docs/operations/CLUSTER_STATUS.md`
- `docs/operations/CLUSTER_DEPLOYMENT.md`
- `docs/operations/CLUSTER_OPERATIONS.md`

## Phase 2: Enforce The Support Matrix In Config

The server should validate deployment mode explicitly.

### Valid combinations

- single-node + `RocksDB + local filesystem`
- single-node + `RocksDB + object-store content` if still useful
- single-node + `SlateDB + S3` for testing/evaluation
- cluster + `SlateDB + S3`

### Invalid combinations

- cluster + `RocksDB`
- cluster + local-only durable truth

Required work:

- explicit deployment mode in config
- startup validation with clear error messages
- doctor checks that explain supported combinations

Acceptance:

- a misconfigured cluster cannot start on `RocksDB`
- docs and validation say the same thing

## Phase 3: Complete The Minimum Cluster Operator API

Cluster mode needs a stable operator surface.

### Read APIs

- `GET /api/admin/nodes`
- `GET /api/admin/tablets`
- `GET /api/admin/routing`
- `GET /api/admin/cluster/health`
- `GET /api/admin/cluster/readiness`
- `GET /api/admin/backups`
- `GET /api/admin/restores`

Status:

- `GET /api/admin/routing` exists
- `GET /api/admin/cluster/health` exists
- `GET /api/admin/cluster/readiness` exists
- `GET /api/admin/nodes` and `GET /api/admin/tablets` are part of the
  broader control-plane/admin direction but are not yet presented here as a
  complete supported operator surface

### Write APIs

- `POST /api/admin/nodes/register`
- `POST /api/admin/drain`
- `POST /api/admin/nodes/{node_id}/maintenance`
- `POST /api/admin/tablets/create`
- `POST /api/admin/tablets/{tablet_id}/handoff`
- `POST /api/admin/tablets/rebalance`
- `POST /api/admin/backups`
- `POST /api/admin/restores`

### Missing capabilities to close

- explicit node join/register
- explicit ownership handoff
- explicit rebalance control
- explicit cluster-level health/readiness
- explicit backup/restore job surface

Acceptance:

- operator can see who is in the cluster
- operator can see who owns what
- operator can initiate a controlled drain/handoff
- operator can observe cluster health without reading raw metadata tables

## Phase 4: Build The Required Control Loops

APIs alone are not enough. Cluster state must converge automatically.

Required loops:

- node heartbeat loop
- stale-node detector
- lease reaper
- ownership reconciler
- handoff reconciler
- rebalance reconciler
- restore reconciler

Required behaviors:

- drained node stops taking new owned writes
- stale node loses ownership
- unowned tablets become eligible for reassignment
- ownership epochs are visible and monotonic
- routing snapshots converge after movement

Acceptance:

- stopping or draining one node does not require metadata surgery
- ownership converges without operator hand-editing
- clients can route around stale or draining nodes

## Phase 5: Single-Node Completeness

Single-node still needs to be excellent while cluster mode is built.

### Production-supported path

Focus on:

- `RocksDB + local filesystem`
- backup/restore
- upgrade
- troubleshooting
- Docker / Compose / Helm single-replica packaging

### Testing/evaluation path

Validate:

- `SlateDB + S3`
- cold/archive/restore behavior
- restart durability
- AWS/integration automation

Acceptance:

- single-node production path remains the easiest and best-supported mode
- single-node `SlateDB + S3` remains available for validation but clearly
  documented as non-primary

## Phase 6: Backup And Restore

Backup/restore must be first-class for both single-node and future cluster
mode.

### Single-node backup/restore

#### RocksDB + local filesystem

Support:

- stopped-node copy or filesystem snapshot
- clear restore procedure
- upgrade rollback safety

#### SlateDB + S3 test mode

Support:

- config backup
- object-store runtime and content verification
- restore validation for AWS testing

### Cluster backup/restore

Cluster backup must include:

- control-plane metadata
- runtime metadata
- object-store content:
  - cold segments
  - archive bundles
  - artifacts
- config and version markers

### Required backup modes

#### Full cluster backup

Use for disaster recovery.

Contents:

- metadata snapshot
- object-store manifest/checkpoint
- config bundle

#### Metadata-only backup

Use for:

- control-plane corruption recovery
- routing/ownership recovery

#### Object-store verification snapshot

Use for:

- proving referenced blobs exist
- checking archive/cold/artifact completeness

### Required restore modes

#### Full cluster restore

- restore metadata
- restore object references
- start clean replacement nodes
- rebuild routing snapshots
- verify ownership and fetch/replay correctness

#### Restore into a new cluster

- restore old logical data into fresh nodes
- do not depend on preserving old node-local state

#### Node replacement

- replacement node rejoins cluster
- local working set is rebuilt from shared truth
- do not require local RocksDB shard restore

### Backup/restore API shape

- `POST /api/admin/backups`
- `GET /api/admin/backups/{id}`
- `POST /api/admin/restores`
- `GET /api/admin/restores/{id}`

Acceptance:

- full backup/restore is scriptable
- object-store-backed cluster restore does not require one specific old node
- restore can be validated automatically

## Phase 7: Cluster Automation

Need full automation, not hand-executed operator notes.

### Infrastructure automation

Terraform should provision:

- S3 bucket(s)
- metadata backend
- N AgentDB nodes
- security groups/networking
- load balancer

### Bootstrap automation

One script or operator tool should:

- initialize cluster metadata
- register nodes
- seed initial tablets/placements
- wait for cluster readiness

### Cluster lifecycle automation

Need automation for:

- add node
- drain node
- replace node
- rolling restart
- rolling upgrade
- rollback
- backup
- restore

### Validation automation

Need a cluster smoke suite that checks:

- create/read session across cluster
- cross-node routing
- cold/archive/read-through
- drain one node
- ownership moves
- traffic continues
- replace node
- backup
- restore
- verify `seq` continuity and routing recovery

Acceptance:

- cluster bring-up is one command or one pipeline
- routine maintenance is scriptable
- DR drill is repeatable

## Phase 8: Dashboard And Observability

Cluster mode is not operable without visibility.

Need:

- nodes by state
- tablets by owner/state
- draining nodes
- stale nodes
- ownership churn
- rebalance progress
- restore backlog
- catch-up backlog
- hot-node pressure
- backup job status
- restore job status

Acceptance:

- operator can see cluster health without shelling into one node
- cluster maintenance progress is visible in the dashboard

## Phase 9: Upgrade And Rollback

Define and test mixed-version behavior.

Questions to answer:

- can old and new binaries coexist temporarily?
- are metadata migrations backward-compatible?
- are routing snapshots versioned?
- are backup artifacts versioned?

Required workflow:

1. drain one node
2. stop it
3. upgrade binary
4. rejoin
5. verify
6. continue to next node

Required rollback:

1. stop rollout
2. drain upgraded nodes
3. restart on prior version
4. verify ownership and routing convergence

Acceptance:

- rolling upgrades are documented and automated
- rollback is tested, not theoretical

## Suggested Milestones

### M1

- support matrix finalized in docs and config validation
- cluster health/readiness APIs
- node/tablet visibility complete

### M2

- node join/drain/maintenance APIs
- stale-node and lease reconciliation
- ownership handoff API

### M3

- rebalance control loop
- bootstrap automation
- cluster smoke test

### M4

- backup API
- metadata snapshot/restore
- object-store manifest verification

### M5

- full cluster restore
- node replacement workflow
- DR drill automation

### M6

- rolling upgrade/rollback automation
- dashboard cluster view
- production runbooks

## Non-Goals For This Plan

- clustered `RocksDB`
- operator support for local-disk-only cluster truth
- multi-region consensus or geo-federation
- full managed control plane

## Bottom Line

The cluster gap is not only a documentation problem. AgentDB still needs:

- a stable cluster operator API
- reconciliation loops that converge ownership
- first-class backup/restore semantics
- automation for bootstrap, maintenance, and DR

This plan closes those gaps while keeping the deployment story simple:

- single-node production: `RocksDB + local filesystem`
- single-node object-store mode: `SlateDB + S3` for testing/evaluation
- cluster target: `SlateDB + S3` only
