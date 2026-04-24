# AgentDB Helm chart (evaluation)

Minimal single-node AgentDB deployment. Intended for
evaluation on a local Kind / k3d cluster; **not
production-hardened**. Multi-node clustering isn't modeled
here (needs the shared meta store + lease reconciliation
that the one-replica single-statefulset shape sidesteps).

## Quick start

```
helm install my-agentdb ./examples/helm/agentdb

# Port-forward to check
kubectl port-forward svc/my-agentdb-agentdb 7878:7878 7879:7879
curl http://localhost:7878/health/ready
open http://localhost:7878/dashboard
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
```

## What the chart ships

- **StatefulSet** with one replica (intentional — see
  above), persistent `/data` volume.
- **Service** exposing REST (7878) and Mongo wire (7879).
- **Readiness** hits `/health/ready`; **liveness** hits
  `/health/live`. Both already carry structured error
  bodies with doc URLs.
- Admin bearer-token auth off by default; flip
  `admin.requireAuth: true` to turn it on, then mint a
  bootstrap token via the HTTP API (first
  `POST /api/admin/tokens` is permitted without auth when
  no active tokens exist).

## What it does NOT ship

- Ingress (operator's call — plenty of flavors).
- Certificate management (bring your own cert-manager
  Certificate and point `tls.secretName` at it).
- External object-store credentials (provision a Secret,
  reference via `secretEnv`).
- Scheduled-backup PVC wiring (the chart writes backups
  to an `emptyDir` when enabled; operators should swap to
  a real PVC or CSI mount for durability).

## Values

See `values.yaml` — every knob is commented. The two most
common changes:

- `image.tag` — pin to a released version.
- `persistence.size` — bump above 10Gi for any real
  workload.

## Hardening checklist (before running in prod)

- [ ] Ingress + TLS via cert-manager.
- [ ] Admin bearer-token auth enabled.
- [ ] External object store configured (S3 / GCS / MinIO).
- [ ] Persistent backup target (PVC or object-store CSI).
- [ ] Resource limits tuned to the workload.
- [ ] NetworkPolicy restricting egress + inter-pod traffic.
