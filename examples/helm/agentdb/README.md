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
kubectl port-forward svc/my-agentdb-agentdb 7878:7878
curl http://localhost:7878/health/ready
open http://localhost:7878/dashboard
```

## What the chart ships

- **StatefulSet** with one replica (intentional — see
  above), persistent `/data` volume.
- **Service** exposing REST (7878) and the dashboard.
- **Readiness** hits `/health/ready`; **liveness** hits
  `/health/live`. Both already carry structured error
  bodies with doc URLs.

## What it does NOT ship

- Ingress (operator's call — plenty of flavors).
- Certificate management; terminate TLS at ingress or a service mesh.
- Scheduled backups; snapshot the PVC with your cluster storage tooling.

## Values

See `values.yaml` — every knob is commented. The two most
common changes:

- `image.tag` — pin to a released version.
- `persistence.size` — bump above 10Gi for any real
  workload.

## Hardening checklist (before running in prod)

- [ ] Ingress + TLS via cert-manager.
- [ ] PVC snapshot/backup policy configured.
- [ ] Resource limits tuned to the workload.
- [ ] NetworkPolicy restricting egress + inter-pod traffic.
