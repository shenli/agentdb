# AgentDB Helm chart (evaluation)

Minimal single-node AgentDB deployment. Intended for
evaluation on a local Kind / k3d cluster; **not
production-hardened**. Multi-node clustering isn't modeled
here (needs the shared meta store + lease reconciliation
that the one-replica single-statefulset shape sidesteps).

## Quick start

```bash
export AGENTDB_API_AUTH_TOKEN='replace-with-at-least-16-characters'
kubectl create secret generic agentdb-api-auth \
  --from-literal=api-token="$AGENTDB_API_AUTH_TOKEN"

helm install my-agentdb ./examples/helm/agentdb \
  --set auth.existingSecret=agentdb-api-auth

# Port-forward to check
kubectl port-forward svc/my-agentdb-agentdb 7878:7878
curl http://localhost:7878/health/ready
open http://localhost:7878/dashboard
```

The token must contain at least 16 visible ASCII characters with no whitespace.

Health probes are intentionally unauthenticated. Enter the bearer token into
the dashboard when using API views; the page keeps it in memory only. For CLI
operations, set `AGENTDB_TOKEN="$AGENTDB_API_AUTH_TOKEN"`.

## What the chart ships

- **StatefulSet** with one replica (intentional — see
  above), persistent `/data` volume.
- **ConfigMap** with `/etc/agentdb/agentdb.toml`.
- **Service** exposing REST (7878) and the dashboard.
- **Existing Secret reference** for the required installation-wide API bearer
  token; the chart does not render plaintext credentials into a ConfigMap.
- **Readiness** hits `/health/ready`; **liveness** hits
  `/health/live`.

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
- `auth.existingSecret` — required Secret name; `auth.tokenKey` defaults to
  `api-token`, whose value must contain at least 16 visible ASCII characters
  with no whitespace.
- `tabletStore.*` — grouped RocksDB tablet-store sizing. The mutable memtable
  budget is approximately
  `maxOpenDomains * domainWriteBufferSizeMiB * domainMaxWriteBuffers`.

## Hardening checklist (before running in prod)

- [ ] Ingress + TLS via cert-manager.
- [ ] API bearer Secret is generated randomly, rotated, and not committed.
- [ ] PVC snapshot/backup policy configured.
- [ ] Resource limits tuned to the workload.
- [ ] NetworkPolicy restricting egress + inter-pod traffic.
