# Single-Node Upgrade

This runbook covers upgrades for the supported one-node deployment shape. Treat
Docker, Compose, and the current Helm chart as single-node restarts with
persistent storage, not rolling cluster upgrades.

## Preconditions

Before upgrading:

1. Record the current version:

   ```bash
   agentdb --version
   ```

2. Verify the node is healthy:

   ```bash
   agentdbctl status
   curl -fsS http://127.0.0.1:7878/health/ready
   ```

3. Create and validate a fresh backup:

   ```bash
   agentdbctl backup create \
     --backup-id pre-upgrade-2026-05-05 \
     --directory /var/backups/agentdb

   agentdbctl restore validate \
     /var/backups/agentdb/pre-upgrade-2026-05-05/manifest.json
   ```

4. Copy the config file:

   ```bash
   cp /etc/agentdb/agentdb.toml \
     /var/backups/agentdb/pre-upgrade-2026-05-05/
   ```

For local RocksDB or filesystem content deployments, also take a stopped-node
copy or crash-consistent volume snapshot before replacing the binary.

## Binary Upgrade

```bash
systemctl stop agentdb

AGENTDB_VERSION=vX.Y.Z \
  curl -sSL https://raw.githubusercontent.com/shenli/agentdb/main/scripts/install.sh | sh

agentdb doctor --config /etc/agentdb/agentdb.toml
systemctl start agentdb
```

Verify after startup:

```bash
agentdb --version
agentdbctl status
curl -fsS http://127.0.0.1:7878/health/ready
```

Then validate the pre-upgrade backup again so the operator log records that
the recovery artifact still exists after the upgrade:

```bash
agentdbctl restore validate \
  /var/backups/agentdb/pre-upgrade-2026-05-05/manifest.json
```

## Docker Or Compose Upgrade

Rebuild or pull the new image, then recreate the single container while keeping
the persistent volume:

```bash
docker compose down
docker compose up --build -d
```

Verify:

```bash
agentdbctl status
docker compose logs --tail=200 agentdb
```

Do not delete Docker volumes as part of an upgrade unless you are intentionally
discarding state.

## Helm Evaluation Upgrade

The Helm chart is single-replica evaluation today.

```bash
helm upgrade my-agentdb ./examples/helm/agentdb
```

Verify the one serving pod and PVC:

```bash
kubectl get pods
kubectl get pvc
agentdbctl status
```

This is not a rolling multi-node AgentDB upgrade.

## Rollback

Rollback means restoring the previous binary or container image and the storage
snapshot taken before the upgrade.

1. Stop the node.
2. Reinstall the previous binary or image.
3. Restore the pre-upgrade data snapshot and matching config if the new binary
   wrote incompatible state:

   ```bash
   agentdbctl restore apply-local \
     --manifest-path /var/backups/agentdb/pre-upgrade/manifest.json \
     --data-backup-dir /var/backups/agentdb/pre-upgrade/data \
     --target-data-dir /var/lib/agentdb \
     --config-backup /var/backups/agentdb/pre-upgrade/agentdb.toml \
     --target-config /etc/agentdb/agentdb.toml \
     --allow-overwrite
   ```

4. Run `agentdb doctor --config <path>`.
5. Start the node and verify `agentdbctl status`.

Do not run an older binary against data that has already been migrated by a
newer binary unless the release notes explicitly say it is safe.
