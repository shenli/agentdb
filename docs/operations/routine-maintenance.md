# Routine Maintenance

This checklist is the boring steady-state routine for a maintained single
AgentDB node.

## Daily

- Check readiness:

  ```bash
  curl -fsS http://127.0.0.1:7878/health/ready
  agentdbctl status
  ```

- Check disk headroom for runtime data, content, and backup volumes.
- Scan recent server logs for `storage_error`, RocksDB IO errors, background
  compaction errors, and failed backup validation.

## Weekly

- Validate the most recent backup manifest:

  ```bash
  agentdbctl restore validate /var/backups/agentdb/<backup-id>/manifest.json
  ```

- Record:
  - AgentDB version
  - config path
  - data directory size
  - backup directory size
  - free disk percentage
  - storage layout
  - any pending compaction pressure

- Run a scratch tablet-checkpoint install drill if tablet checkpoints are part
  of the deployment's recovery plan.
- Remove old backup artifacts only after confirming newer verified backups
  exist.

## Monthly Or Before Risky Changes

- Review `agentdb.toml` and compare it with the intended deployment shape.
- Confirm object-store bucket policy, lifecycle rules, and credentials still
  match the deployment.
- Run a restore drill into scratch storage.
- Review upgrade notes before replacing binaries or images.
- Confirm monitoring alerts for readiness, disk, backup validation, and storage
  errors still route to the right owner.

## Before Planned Maintenance

1. Create and validate a fresh backup.
2. Copy the config file into the backup directory.
3. Confirm free disk is sufficient for the maintenance action.
4. Stop the node if the action touches local runtime data.
5. Run `agentdb doctor --config <path>` before restart.
6. Verify `agentdbctl status` and `/health/ready` after restart.

## After Incidents

Record:

- exact symptom and first failing command
- server version and config path
- relevant log lines
- disk free before and after remediation
- backup ID used for validation or recovery
- whether the incident changed retention, backup, or capacity policy

Then update the local operator notes if the recovery required a step not
covered by the runbooks.
