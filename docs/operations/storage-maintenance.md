# Storage Maintenance

This runbook covers single-node storage inspection and compaction. It focuses
on the maintained one-node deployment shape and uses `agentdbctl` by default.

## Inspect Storage State

Start with:

```bash
agentdbctl status
agentdbctl -o json status
```

When the server exposes tablet-store details, status includes:

- tablet-store layout
- root path
- tablets per domain
- max open domains
- open domain count
- mutable memtable budget
- pending compaction bytes for open domains
- background compaction or flush errors
- retained tablet-log compaction counters

For local disk pressure:

```bash
df -h /var/lib/agentdb
du -sh /var/lib/agentdb
```

For object-store-backed deployments, inspect bucket/prefix size and lifecycle
policy with the provider-native tooling.

## Manual Tablet-Store Compaction

Use manual compaction after heavy churn, restore drills, or when a specific
local tablet domain shows sustained compaction pressure:

```bash
agentdbctl tablet-store compact --tablet-id 7 --dry-run
agentdbctl tablet-store compact --tablet-id 7
```

Run the command against the node that owns the local tablet-store domain.
Compaction is best-effort; backends without a native compaction hook may reject
or no-op the request.

After compaction, check:

```bash
agentdbctl status
df -h /var/lib/agentdb
```

Compaction may temporarily need extra disk while rewriting files. Do not start
manual compaction when the disk is already critically full.

## Retained Tablet-Log Compaction

Retained tablet-log cleanup is opt-in:

```bash
export AGENTDB_TABLET_LOG_COMPACTION_MIN_DEBT_OPS=10000
```

This compacts local tablet logs when retained committed log entries reach the
threshold. It is separate from LSM/RocksDB compaction. `agentdbctl status`
exposes local compaction success and failure counters so operators can
distinguish an idle worker from a failing one.

## Sizing Checks

For grouped RocksDB tablet domains, the rough mutable memtable cap is:

```text
max_open_domains * domain_write_buffer_size_mib * domain_max_write_buffers
```

Keep that budget inside the instance memory envelope before raising
`max_open_domains` or lowering `tablets_per_domain`.

Use `tablets_per_domain = 1` only when you need physical tablet checkpoints or
high-isolation restore tests. It can increase file descriptor and memory
pressure because each tablet may reserve its own domain resources.

## Routine Storage Maintenance

Weekly:

- run `agentdbctl status`
- record disk free percentage and data directory size
- inspect pending compaction bytes and background errors
- validate the most recent backup manifest
- remove only backup artifacts that have a newer verified replacement

Before large imports or benchmark runs:

- confirm free disk is comfortably above the expected working set
- avoid running manual compaction and large write tests at the same time
- keep backup output on a different volume when possible

After restore drills or heavy churn:

- inspect compaction pressure
- run targeted manual compaction only if the backend supports it
- verify backup manifests again after cleanup
