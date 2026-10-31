# Encryption At Rest And Tenant-Managed Keys

Status: opt-in implementation with remaining deployment qualification; cluster use is experimental.

This document describes the encryption implementation boundary, threat model,
key lifecycle, and design requirements for shared RocksDB storage. Operational
configuration belongs in the operations guide; historical rollout notes are
archived separately.

## Decision Summary

AgentDB should use two complementary layers:

1. Infrastructure encryption for every deployment: encrypted disks, object
   storage, backups, and, where available, RocksDB or filesystem encryption.
2. Authenticated envelope encryption for tenant payloads that require
   tenant-specific keys or bring-your-own-key (BYOK) control.

Application or key-value-layer encryption is an established design when a
shared physical store must contain data protected by different tenant keys. It
is not the cheapest baseline for all data. The infrastructure layer protects
lost media with low application complexity; the tenant envelope layer adds
cryptographic tenant separation, key revocation, and BYOK semantics.

The hot path must not call a remote KMS for each key-value operation. A KMS key
encryption key (KEK) wraps versioned data encryption keys (DEKs). AgentDB
unwraps a DEK infrequently, keeps it in a bounded local cache, and performs
authenticated encryption locally:

```text
Tenant KEK in KMS or external key manager
                |
                | wrap / unwrap on cache miss or key operation
                v
Wrapped, versioned tenant DEKs in the AgentDB key catalog
                |
                | bounded plaintext-key lease in a serving process
                v
Local authenticated encryption and decryption
                |
                v
RocksDB, Raft state, snapshots, and object storage hold ciphertext
```

This keeps steady-state KMS traffic near zero calls per row. KMS calls occur on
cache misses, key creation, rewrap, rotation, or explicit policy checks rather
than on every read and write.

## Current Implementation Status

Envelope encryption is opt-in (`AGENTDB_ENCRYPTION_MODE=envelope`); the default
is disabled. The implemented surface includes:

| Area | Implementation |
|---|---|
| Cryptography | Authenticated v2 envelopes using AES-256-GCM or XChaCha20-Poly1305, with tenant/object/purpose/key-version AAD and bounded DEK caching. Individual event AAD includes the sequence number. |
| Key sources | Wrapped-DEK catalogs, a local KEK provider, and an external-command KEK provider for KMS/HSM integration. |
| Payload paths | Event and primary tenant/agent/session/record/artifact values, export-ledger values, and content blobs; payload sealing precedes replicated log writes. |
| Runtime layouts | Persistent single-node and both experimental replicated tablet layouts. Cluster key state uses shared control metadata and periodic refresh (`AGENTDB_ENCRYPTION_REFRESH_MS`, default 30 seconds). |
| Key lifecycle | KEK rewrap, DEK rotation, revocation, metadata/content re-encryption, and guarded version retirement, with admin API and CLI workflows. |

See the [operations guide](../operations/operations.md) for configuration and
[agentdbctl](../operations/agentdbctl.md) for key-management commands.
Implementation evidence includes the
[coverage tests](../../crates/agentdb-core/tests/encryption_coverage.rs),
[Raft ordering test](../../crates/agentdb-core/tests/encryption_raft_ordering.rs),
and [re-encryption tests](../../crates/agentdb-core/tests/reencryption_walk.rs).

Current limits and qualification requirements:

- Enabling encryption does not migrate existing plaintext rows. Legacy v1
  envelopes may contain plaintext; an envelope header alone is not proof of
  encryption. The re-encryption walk updates older encrypted key versions.
- RocksDB keys, identifiers, and some operational metadata remain plaintext.
  Logs, diagnostics, exports, backups, and derived data require their own
  coverage review and infrastructure protection.
- Key refresh and shared metadata remain cluster dependencies. Encryption does
  not establish independent Cell operation or instantaneous cross-node revocation.
- Content-key retirement requires a backend whose objects can be enumerated
  and verified; otherwise old versions remain retained.
- Dedicated-domain placement enforcement and graceful hydration of undecryptable
  tenant rows remain gaps. A decrypt failure during startup scans can propagate.
- Production BYOK qualification still requires the failure, backup/restore,
  retention, and performance checks below. The remaining design sections state
  requirements and targets, not a claim that every acceptance gate has passed.

The [implementation history](../archive/encryption-implementation-history.md)
preserves the 24 milestones and original delivery plan.

## Threat Model

The design should protect tenant payloads from:

- copied or lost disks;
- direct inspection of SST, WAL, checkpoint, snapshot, backup, or object-store
  files;
- accidental cross-tenant ciphertext disclosure from shared physical storage;
- continued decryption after a tenant key has been revoked, subject to the
  documented key-cache lease bound.

The design does not protect against:

- a compromised live AgentDB process that is authorized to obtain the tenant's
  active DEK;
- an application or operator authorized to read plaintext through AgentDB;
- leakage through logs, crash dumps, debug endpoints, or plaintext exports;
- plaintext key names, tenant/session identifiers, object sizes, timing, or
  access patterns unless those fields are separately protected.

RocksDB keys should initially remain plaintext because AgentDB depends on
prefix and range scans. Hashing or encrypting keys would change routing and scan
semantics and is outside this proposal. Values containing tenant payloads are
the initial encryption boundary.

## Storage And Isolation Model

One grouped RocksDB domain may hold multiple tablets, and multiple tenants may
therefore share one LSM tree. That is compatible with per-tenant envelope
encryption: rows from different tenants can be ciphertext under different DEKs
in the same SST.

The isolation claim must distinguish a tablet from a physical RocksDB domain.
A tenant with an exclusive tablet is logically isolated, but is not necessarily
in its own LSM when `tablets_per_domain` is greater than one. Physical
co-residency requirements need one of:

- `tablets_per_domain = 1` for the relevant placement class; or
- an allocator invariant that assigns the tenant's tablets to a dedicated
  RocksDB domain or cell.

Recommended service classes are:

| Class | Physical storage | Keying | Intended guarantee |
|---|---|---|---|
| Shared | Shared RocksDB domain permitted | Platform storage encryption; tenant envelope when requested | Cryptographic tenant separation, not physical separation |
| Dedicated | Dedicated RocksDB domain | Tenant or platform domain key; tenant envelope optional | No tenant co-residency in the LSM |
| Regulated | Dedicated domain or cell | BYOK domain protection plus tenant envelopes | Physical and cryptographic isolation with an explicit key lifecycle |

A shared SST cannot be handed to one tenant as “their SST.” Per-tenant export
must be a logical, envelope-aware operation. Crypto-shredded ciphertext may
remain in shared SSTs until compaction; shredding means it is no longer
decryptable, not that every physical byte disappeared immediately.

## Key Hierarchy

The recommended hierarchy is:

- A tenant KEK is held by a supported KMS or external key manager. For BYOK,
  the customer controls this key or its external backing material.
- AgentDB generates one or more random DEKs per tenant and purpose. It stores
  only wrapped DEKs durably.
- An envelope records enough non-secret metadata to select the correct DEK
  version and algorithm.
- The serving process obtains a bounded lease on plaintext DEK material and
  performs authenticated encryption locally.

Initial purposes should correspond to actual lifecycle and access differences,
not every Rust type. A practical starting set is `metadata`, `content`,
`index`, `export`, and `snapshot`. Fewer purposes reduce operational overhead;
new purposes should require a concrete isolation or rotation need.

The durable key catalog should contain, at minimum:

```text
tenant_id
purpose
dek_version
wrapped_dek
wrapping_key_ref
wrapping_key_version, when the provider exposes one
status: active | read_only | retired | revoked
created_at, activated_at, retired_at
policy_epoch
rotation progress or watermark references
```

It must never contain a plaintext DEK.

## Envelope Format And Cryptographic Context

Add a versioned `AGDB-ENVELOPE-v2` format. It should include:

- format version and algorithm suite;
- DEK identifier and version;
- nonce;
- ciphertext and authentication tag;
- bounded lengths for every variable field.

Use an audited authenticated-encryption implementation rather than custom
cryptography. AES-256-GCM is a reasonable default when nonce uniqueness can be
guaranteed and accelerated implementations are available. XChaCha20-Poly1305
is a reasonable alternative when a larger nonce space simplifies safe nonce
generation. The implementation decision must specify nonce generation,
per-key usage limits, and provider/FIPS constraints before release.

Associated authenticated data (AAD) should bind ciphertext to its location and
meaning. Derive it canonically from at least:

- tenant ID;
- logical storage key or stable object ID;
- object kind and schema/envelope version;
- DEK version;
- tablet or placement epoch where moving the ciphertext should invalidate it,
  otherwise omit that field deliberately to preserve movement semantics.

Changing the tenant, object ID, or storage key must make decryption fail. AAD
does not need to be stored in the envelope when it can be reproduced exactly.

The reader should support v1 during a migration window, but v1 must be treated
as legacy framed data rather than proof of encryption. Unknown versions,
algorithms, impossible lengths, bad tags, and missing historical keys must fail
closed with an actionable error.

## Rotation Semantics

“Rotate a key” describes several different operations. They need separate APIs,
states, and completion criteria.

### KMS material rotation under one logical KEK

Some KMS providers retain old material and select it transparently when
decrypting. That operation changes the provider's current KEK material without
rewriting AgentDB data. It does not rotate AgentDB DEKs or re-encrypt tenant
payloads.

### Replacing a tenant KEK

Create or select the new KEK, unwrap each tenant DEK with the old KEK, and wrap
it with the new KEK. The cost is proportional to the number of retained DEK
versions, not the number of rows. Verify every wrapped DEK under the new KEK
before disabling the old KEK.

### Rotating a tenant DEK

DEK rotation is an online data migration:

1. Create DEK v2 and make it active for new writes.
2. Keep v1 read-only so readers accept v1 and v2.
3. Rewrite old values in bounded background batches, or rewrite lazily on read
   plus a background completion sweep.
4. Track remaining objects and bytes across RocksDB, segments, archives,
   snapshots, backups, and exports.
5. Retire v1 only after all required copies have been rewritten or have expired
   under a documented retention policy.

The worker must be idempotent, restart-safe, rate-limited, observable, and
pausable. It should checkpoint a cursor or watermark and expose a dry-run
inventory before starting. RocksDB compaction alone is not a rotation protocol.

### Revocation And Crypto-Shredding

Plaintext DEK caching means revocation cannot honestly be instantaneous. Define
a configurable maximum lease, bump a tenant policy epoch, stop new operations,
and invalidate matching cache entries on all serving nodes. A strict
revocation acknowledgement can be emitted only after all relevant nodes have
acknowledged eviction or their leases have expired.

Use a two-phase state such as `revoking -> revoked`, retain a non-sensitive
audit tombstone, and document the revocation SLO. Deleting or disabling the KEK
makes retained ciphertext unrecoverable only after no authorized cache still
holds usable key material.

## Key Cache Requirements

A production cache should provide:

- maximum entries and maximum bytes;
- time, use-count, or byte-count limits on each lease;
- single-flight unwraps to avoid a KMS stampede;
- a short negative cache for unavailable or denied keys;
- explicit invalidation by tenant and policy epoch;
- zeroization on eviction and locked memory where the platform supports it;
- cache hit, miss, eviction, age, and KMS latency/error metrics.

Steady-state reads and writes should perform local AEAD operations. Deployment
qualification should measure and report `KMS calls / KV operation`; after
warmup the target is zero except for bounded cache churn and key-management
events.

## Durable Plaintext Coverage Inventory

Before AgentDB advertises tenant BYOK, every durable byte path should be listed
and classified as tenant payload, derived tenant data, operational metadata, or
intentionally plaintext. The inventory must include:

- tenants, sessions, events, records, policies, and
  tenant metadata;
- artifact metadata and artifact content;
- sealed event segments, archives, and replay-prefix materializations;
- idempotency rows, local/global record indexes, outbox entries, and export
  ledger/changefeed entries;
- Raft commands and logs, tablet snapshots, RocksDB checkpoints, and recovery
  manifests;
- backups, local temporary files, object-store staging objects, and restored
  copies;
- persisted text/vector search indexes and other derivatives;
- logs, traces, crash dumps, diagnostics, and authorized external exports.

For each item, record its location, owner, encryption key/purpose, AAD, rotation
behavior, revocation behavior, and retention period. A feature is not covered
merely because its source event was encrypted.

Derived search data needs special attention. A persisted text or vector index
can reveal tenant content even when base rows are encrypted. Either rebuild it
in memory from encrypted base data, encrypt tenant-specific persisted index
segments, or place it in a dedicated encrypted domain. Full BYOK must not be
claimed while a plaintext derived index remains durable.

## Backup, Restore, And Data Movement

Backup, restore, and tablet movement are part of the encryption boundary. They
must preserve encryption and key-lifecycle guarantees rather than becoming
side paths that expose plaintext or strand ciphertext under unavailable keys.

The word “snapshot” covers several different artifacts and each needs an
explicit contract:

| Artifact | Required behavior |
|---|---|
| RocksDB checkpoint or filesystem backup | Preserve ciphertext and wrapped key metadata without exporting plaintext DEKs |
| Raft/tablet catch-up snapshot | Transfer ciphertext and prove the target can read every referenced key version before ownership changes |
| Whole-tablet SST movement | Copy ciphertext opaquely when the target storage layer can ingest the source SST safely |
| Logical split/merge snapshot | Preserve envelopes only when their AAD remains valid; otherwise decrypt and reseal under the destination context |
| Sandbox filesystem snapshot | Remains owned by the sandbox service; its encryption and BYOK contract must be explicit rather than inferred from AgentDB refs |

### Backup Contract

A backup should copy encrypted RocksDB values, content objects, segments, and
archives without decrypting them. It should include:

- the wrapped-DEK catalog and every referenced DEK version;
- KEK references and provider/region metadata needed to locate them;
- envelope and encryption-policy versions;
- a key-usage inventory identifying which backup component requires which DEK;
- authenticated backup-manifest metadata and checksums for every component.

It must not contain plaintext DEKs, provider credentials, or decrypted tenant
payloads. Operational configuration containing credentials remains a separate
secret-management concern.

Current AgentDB backup validation proves referenced file presence, size, and
checksum consistency. It does not yet prove complete tenant-payload coverage or
that all required historical keys can be unwrapped. Runtime summary data is not
a full runtime backup, and filesystem/object-store dependencies still require
an explicit capture procedure.

A physical checkpoint of a shared tablet or domain can contain multiple
tenants. It is a tablet/domain recovery artifact, not a tenant-owned backup.
Per-tenant backup, export, and restore require a logical envelope-aware path.

### Restore Contract

Restore must run a cryptographic preflight before the restored node becomes
ready:

1. Read and validate the backup manifest and key-usage inventory.
2. Verify access to every required KEK and historical DEK version.
3. Unwrap each required DEK once and authenticate representative metadata and
   content envelopes.
4. Restore into an isolated, unready location.
5. Run structural validation plus full or sampled cryptographic validation
   according to an explicit confidence policy.
6. Make the restored deployment ready only after all required dependencies are
   usable.

The inventory check, rather than a sample decrypt alone, proves that no known
historical key version is missing. Decryption work should use cached DEKs and
must not make one KMS request per restored row.

When restoring under a replacement KEK, AgentDB should rewrap the retained DEKs
instead of rewriting every payload. If a required KEK or DEK was deliberately
destroyed for crypto-shredding, restore must fail permanently with an explicit
`key_revoked` or `historical_key_unavailable` error. That failure is the
intended security result and must not be bypassed through an older backup.

### Whole-Tablet SST Movement

Per-value envelopes allow the current SST movement path to transfer and ingest
ciphertext without decrypting or re-encrypting every row. The movement protocol
should be:

```text
prepare target
  -> validate encryption policy and all referenced DEK versions
  -> transfer ciphertext SST and authenticated manifest
  -> ingest into an unowned target tablet
  -> verify structural integrity and decryptability
  -> acknowledge readable
  -> transfer ownership
  -> evict source key leases after handoff
```

The target must not become the serving owner merely because SST ingest
succeeded. It must first demonstrate access to every required tenant key and
encryption-policy version. Rotation state and rewrite watermarks move with the
tablet so the source does not retire an old DEK while the destination still
needs it.

Current `ADSS` framing carries the tablet ID, applied index, length, and raw SST
bytes. The encrypted design should add an authenticated movement manifest with
at least the source and target tablet identity, snapshot/applied index, byte
checksum, envelope versions, encryption-policy versions, referenced DEK
versions, and rotation watermarks. RocksDB checksums detect storage corruption;
they are not a substitute for authenticating the movement authorization and
manifest.

Movement scratch files must remain ciphertext-only, use restrictive
permissions, live on encrypted infrastructure storage, and be reconciled after
a crash. Metrics should expose orphaned scratch bytes and cleanup failures.

### Logical Split And Merge

AAD determines whether ciphertext is portable. Initially bind envelopes to the
tenant, logical KV key or object ID, object kind, envelope/schema version, and
DEK version. Do not bind them to a node, filesystem path, RocksDB domain, or
replica.

Whole-tablet movement currently preserves the tablet ID, but split and merge
can place values into new tablets. Binding AAD to tablet ID or tablet epoch
would force those operations to decrypt and reseal every value. AgentDB should
therefore omit tablet identity from initial AAD unless preventing cross-tablet
movement is an explicit security requirement worth that rewrite cost.

### Storage-Layer Encryption Compatibility

Application-layer ciphertext is portable across domains. A source SST encrypted
by a RocksDB filesystem or file-level key may not be ingestible in a destination
domain. Before combining file-level encryption with SST movement, choose and
test one supported contract:

- source and destination share the relevant domain key;
- the SST carries a separately wrapped portable file key;
- movement decrypts and re-encrypts the file through a trusted streaming path;
- the destination reconstructs a logical snapshot instead of ingesting the SST.

Disk encryption is normally transparent to an SST copied through the
application, while provider-managed volume snapshots require the destination
account and region to have the appropriate key grants. These are separate
deployment contracts and should not be conflated with tenant envelope keys.

### Historical-Key Retention

Completing a live-row rewrite is insufficient to retire a DEK. Rotation status
must account for every retained copy in Raft logs, tablet snapshots, SST
movement files, checkpoints, backups, sealed segments, archives, exports, and
temporary files. A DEK can be destroyed only when each required copy has been
rewritten or has expired under retention policy.

## Performance And Capacity Effects

Authenticated envelopes add a nonce, tag, version fields, and key metadata. A
reasonable planning estimate is 50–100 bytes per individually encrypted value,
depending on identifier lengths and encoding. For the existing 256-byte event
benchmark that is roughly 20–40% raw expansion before allocator, block, and
compression effects. This estimate must be replaced with measurements from the
chosen format.

Encrypting each value before RocksDB sees it also removes cross-value and block
compression opportunities. That storage and write-amplification effect may be
larger than AEAD CPU cost. Recommended granularity is therefore lifecycle
aware:

- hot events and mutable metadata: encrypt per durable value;
- sealed segments and archives: compress a bounded group, then encrypt it once;
- artifacts: encrypt per content object or bounded chunk;
- backups and snapshots: preserve already encrypted values and separately
  protect manifests and any uncovered operational files.

Always compress before encryption. Decryption adds CPU, allocations, and
possibly copies on reads; implementations should support buffer reuse and
measure allocation rate rather than assuming cryptography is the only cost.

The evaluation matrix should cover 256-byte append-and-fetch operations at
concurrency 1, 4, 16, and 64 and at least one larger artifact/segment workload.
Report:

- throughput and p50/p90/p99 latency;
- CPU time and allocations per operation;
- WAL, SST, checkpoint, and backup byte growth;
- compression ratio and compaction bytes;
- KMS calls, cache hit rate, and KMS latency;
- rotation throughput and impact on foreground p99 latency.

Before production, adopt explicit regression budgets. An initial evaluation
gate can require no more than 10% throughput or p99 regression on the hot-event
benchmark after cache warmup, but that number is a proposed product target, not
a measured current result.

## Validation And Operations

Required functional and failure tests include:

- wrong tenant, object, purpose, or storage-key AAD fails authentication;
- a one-bit mutation in header, nonce, ciphertext, or tag fails closed;
- nonce uniqueness and per-DEK usage limits are enforced;
- old and new DEK versions remain readable during rotation;
- writes fail predictably when the active key is unavailable;
- bounded cached reads follow the documented outage policy;
- cache invalidation and policy-epoch changes propagate across nodes;
- a crash and restart at every rotation checkpoint is idempotent;
- moving ciphertext to another tenant or object fails;
- restore with a missing or disabled historical key remains unready;
- a restored backup cannot bypass a key revocation or crypto-shred tombstone;
- whole-tablet movement validates all referenced DEK versions before ownership
  transfer and preserves rotation watermarks;
- split/merge either preserves valid AAD or explicitly reseals values;
- movement scratch files contain no plaintext and crash leftovers are detected;
- file-level encrypted SST movement fails closed or uses its documented rekey
  path when source and destination domain keys differ;
- identical plaintext for two tenants yields independent ciphertext;
- v1 plaintext envelopes cannot be mistaken for v2 authenticated ciphertext.

Expose at least these metric families, using the repository's normal naming
and label-cardinality conventions:

```text
agentdb_crypto_encrypt_seconds
agentdb_crypto_decrypt_seconds
agentdb_crypto_failures_total{reason}
agentdb_crypto_key_cache_hits_total
agentdb_crypto_key_cache_misses_total
agentdb_crypto_key_cache_entries
agentdb_crypto_key_cache_bytes
agentdb_crypto_kms_requests_total{operation,result}
agentdb_crypto_kms_request_seconds{operation}
agentdb_crypto_rotation_objects_remaining{tenant,purpose}
agentdb_crypto_rotation_bytes_remaining{tenant,purpose}
agentdb_crypto_revocation_age_seconds{tenant}
```

Tenant labels may be too high-cardinality for some metric backends. If so,
retain per-tenant detail in admin status and logs while aggregating metrics by
provider, purpose, and result.

Operator runbooks should cover KMS throttling/unavailability, missing historical
keys, corrupted envelopes, cache pressure, stalled rotation, partial
revocation, backup restore, and rollback from a mixed-version deployment.

## Open Decisions

- What additional platform validation or FIPS qualification is required for the
  implemented AEAD suites?
- What revocation SLO determines the maximum plaintext-DEK cache lease?
- Is a DEK per tenant/purpose sufficient, or do large artifacts/segments need
  per-object DEKs wrapped by a tenant KEK?
- Which operational metadata is intentionally plaintext, and how is that
  disclosed to customers?
- Must ciphertext remain movable between tablets/domains, which determines
  whether placement epoch belongs in AAD?
- What backup-retention rule allows a retired DEK to be destroyed?
- Which service class first offers BYOK, and does it require a dedicated domain?

## References

- [Google Cloud KMS: Envelope encryption](https://docs.cloud.google.com/kms/docs/envelope-encryption)
- [AWS Encryption SDK: Data key caching](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/data-key-caching.html)
- [AWS KMS: Rotate keys](https://docs.aws.amazon.com/kms/latest/developerguide/rotate-keys.html)
