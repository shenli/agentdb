# Encryption At Rest And Tenant-Managed Keys

Status: proposed design; cryptographic implementation is not complete.

This document defines how AgentDB should add encryption at rest without giving
up shared RocksDB domains, predictable performance, or manageable key rotation.
It also records the current implementation boundary and proposes an incremental
code-change plan.

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

**Milestone 1 (landed): the crypto core exists but is not yet wired to any
durable path.** `agentdb-storage/src/crypto/` provides the cohesive keyring the
"Proposed Code Changes" §1–3 call for:

- `EnvelopeCrypto` (seal/open) over per-tenant, per-purpose DEKs, with a
  production `AeadTenantKeyring` (AES-256-GCM and XChaCha20-Poly1305) and an
  explicit `DisabledEnvelopeCrypto` that fails closed.
- The self-describing `AGDB-ENVELOPE-v2` frame (bounded parser, golden vector,
  fuzz test) — distinct from v1, which stays legacy framed plaintext.
- Canonical AAD binding tenant, object kind, object id, purpose, key id, key
  version, and suite, so ciphertext cannot move between tenants/objects/purposes
  or be downgraded.
- A `KeyProvider` seam with a `LocalKeyProvider` (HKDF-SHA256, dev/single-key
  source; the KMS-backed wrapped-DEK catalog is a later milestone) and a bounded
  plaintext-DEK cache.
- Explicit `CryptoConfig` (`disabled | envelope`, suite, cache bounds).

**Milestone 2 (landed): durable content is wired through the keyring.** The
runtime's `seal_content`/`open_content` helpers seal every durable content
object — artifacts, session segments, session archives, and replay prefixes —
as an `AGDB-ENVELOPE-v2` frame before it reaches the content backend when the
keyring is enabled, and fall back to the legacy v1/identity path when it is
disabled (the default). Reads detect the frame version, so a disabled→enabled
rollout (and the reverse) stays readable — a mixed v1/v2/plaintext reader. A
raw-storage marker test proves plaintext is absent from stored content, and
identical content for two tenants yields independent ciphertext.
`Runtime::with_kv_content_and_keyring` injects a keyring; the default stays
disabled, so existing deployments are unchanged until they opt in.

**Not yet done:** the *event and metadata* hot paths are still plaintext (only
content-backend objects are wired); records, skills, indexes, idempotency,
export, Raft, snapshots, checkpoints, and backups are not covered; there is no
KMS provider or wrapped-DEK catalog, rotation worker, or revocation; and the
server does not yet build a keyring from configuration. The remaining stages add
these, plus the benchmark matrix.

The pre-existing seams remain as the integration points those stages build on:

- `agentdb-storage/src/kms.rs` resolves a tenant key identifier and availability
  state. It does not return or manage cryptographic key material.
- `agentdb-storage/src/content_crypto.rs` / `metadata_crypto.rs` define the
  content and metadata adapters; the default adapters copy plaintext.
- `agentdb-core/src/persist.rs` invokes the metadata adapter at selected
  persistence boundaries; `runtime.rs` wires the allow-all resolver and identity
  adapters in default constructors.

The `AGDB-ENVELOPE-v1` frame records a key identifier and optional version
around adapter output; with the identity adapters the framed payload is still
plaintext and must not be described or detected as encrypted merely because it
has an envelope header.

Production encryption therefore remains incomplete until the durable paths are
wired to the keyring, a KMS-backed provider and key catalog exist, every durable
tenant-payload path is inventoried and covered, and rotation, revocation,
backup, restore, and observability contracts are implemented.

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

- tenants, sessions, events, records, record edges, skills, policies, and
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

## Proposed Code Changes

### 1. Make the crypto dependency cohesive

The current `KmsKeyResolver`, `ContentCryptoAdapter`, and
`MetadataCryptoAdapter` can be configured inconsistently. Introduce a cohesive
`TenantKeyring` or `EnvelopeCrypto` abstraction, while retaining the existing
traits as internal migration layers if useful:

```rust
trait TenantKeyring {
    fn seal(&self, context: &CryptoContext<'_>, plaintext: &[u8])
        -> Result<Vec<u8>, CryptoError>;
    fn open(&self, context: &CryptoContext<'_>, envelope: &[u8])
        -> Result<Vec<u8>, CryptoError>;
    fn active_key(&self, tenant_id: &str, purpose: KeyPurpose)
        -> Result<KeyMetadata, CryptoError>;
    fn invalidate(&self, tenant_id: &str, policy_epoch: u64);
}
```

Keep raw DEK bytes inside the implementation or an opaque AEAD lease; do not
return them through general runtime APIs. Extend key metadata with purpose, DEK
version, and policy epoch so cache entries and ciphertext selection cannot be
confused across policies.

Suggested ownership: add `tenant_keyring.rs` under `agentdb-storage` or a small
dedicated crypto crate if provider dependencies would otherwise burden the
storage crate.

### 2. Make insecure mode explicit

Update runtime/server construction so production encryption configuration
cannot silently select identity adapters. Identity behavior should require an
explicit `disabled` or `development` mode. If a tenant policy requires envelope
encryption and no real provider is configured, startup or tenant activation
must fail closed.

Suggested configuration shape:

```toml
[encryption]
mode = "disabled" # disabled | envelope
provider = "aws_kms"
key_cache_ttl_ms = 300000
key_cache_max_entries = 1000
key_cache_max_bytes = 8388608
```

The exact defaults require a workload and revocation-SLO decision. Configuration
validation should explain which provider credential, key reference, or tenant
policy is missing.

### 3. Add envelope v2 and production AEAD

Evolve `content_crypto.rs` into the shared bounded parser/encoder for v2, or add
an `envelope.rs` module used by metadata and content. Fuzz the parser and keep
golden vectors. Add a production implementation based on an audited Rust crypto
library or provider SDK, with no custom cipher construction.

### 4. Move from selected types to complete persistence coverage

`MetadataObjectKind` currently covers only artifacts and events. Either extend
it to every durable tenant-payload type or put encryption below typed
serialization at a tenant-aware persistence boundary. The latter reduces the
chance that a new object type bypasses encryption, but it must distinguish
tenant payloads from routing/control records and preserve prefix scans.

Add an invariant test that writes a unique secret marker through every durable
path and confirms it is absent from raw RocksDB values, WAL/Raft files,
checkpoints, snapshots, backups, temporary objects, and persisted derivatives.
Because some formats may transform plaintext without encrypting it, the test is
a regression detector rather than the only security proof.

### 5. Verify Raft and snapshot ordering

Confirm encryption occurs before a tenant payload enters any durable Raft log
or tablet snapshot. Add tests that inspect the real persisted log/snapshot
representation; do not infer coverage only from the `KvStore` boundary. Restore
must fail closed with an actionable missing-key error before cutover.

### 6. Add key catalog, rotation worker, and admin status

Store wrapped DEK records and lifecycle state in control metadata. Add a
rate-limited worker in the core/operations layer with checkpointed progress,
pause/resume, dry-run, retry/backoff, and per-tenant fairness. Expose status and
commands through `agentdbctl` and the admin API rather than an ad hoc script.

The status should report active/read-only versions, objects and bytes remaining,
last error, last progress time, configured rate, and historical copies that
still prevent retirement.

### 7. Enforce placement promises

Update the grouped RocksDB domain allocator and placement metadata so a
physical-isolation policy implies an exclusive domain, not merely an exclusive
tablet. Validate the invariant on startup and after tablet movement. Reject a
move that would violate the tenant's encryption/placement class.

### 8. Define export, backup, and restore semantics

Logical export can return plaintext to an authorized sink, but that is a new
security boundary and must be audited. Optionally support a ciphertext-preserving
export for tenant-controlled downstream decryption. Backups should retain
ciphertext and include only wrapped key-catalog records and key references.
Restore validation must prove all required historical keys are usable before
making the restored node ready. Tablet movement should implement the
key-readiness and authenticated-manifest protocol defined above.

## Delivery Plan

Implement the design in independently reviewable stages:

1. Publish this threat model and complete the durable plaintext inventory.
2. Introduce the cohesive keyring abstraction and explicit secure/insecure
   configuration without changing stored data.
3. Implement envelope v2 and production AEAD first for artifact content, sealed
   segments, and archives; add corruption, AAD-substitution, and golden-vector
   tests.
4. Encrypt the event and metadata hot paths with a bounded DEK cache and run the
   benchmark matrix.
5. Cover records, skills, indexes, idempotency, export, Raft, snapshots,
   checkpoints, backups, and temporary files; add raw-storage marker tests.
6. Add KEK rewrap and the online DEK rotation worker, admin API, CLI, and
   metrics.
7. Add two-phase revocation/crypto-shredding and enforce dedicated-domain
   placement for physical isolation classes.
8. Run backup/restore, unavailable-KMS, restart-during-rotation, revocation, and
   multi-node cache-invalidation drills before enabling BYOK claims.

Do not migrate existing rows merely by enabling a configuration flag. A mixed
v1/v2 reader, explicit migration status, and rollback window are required.

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

- Which AEAD suite and audited Rust implementation satisfy target platforms and
  any FIPS requirement?
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
