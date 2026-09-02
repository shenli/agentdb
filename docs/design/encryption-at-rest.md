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

**Milestone 3 (landed): the event and metadata hot paths are wired.** Persist's
`encrypt_metadata`/`decrypt_metadata` choke points route durable event values
and artifact-metadata rows through the keyring — sealed as `AGDB-ENVELOPE-v2`
before the KV write when enabled, legacy metadata-adapter path when disabled,
with the same version-detecting mixed reader. A raw-KV marker test proves an
event payload never appears in a stored KV value, and legacy plaintext events
stay readable after enabling. The bounded plaintext-DEK cache from M1 carries
the per-value hot path (the seal path was fixed to hit the cache instead of
re-deriving the DEK on every write). An `#[ignore]`d benchmark
(`tests/encryption_bench.rs`) measures both the crypto primitive and the append
hot path. On li-dev (x86, AES-NI) the **primitive** seals/opens a 256-byte value
at ~0.75M ops/s (XChaCha20-Poly1305) to ~1.8–2.2M ops/s (AES-256-GCM) — roughly
1–2 µs, well under 1% of a ~1 ms append, so the crypto cost clears the ≤10%
gate with large margin. The append **macro** benchmark shows a larger swing at
in-memory tight-loop scale, but that is dominated by the adaptive write
batcher's timing, not AEAD cost (the fast primitive proves it); it must be
re-measured on RocksDB with the batcher configured before being read as a real
regression.

**Milestone 4 (landed): the raw-storage marker coverage map.** A single-marker
invariant test (`tests/encryption_coverage.rs`) writes a distinct secret through
every durable path and asserts, per path, whether it appears in raw storage —
the regression detector the design's §4 calls for. It is two-sided: encrypted
paths must show no marker (a leak is a regression), and not-yet-encrypted paths
are asserted to *still* leak, so the test is also a tripwire that forces this map
to be updated when a later milestone encrypts one of them. Current coverage:

| Durable path | Value encrypted? |
|---|---|
| Session events | **Yes** (M3) |
| Artifact content | **Yes** (M2) |
| Artifact metadata row | **Yes** (M3) |
| Session segments / archives / replay prefixes | **Yes** (M2) |
| Agent primary records | **Yes** (M17) |
| Searchable record primary rows | **Yes** (M17) |
| Skill primary records | **Yes** (M17) |
| Export-ledger entries (all object types) | **Yes** (already sealed via `encrypt_metadata`) |
| Session primary records | **Yes** (M18) |
| Tenant primary records | **Yes** (M18) |
| Secondary indexes, aliases, idempotency, commit/outbox | No (routing/control; keys stay plaintext for scans) |

**Milestone 5 (landed): encryption precedes the Raft log in the clustered
path.** `Runtime::with_kv_content_keyring_and_replicated_logs` wires the keyring
into a replicated runtime — into both Persist (event/metadata) and the runtime
(content) — so a payload is sealed *before* `write_ops` reaches the replicated
log. A test (`tests/encryption_raft_ordering.rs`) appends an event on a
replicated runtime and confirms the plaintext marker is absent from the entire
KV (which holds both the log entries and the applied state), and that it still
round-trips through raft apply. This is the design's "verify Raft and snapshot
ordering" requirement, and it extends the working event/content encryption to
the production (clustered) deployment shape.

**Milestone 6 (landed): the wrapped-DEK key catalog and KEK.** `crypto/kek.rs`
adds a `KekProvider` seam (wrap/unwrap DEKs under a key-encryption key that
never leaves the manager) with a `LocalKekProvider` (a KMS-backed KEK is a
drop-in). `crypto/catalog.rs` adds a durable `KeyCatalog` (one wrapped-DEK
record per tenant/purpose/version plus an active-version pointer, under a
reserved `\0agentdb/keycatalog/` prefix so it is control state, never a tenant
payload) and a `CatalogKeyProvider` implementing the M1 `KeyProvider`: it mints
a random DEK on first use, wraps it under the KEK, stores it, and serves the
unwrapped lease; later calls read and unwrap the active (or a specific)
version, failing closed on a retired/revoked/missing version. This is the
production key source that replaces the dev `LocalKeyProvider` (which derived
keys deterministically): DEKs are now random and stored only wrapped. A test
proves a catalog-backed keyring's ciphertext survives a process restart (the
DEK is durably wrapped, not re-derived) and is unreadable under a different KEK
— i.e. losing/rotating-away the KEK crypto-shreds the data.

**Milestone 7 (landed): the server builds the keyring from configuration
(single-node).** `AGENTDB_ENCRYPTION_MODE=envelope` plus
`AGENTDB_ENCRYPTION_KEK_SECRET` (and optional `_KEK_REF` / `_SUITE`) makes the
server build a `CatalogKeyProvider` (wrapped-DEK catalog in the runtime KV) + a
`LocalKekProvider` + an `AeadTenantKeyring`, and wire it into the persistent
single-node runtime. It **fails closed** when envelope mode is requested without
a KEK secret, with the in-memory runtime (ephemeral keys), or with the
replicated tablet layout — the per-node local key catalog would mint a different
DEK per node, so cluster envelope encryption needs a shared/replicated catalog
(a dedicated follow-up). So encryption is now flippable end-to-end for a
single-node persistent deployment.

**Milestone 8 (landed): KEK rotation (rewrap).** `KeyCatalog::rewrap_all(old,
new)` rotates the key-encryption key — it unwraps every catalog DEK under the
old KEK and re-wraps it under the new one. The DEK *bytes* are unchanged, so
**no tenant data is re-encrypted** (only the wrapping changes). It skips DEKs
already under the new KEK, so a re-run after a crash mid-rewrap is idempotent,
and it fails closed if a DEK cannot be unwrapped under the old KEK. A test seals
data under KEK v1, rewraps the catalog v1→v2, and confirms the same ciphertext
opens under v2 while v1 alone no longer serves it. This is the design's "KMS
material rotation under one logical KEK." (Online *DEK* rotation — minting a new
DEK version and re-encrypting data v1→v2 — is separate and still to come.)

**Milestone 9 (landed, first cluster step): read-only provider + coordinated
provisioning.** `CatalogKeyProvider::new_read_only` makes the serve (seal/open)
path never mint a DEK — it fails closed on an unprovisioned key — while
`provision(tenant, purpose)` is the single coordinated entry point that mints,
wraps, and stores a DEK. This is the pre-provisioning model the cluster design
picked: a leader provisions a tenant's DEK once (e.g. at tenant creation) into
the shared/replicated catalog, and every node's read-only path then serves the
same key with no cross-node minting race. A test with two providers over one
shared catalog (two simulated nodes) shows a follower serving the exact DEK the
leader provisioned. The remaining cluster wiring — provisioning at tenant
creation, the catalog on the replicated store, and follower cache consistency —
is the next step.

**Milestone 10 (landed): the cluster-shared key catalog in control metadata.**
`agentdb_meta::dek_catalog` is the durable, cross-node table of wrapped DEK
records (one row per tenant/purpose/version; ciphertext only, never plaintext
keys) — the design's "wrapped DEK records in control metadata," and the shared
home a cluster needs (it sidesteps the node-local KV catalog and its
tablet-scoping). Provisioning is race-safe *across nodes*: `provision_if_absent`
uses `INSERT … ON CONFLICT DO NOTHING` and returns whichever row won, so two
nodes minting a DEK for the same tenant converge on one (the loser's key is
never used) — stronger than the in-process lock of the KV catalog. `list_all`
pre-loads a serving node's key cache; `get_active` / `get_version` / `set_status`
round out CRUD (the last for rotation/revocation). The DEK table is folded into
the consolidated schema.

**Milestone 11 (landed): the meta-catalog-backed key provider.**
`agentdb_api::dek_provider::MetaCatalogKeyProvider` serves per-tenant DEKs from
the shared M10 catalog under a KEK. Because the catalog is async but the
seal/open hot path is sync, it **pre-warms** every wrapped DEK into an in-memory
map at startup (`load`) and unwraps it under the KEK; the sync `KeyProvider`
serve path then reads that map and never touches the database. Minting is
explicit and async — `provision_tenant` (called at tenant creation on the
leader) writes through the race-safe `provision_if_absent`, and `refresh`
reloads the map so a follower picks up a newly-provisioned tenant's key. The
serve path never mints, so it fails closed on an unprovisioned tenant. Tests
prove: serve fails closed until provisioned then round-trips; **two providers
over one shared catalog (two nodes) seal/open under the same DEK** after the
follower refreshes; and a restarted provider re-warms from the catalog and opens
prior ciphertext.

**Milestone 12 (landed): the runtime provisions tenant keys at creation.** A
`TenantKeyProvisioner` trait (async) and an optional runtime hook: `create_tenant`
calls it right after the tenant is durably created, so the tenant's DEKs are
minted (on the leader) before any of its data can be written — exactly what the
read-only cluster serve path needs. `Runtime::with_kv_content_keyring_provisioner_and_replicated_logs`
wires a keyring + provisioner for the cluster shape, and `MetaCatalogKeyProvider`
implements the trait. A test confirms `create_tenant` provisions the tenant's
keys; the serve path stays unchanged when no provisioner is set (single-node).

**Milestone 13 (landed): the server enables cluster encryption.** For the
replicated **prefix-scoped** layout, server startup now builds the shared-catalog
keyring + provisioner (`build_cluster_keyring_and_provisioner`: a
`MetaCatalogKeyProvider` over the meta pool, used as both the keyring's key
source and the tenant provisioner) and wires them via
`with_kv_content_keyring_provisioner_and_replicated_logs_for_default_tablet` —
so the M7 replicated fail-closed is **lifted** for this layout. Envelope mode
still fails closed without a control plane, and (for now) on the grouped-rocksdb
layout, which needs its own keyring+provisioner constructor. `encryption_kek_and_suite`
is the shared KEK/suite parser for the single-node and cluster paths.

**Milestone 14 (landed): cluster encryption is complete.** The **grouped-rocksdb**
layout now has its keyring+provisioner constructor
(`with_tablet_store_lifecycle_content_keyring_provisioner_and_replicated_logs_for_default_tablet`),
so both replicated layouts encrypt. A background **refresh loop** reloads each
node's key cache on an interval (`AGENTDB_ENCRYPTION_REFRESH_MS`, default 30 s),
so a follower — or a node that becomes leader — picks up a tenant provisioned
after its startup (bounded eventual consistency, closing the failover gap). With
this, a real multi-node cluster encrypts tenant events and content end-to-end
(validated by a clean partition-scenario cluster smoke with encryption on).

**Milestone 15 (landed): online DEK rotation (control plane).** A tenant's DEK
can now be rotated on demand: `rotate` mints a fresh key at the *next* version,
makes it active (new writes seal under it), and demotes the previous version to
`read_only` — old ciphertext stays readable because every envelope records the
version it was sealed under, so **no data re-encryption is required for
correctness**. The new active version is written before the old is demoted, so a
crash mid-rotation leaves the active pointer aimed at a live record. Both catalog
backends implement it: the single-node `CatalogKeyProvider::rotate` (KV catalog)
and the cluster `MetaCatalogKeyProvider::rotate` (meta catalog), the latter
race-safe across nodes via `provision_if_absent` on the new version and picked up
by other nodes on their next `refresh`. A `TenantKeyRotator` runtime hook
(installed once at startup, like the provisioner) exposes it as
`Runtime::rotate_tenant_dek`, which also **invalidates the keyring's DEK cache**
for the tenant so the seal path adopts the new key immediately rather than after
the cache TTL. The admin endpoint `POST /api/admin/tenants/{id}/rotate-dek` drives
it (resolving the tenant first, so rotation never mints keys for a ghost tenant).
Tests cover both backends: active advances, old ciphertext still opens, new
writes use the new key, and a second cluster node adopts the new active version
after refresh. What remains is the **data-plane re-encryption walk** — a
rate-limited, checkpointed background job that rewrites old-version ciphertext
under the active DEK so a retired version can eventually be dropped (crypto-shred
/ key hygiene); it is a separate milestone because it is not needed for rotation
correctness, only for retiring old keys.

**Milestone 16 (landed): two-phase revocation / crypto-shred.** A tenant's DEKs
can be revoked on demand, in two strengths. A **soft** revoke marks every
version `revoked`, so the serve path refuses to unwrap it and the tenant's
encrypted data is immediately unreadable — but the wrapped bytes remain, so it is
reversible. A **crypto-shred** (`destroy = true`) deletes the wrapped key
material outright, so the data can never be decrypted again (irreversible; the
KEK-independent counterpart to the M6 "lose the KEK" shred, scoped to one
tenant). Both backends implement it — single-node `CatalogKeyProvider::revoke_tenant`
(KV catalog) and cluster `MetaCatalogKeyProvider::revoke_tenant` (meta catalog) —
and the cluster refresh/load now **skips non-readable rows**, so a soft revoke
stays effective after a reload and the periodic refresh never resurrects a
revoked key. A `TenantKeyRevoker` runtime hook (installed like the M15 rotator)
exposes `Runtime::revoke_tenant_dek(tenant, destroy)`, which invalidates the
keyring cache afterwards; the admin endpoint `POST /api/admin/tenants/{id}/revoke-dek`
(`?destroy=true` for a shred) drives it. Tests cover soft-revoke-then-shred at
every layer, including that a freshly-loaded cluster node will not serve a
revoked key.

**Milestone 17 (landed): primary-record coverage for agents, records, and
skills.** The agent, searchable-record, and skill **primary rows** are now sealed
under the tenant Metadata DEK, closing the plaintext gaps the coverage map
tracked (their export-ledger copies were already encrypted). Each row's AAD binds
its `(tenant, object_kind, object_id)` via new `MetadataObjectKind` variants, so
a ciphertext cannot be moved between rows. Every write path routes the primary
value through `encrypt_metadata`, and every read path — point reads
(`get_*_row`), tenant scans, and the startup **hydration** scans (`scan_all_*`) —
routes through `decrypt_metadata`, reconstructing the AAD from the key. With the
keyring disabled the rows take the identity/legacy path unchanged, so plaintext
deployments are unaffected and a disabled→enabled rollout stays readable. The
coverage-map tripwires for agent/record/skill are flipped to assert *no* leak,
and a new test round-trips all three through a fresh runtime that hydrates by
decrypting.

**Milestone 18 (landed): session and tenant primary-record coverage.** Extends
M17's pattern to the last two primary-record types, so **every** primary row
(tenant, agent, session, record, skill) and the export-ledger are now sealed at
rest. Session rows are written from three paths (create, and two event-append
paths); a shared `encrypted_session_put_op` helper seals all three identically so
none can drift into plaintext. The tenant row uses a single-id-segment key
(`record/tenant/{tenant_id}`), so it gets its own scan decoder
(`decrypt_tenant_scan_value`) where tenant and object id coincide. Tenant
encryption is safe because the Metadata DEK is minted before the first
`put_tenant` (M14 ordering) — for the system tenant too — or auto-generated on
first seal (single-node). The coverage map gains a tenant-policy tripwire, and
the hydration round-trip test now also reconstructs the session and tenant.

**Milestone 19 (landed): the external / KMS KEK seam.** Alongside the in-process
`LocalKekProvider`, a `CommandKekProvider` delegates DEK wrap/unwrap to an
external program — the KMS/HSM integration point, where **the KEK never enters
AgentDB's process**. The program is invoked per key operation with the operation
(`wrap`/`unwrap`), tenant, purpose, and KEK ref in the environment (so a wrapper
can bind tenant+purpose as the KMS encryption context) and the raw bytes on
stdin/stdout; a non-zero exit fails closed. Because DEKs are cached, this runs
only on a cache miss or a key operation. The server selects it with
`AGENTDB_ENCRYPTION_KEK_PROVIDER=command` +
`AGENTDB_ENCRYPTION_KEK_COMMAND[_ARGS]` (default `local`), on both the single-node
and cluster paths, so BYOK deployments plug in `aws kms`, `vault transit`, or an
HSM CLI without code changes. A real KMS SDK provider is now just another
`KekProvider`.

**Milestone 20 (landed): the data-plane DEK re-encryption walk.**
`Persist::reencrypt_tenant_metadata` (exposed as `Runtime::reencrypt_tenant_dek`
and the admin endpoint `POST /api/admin/tenants/{id}/reencrypt-dek`) rewrites a
tenant's metadata rows that are sealed under an **older** DEK version so they move
to the current active version — the data-plane half of rotation, after which the
old version has no remaining references and can be retired. It covers every
Metadata-DEK-sealed KV row (the tenant's own record, its agent/session/record/
skill/artifact primary rows, its session events, and its export-ledger entries),
deriving each row's AAD context from its key. It is **safe by construction**: each
row is decrypted under its recorded version *before* being re-sealed, so a
context-mapping bug fails that single row loudly (counted in the report's
`errors`) instead of corrupting it. It is **idempotent** — rows already at the
active version are skipped, so an interrupted pass simply re-runs — and a no-op
when encryption is disabled. A test rotates a tenant's Metadata DEK, re-encrypts,
and confirms every one of that tenant's rows moved from v1 to v2 (leaving other
tenants untouched), the data still decrypts through a fresh runtime, and a second
pass rewrites nothing.

**Milestone 21 (landed): content-blob re-encryption.** `reencrypt_tenant_dek` now
also rewrites a tenant's **content** blobs (session segments, archive bundles,
replay prefixes, and artifacts, sealed under the **Content** DEK) that are under
an older version, so a rotation — which advances both the Metadata and Content
DEKs — can be fully drained and the old versions of *both* purposes retired. It
enumerates the content backend (`list_objects`; a no-op when the backend cannot
enumerate its namespace) and, for each stale blob, recovers the object kind by
trying each content kind until one opens — safe because a wrong kind fails the
AAD check rather than corrupting the blob — then re-seals under the active Content
DEK through the existing `open_content`/`seal_content` path. Same idempotency and
counts as the metadata walk; a test rotates the Content DEK, re-encrypts, and
confirms the artifact blob moved from v1 to v2 and still decrypts through a fresh
runtime.

**Milestone 22 (landed): the `agentdbctl encryption` CLI.** The DEK lifecycle is
now driveable from the command line over the admin API: `agentdbctl encryption
rotate --tenant-id …`, `… revoke --tenant-id … [--destroy]`, and `… reencrypt
--tenant-id …` wrap the rotate / revoke-shred / re-encrypt admin endpoints, so an
operator (or an automation) can run the whole lifecycle without hand-crafting
HTTP calls.

**Not yet done:** automatic retirement of a fully-drained version.
Event AAD binds tenant + session + kind (not yet the per-event seq).
Dedicated-domain placement enforcement remains, as does graceful hydration when a
tenant's rows are undecryptable after a crypto-shred (today a `scan_all_*`
decrypt error propagates — a pre-existing behavior shared with the
already-encrypted artifact path).

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
