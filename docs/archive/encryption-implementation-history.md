# Encryption Implementation History

Historical record extracted from the encryption design. Milestones describe
successive implementation stages; their “current”, “not yet”, and completeness
statements apply to those stages and are superseded by the
[current encryption status](../design/encryption-at-rest.md#current-implementation-status).
The original code-change and delivery plans below are retained for context,
not as current configuration instructions or a production-readiness claim.

## Implementation Milestones

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
hot path. On the x86 benchmark host (AES-NI) the **primitive** seals/opens a 256-byte value
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

**Milestone 17 (landed): primary-record coverage for agents and records.** The
agent and searchable-record **primary rows** are now sealed
under the tenant Metadata DEK, closing the plaintext gaps the coverage map
tracked (their export-ledger copies were already encrypted). Each row's AAD binds
its `(tenant, object_kind, object_id)` via new `MetadataObjectKind` variants, so
a ciphertext cannot be moved between rows. Every write path routes the primary
value through `encrypt_metadata`, and every read path — point reads
(`get_*_row`), tenant scans, and the startup **hydration** scans (`scan_all_*`) —
routes through `decrypt_metadata`, reconstructing the AAD from the key. With the
keyring disabled the rows take the identity/legacy path unchanged, so plaintext
deployments are unaffected and a disabled→enabled rollout stays readable. The
coverage-map tripwires for agent/record are flipped to assert *no* leak,
and a new test round-trips both through a fresh runtime that hydrates by
decrypting.

**Milestone 18 (landed): session and tenant primary-record coverage.** Extends
M17's pattern to the last two primary-record types, so **every** primary row
(tenant, agent, session, record) and the export-ledger are now sealed at
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
Metadata-DEK-sealed KV row (the tenant's own record, its
agent/session/record/artifact primary rows, its session events, and its
export-ledger entries),
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

**Milestone 23 (landed): safe DEK-version retirement.**
`Runtime::retire_tenant_dek_versions` (admin endpoint `POST
/api/admin/tenants/{id}/retire-dek-versions`, CLI `agentdbctl encryption retire`)
is the safe completion of a rotation: once a version's data is fully re-encrypted
away, the version is marked `retired` so it can never decrypt again (a precursor
to deleting its wrapped material). It is **safe by construction**: it first
re-encrypts (idempotent) and *refuses to proceed if any object could not be
re-sealed*; then it **scans every stored object** of each purpose and retires only
versions it did **not** observe still in use — never the active version, only
already-rotated-away read-only ones. Content versions are retired only when the
content backend can be enumerated (the report's `content_verified`); otherwise
content drainage is unverifiable and those versions are left untouched. A test
rotates both DEKs, retires, and confirms the old versions are refused by the
provider while the data still decrypts. This closes the key-lifecycle loop:
provision → rotate → re-encrypt → **retire** → (crypto-shred).

**Milestone 24 (landed): per-event AAD.** An individual event row is now sealed
under `object_id = {session}:{seq}` instead of just the session id, so its
sequence number is authenticated — a ciphertext cannot be moved to a different
position within the same session and still decrypt. Every event write and read
(the append hot path, the session scans, `get`-by-seq, and the re-encryption
walk) reconstructs the same `{session}:{seq}` from the event key. Append-log
**batches** (a group of events under one key, with no single seq) stay bound to
the session id via `append_batch_context`, and the export-ledger event entries —
already keyed by the unique `event_id` — are unchanged.

**Not yet done:** nothing required for a complete encryption-at-rest system.
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
5. Cover records, indexes, idempotency, export, Raft, snapshots,
   checkpoints, backups, and temporary files; add raw-storage marker tests.
6. Add KEK rewrap and the online DEK rotation worker, admin API, CLI, and
   metrics.
7. Add two-phase revocation/crypto-shredding and enforce dedicated-domain
   placement for physical isolation classes.
8. Run backup/restore, unavailable-KMS, restart-during-rotation, revocation, and
   multi-node cache-invalidation drills before enabling BYOK claims.

Do not migrate existing rows merely by enabling a configuration flag. A mixed
v1/v2 reader, explicit migration status, and rollback window are required.
