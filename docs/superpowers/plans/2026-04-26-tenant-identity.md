# Tenant identity: split durable identity from mutable names

Date: 2026-04-26
Status: design accepted in principle; revise before implementation

## Implementation check

Checked against the workspace on 2026-05-02. The original version of this plan
is not implemented:

- `crates/agentdb-core/src/tenant.rs` stores `Tenant { tenant_id,
  external_key, policy, created_at_ms, deleted_at_ms }`; there are no `slug` or
  `display_name` fields.
- `tenant_id` is already a server-generated opaque ID (`ten_*`) and is the value
  used in current `/api/tenants/:tenant_id/...` routes.
- `external_key` still exists as an operator/developer-supplied tenant alias at
  creation time.
- `clients/typescript/src/client.ts` still exposes `createTenant(externalKey)`
  and sends `{ external_key, policy }`.

Conclusion: the durable identity split is partially implemented already. The
remaining work is to clarify the naming model and add mutable tenant names, not
to force an immediate tenantless API rewrite.

## Problem

Tenant identity has two different jobs:

1. **Durable routing identity**: immutable, system-generated, safe to use in
   storage keys, route paths, SDK calls, audit records, and cross-service
   references.
2. **Human naming**: mutable, operator-friendly, safe to show in dashboards,
   tickets, logs, and customer-facing workflows.

Current code has the durable `tenant_id`, which is good. The weak spot is
`external_key`: the name reads like identity, but the value is supplied by the
caller and is likely to be treated as a customer name or slug. That ambiguity
creates two failure modes:

- Operators may assume `external_key` is safe to rename after a customer
  rebrands or a typo is fixed.
- SDK users may treat `external_key` as the stable tenant handle even though
  the real stable handle is `tenant_id`.

The architecture should make the stable and mutable concepts impossible to
confuse.

## Decision

Keep `tenant_id` as the only durable identity. It remains immutable, opaque, and
server-generated.

Add explicit mutable naming fields:

```sql
CREATE TABLE tenants (
    internal_id    BIGSERIAL PRIMARY KEY,    -- if using SQL metadata
    tenant_id      TEXT NOT NULL UNIQUE,     -- immutable, opaque, server-generated
    slug           TEXT UNIQUE,              -- mutable URL/support handle
    display_name   TEXT,                     -- mutable human-readable label
    created_at_ms  BIGINT NOT NULL,
    deleted_at_ms  BIGINT
);
```

In the current in-memory/RocksDB model, the equivalent runtime shape should be:

```rust
pub struct Tenant {
    pub tenant_id: Id,
    pub slug: Option<String>,
    pub display_name: Option<String>,
    pub policy: serde_json::Value,
    pub created_at_ms: u64,
    pub deleted_at_ms: Option<u64>,
}
```

Field semantics:

| Field | Mutable? | Set by | Used for |
|---|---|---|---|
| `tenant_id` | No | server | routing, storage keys, SDK references, audit records |
| `slug` | Yes | operator or signup flow | support/debug/dashboard handle; never storage identity |
| `display_name` | Yes | operator or signup flow | UI label; not unique; never route identity |

`external_key` should be deprecated as a wire/API concept. If backward
compatibility is needed, accept it as an alias for `slug` during a transition
period, but do not document it as tenant identity.

## API impact

Do not remove `/api/tenants/{tenant_id}/...` as part of this feature.

Tenant-in-path routes are explicit, debuggable, and useful for admin/operator
tools. The path value is already the generated `tenant_id`, not a mutable
display name. Removing these routes would be a larger auth and SDK migration
than this feature requires.

Near-term API additions:

```text
POST  /api/tenants
GET   /api/tenants/{tenant_id}
PATCH /api/tenants/{tenant_id}
```

`POST /api/tenants` should return the generated `tenant_id` and may accept
optional `slug` and `display_name`.

`PATCH /api/tenants/{tenant_id}` should update only mutable fields such as
`slug`, `display_name`, and eventually policy fields if that is the chosen
policy-management surface. It must not allow changing `tenant_id`.

Auth-derived tenantless routes such as `/api/sessions/{session_id}/...` remain a
separate future ergonomics feature. They may be useful for end-user SDKs once
the auth model is mature, but they are not required to solve tenant naming.

## SDK and CLI impact

SDKs should make `tenant_id` the explicit durable handle:

- Prefer `createTenant({ slug, displayName, policy })` over
  `createTenant(externalKey, policy)`.
- Return `tenant_id`, `slug`, and `display_name` from tenant creation.
- Keep accepting the old positional `externalKey` form for one compatibility
  window if needed, mapping it to `slug`.
- Avoid naming any SDK method argument `externalKey` once the new shape is
  available.

CLI/admin tooling should follow the same language:

```bash
agentdb tenant create --slug acme --display-name "Acme Inc."
agentdb tenant update ten_... --slug acme-inc --display-name "Acme Inc."
```

User provisioning should bind users to `tenant_id`, not to slug/display name.
Slugs can help operators find tenants, but they are not authorization identity.

## Migration plan

This should be a non-breaking migration first:

1. **Add fields**: add `slug` and `display_name` to `Tenant`, persistence, DTOs,
   and API responses.
2. **Map legacy input**: keep accepting `external_key` on `POST /api/tenants`
   and store it as `slug` when `slug` is absent.
3. **Update SDKs**: introduce object-style create/update methods using `slug`
   and `displayName`; keep old positional creation as a compatibility shim.
4. **Update docs and dashboard**: consistently describe `tenant_id` as durable
   identity, `slug` as mutable support handle, and `display_name` as UI label.
5. **Warn on old names**: mark `external_key` as deprecated in DTO docs,
   generated schemas, and SDK comments.
6. **Remove `external_key` later**: only after a compatibility window and only
   if public API stability allows it.

Breaking follow-up, optional:

- Add auth-derived tenantless SDK routes if they improve developer ergonomics.
- Consider hiding tenant IDs from end-user SDK flows while keeping
  `/api/tenants/{tenant_id}/...` for admin/operator APIs.
- Remove deprecated `external_key` from the wire format.

## Non-goals

- Removing `/api/tenants/{tenant_id}/...` routes.
- Making slug load-bearing for routing, storage, auth, or foreign-key
  references.
- Supporting multi-tenant users or per-request tenant switching.
- Designing external identity provider mapping for OIDC/JWT/Clerk/Auth0.
- Hard tenant deletion or GDPR cascade semantics.

## Open questions

These need a position before implementation:

1. **Should `slug` be globally unique?** Lean yes for the current flat tenant
   model. If AgentDB later supports organizations/projects, the uniqueness
   scope may need to change.
2. **Should `slug` be optional?** Lean yes. Defaulting to `None` avoids making
   an arbitrary generated value look human-authored. Dashboards can fall back to
   `tenant_id`.
3. **Slug character set and length.** Lean DNS-label style: lowercase ASCII
   letters, digits, hyphens, 1-63 chars, cannot start or end with hyphen.
4. **Wire compatibility name.** Decide whether `external_key` remains accepted
   indefinitely as a deprecated alias or is removed in the next breaking API
   version.

## What gets logged here when the work happens

When implementation starts, link the PR from this section. When the
compatibility path is chosen, record whether `external_key` is retained as a
long-term alias or scheduled for removal.
