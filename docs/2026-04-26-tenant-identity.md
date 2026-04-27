# Tenant identity: split the routing key from the display name

Date: 2026-04-26
Status: design accepted; implementation deferred to v0.2.0

## Problem

Today the `tenants` table looks roughly like this:

```sql
CREATE TABLE tenants (
    internal_id    BIGSERIAL PRIMARY KEY,
    external_id    TEXT NOT NULL UNIQUE,
    created_at_ms  BIGINT NOT NULL
);
```

`external_id` is a single field doing four jobs:

1. **Routing key** — every API request that names a tenant uses
   it; every locator row references it; the per-box auth
   middleware verifies `path_tenant == authed_user.tenant_external`.
2. **Operator-chosen string** — `agentdb user add ... --tenant
   <t>` takes whatever the operator types.
3. **URL surface** (legacy route) — appears in
   `/api/tenants/{external_id}/...`.
4. **Display name** by accident — operators with no other
   convention type the customer's name (`"acme-corp"`,
   `"big-co-inc"`) into this field, since it's the only place to
   put it.

Conflating routing identity with display name is the standard
multi-tenant SaaS anti-pattern. It bites the moment any operator
wants to **rename a tenant**:

- Customer rebrands (Acme Corp → Acme Inc.).
- Typo in the original `--tenant` argument noticed three
  months later.
- Internal reorganization: department `t-billing` becomes
  `t-finance`.

Renaming `external_id` is a multi-table data migration touching
`tenants`, `session_locators`, every `users` row,
`tenant_quotas`, and any audit-log row that references the
tenant. There's no in-place rename today, and no plan to add
one to the current schema.

## Why this is worth fixing properly

Adopters who hit this discover it at exactly the worst time:
their first customer asks them to rename. The "cheap" fix —
add a separate `display_name` column and tell operators "pick
`external_id` once and never change it" — works, but it
doesn't address two real problems:

1. The name `external_id` *suggests* it's user-facing, so
   operators naturally want to put a human-readable string
   there. Documentation alone doesn't fight nomenclature.
2. The legacy URL surface bakes the routing key into URLs the
   operator might publish (support tickets, dashboards,
   webhooks). Even if a `display_name` exists for UI, the URL
   shape stays — and someone *will* reorganize their tenant
   ids and find URLs broken.

The right fix splits identity from naming and renames the
surface to match.

## Decision

For v0.2.0 (next breaking version), AgentDB adopts the standard
three-field model:

```sql
CREATE TABLE tenants (
    internal_id    BIGSERIAL PRIMARY KEY,    -- unchanged; per-box compactness
    tenant_id      TEXT NOT NULL UNIQUE,     -- ULID / UUID, immutable, opaque
    slug           TEXT UNIQUE,              -- mutable, URL-friendly handle (optional)
    display_name   TEXT,                     -- mutable, human-readable
    created_at_ms  BIGINT NOT NULL
);
```

| Field | Mutable? | Set by | Used for |
|---|---|---|---|
| `internal_id` | No | server (BIGSERIAL) | per-box keys, foreign-key joins |
| `tenant_id` | **No** | server (ULID at tenant create) | routing, locators, all FK references at the meta layer, the only field the API and SDKs see |
| `slug` | yes (with redirect grace period) | operator | URL-friendly handle for support / debugging dashboards; **never** load-bearing in routing |
| `display_name` | freely | operator | UI labels; not unique; not in any URL |

**The end-user-facing API never names a tenant.** Tenant routing
is auth-derived (the M26 closeout established this in PR #81);
the only place `tenant_id` appears in client code is when the
operator's signup flow hands a freshly-generated ULID to the
SDK during user provisioning.

## API impact

The legacy `/api/tenants/{external_id}/sessions/{sid}/...` route
is deprecated and removed in v0.2.0. The auth-bound
`/api/sessions/{sid}/append_and_fetch` shape (already shipped
in PR #81) is the only path forward. Same for `fork`. Tenant
identity disappears from URLs entirely.

The Mongo wire's `$db` field changes meaning slightly: today
it carries `tenant_external`; in v0.2.0 it carries `tenant_id`
(the ULID), and the SCRAM-SHA-256 auth flow continues to verify
that `$db == authed_user.tenant_id`. The wire format is
unchanged; the semantic contract is "opaque routing key,"
which the existing checks already enforce.

The admin endpoints gain a small surface for managing names:

```
POST   /api/admin/tenants                    → creates tenant; returns generated tenant_id
PATCH  /api/admin/tenants/:tenant_id         → updates slug + display_name
GET    /api/admin/tenants                    → lists with all three fields
GET    /api/admin/tenants/:tenant_id         → details
DELETE /api/admin/tenants/:tenant_id         → tombstone (hard delete is its own follow-up)
```

`agentdb user add` no longer takes `--tenant` as a free-form
string. Either:

- `--tenant <ulid>` references an existing tenant by id, or
- `--new-tenant` (no value) creates a fresh tenant with a
  generated ULID and binds the new user to it.

## Migration plan

Existing v0.1.x deployments will need to migrate. The plan in
broad strokes:

1. **Schema migration** (014, presumably): add `tenant_id`,
   `slug`, `display_name` columns. Backfill `tenant_id` from
   `external_id` for existing rows (since that string is what
   locators and users currently reference). Drop the
   `external_id` column at the end of the migration after the
   API surface is updated.

2. **Locator / users / quota tables**: the FK column rename
   from `tenant_external_id` to `tenant_id` is mechanical;
   the values stay the same since we backfill identity from
   `external_id`.

3. **API + SDK surface**: a single coordinated cut. The
   legacy `/api/tenants/{tid}/...` route returns 410 Gone with
   a Link header pointing to the auth-bound route. SDKs ship
   v0.2.0 simultaneously with the route change; v0.1.x SDKs
   keep working against v0.1.x servers but not v0.2.x servers.

4. **Operator dashboard / docs**: update to show `tenant_id` /
   `slug` / `display_name` separately. Recommend operators
   pick a `slug` matching their support convention; default
   `slug = tenant_id` if unspecified.

5. **Backward-compat alias** (one minor version): for the
   first release after the cut, accept the old `external_id`
   in admin APIs as an alias for `tenant_id`. Drop in the
   release after.

This is a real breaking change — coordinated cut, not a
silent shim. The tenant_id semantics are too load-bearing to
keep dual identity around long-term.

## Out of scope (separate follow-ups)

- **Multi-tenant users.** A user belonging to multiple tenants
  is its own design problem (ACL table, per-request tenant
  context). This doc just fixes the single-tenant identity
  story.
- **External identity providers** (OIDC / JWT / Clerk /
  Auth0). Tenant claims from external IdPs need their own
  trust model. Out of scope for the rename; orthogonal.
- **Hard tenant deletion / GDPR.** Cascading delete across
  sessions, events, locators, indexes, audit log. Belongs in
  its own design doc — naming is a precondition, not the same
  problem.
- **Slug uniqueness across the cluster vs across a tenant
  hierarchy.** Today there's no tenant hierarchy; if we add
  one (sub-organizations, projects-within-tenant), slug
  uniqueness scope might need revisiting. Not happening in
  v0.2.0.

## Open questions

These need a position before the implementation PR opens:

1. **ULID vs UUID-v7 vs UUID-v4 for `tenant_id`.** ULID is
   shorter (26 chars vs 36) and lexicographically sortable by
   creation time, which is nice for debugging. UUID-v7 has
   the same property and is more standard. UUID-v4 is the
   default everywhere but loses the time ordering. **Lean: ULID.**
2. **Slug character set + length.** Lowercase ASCII letters,
   digits, hyphens; 1–63 chars; can't start/end with hyphen.
   That's the DNS-label rule and aligns with what most B2B
   SaaS products use. **Lean: DNS-label.**
3. **What happens when the operator omits `--new-tenant` and
   no tenant exists yet?** Either auto-create on first user
   in dev mode (consistent with how tenants work today), or
   require an explicit `--new-tenant` (safer, more explicit).
   **Lean: explicit, with `agentdb tenant add` as the
   primary admin command.**

## What gets logged here when the work happens

When the implementation PR opens, link it from the section
header: "Implemented in #NNN." When the breaking-cut release
ships, link the migration runbook. Keep this doc as the design
record; do not delete it after the work is done.
