# Admin-plane bearer-token auth

Status: in progress (2026-04-23).

M19 locked down `/api/tenants/*` with SCRAM + Basic Auth.
Admin endpoints (`/api/admin/*`) were explicitly out of
scope for M19 — the threat model assumed operators on
loopback. That holds for `agentdb serve --dev` but breaks
every non-toy deployment: the compose example exposes
`:7878` publicly, a cluster node advertises its admin
address to peers for cross-node forwarding, and so on.

This PR closes the gap with bearer-token auth.

## What's in / out

**In:**
- `admin_tokens` meta table (migration 004):
  `(token_id, token_hash_b64, description, created_at_ms,
  revoked_at_ms nullable)`. Hash is SHA-256 of the
  plaintext; plaintext never lands on disk.
- `agentdb_meta::admin_tokens` CRUD: `mint` (server
  generates 32 random bytes, hashes, persists, returns the
  plaintext once), `verify_by_hash`, `list_active`,
  `revoke`.
- Axum middleware on `/api/admin/*`. Under
  `AGENTDB_REQUIRE_ADMIN_AUTH=true`:
  - Extract `Authorization: Bearer <token>`; 401 if absent
    or malformed.
  - SHA-256 the presented token; look up in
    `admin_tokens`; 401 if missing or revoked.
  - Pass through on success.
- **Bootstrap exception.** When no active tokens exist
  yet, `POST /api/admin/tokens` is allowed without auth so
  operators can mint the first one. Every other admin
  endpoint still 401s. Once the first token is active, the
  mint endpoint is also gated.
- Admin endpoints: `POST /api/admin/tokens` (mint —
  returns plaintext ONCE), `GET /api/admin/tokens` (list,
  summaries only), `DELETE /api/admin/tokens/:id` (soft
  revoke — sets `revoked_at_ms`, never deletes the row so
  audit history is preserved).
- CLI: `agentdb admin-token add --description X | list |
  revoke <id>`. The `add` subcommand prints the plaintext
  to stdout exactly once; subsequent calls redact it.
  Reads `AGENTDB_ADMIN_TOKEN` env var (or `--token`) when
  invoking protected admin endpoints.
- Existing admin CLI subcommands (`drain`, `show-*`,
  `user *`, etc.) accept `AGENTDB_ADMIN_TOKEN` /
  `--token` and attach the Bearer header when set.
- `/health/live`, `/health/ready`, `/metrics`,
  `/openapi.json`, `/dashboard` **always** stay open —
  operators need them to probe without credentials.

**Out of scope (explicit):**
- **mTLS / certificate-based admin auth.** Real fleets
  would prefer mTLS; bearer tokens are the smaller step.
  Swappable later — middleware abstraction.
- **Token scopes / RBAC.** All admin tokens are full-access
  today. A future PR can add scoped tokens
  (e.g. "read-only", "shed-only").
- **Token rotation helpers.** Rotate = mint new + revoke
  old. Explicit two-step; a single `rotate` helper is a
  nice-to-have, not needed.
- **Audit log of admin actions.** Knowing "token X revoked
  token Y at time Z" would require a separate table and
  middleware hook.

## Design

### Bootstrap

When `AGENTDB_REQUIRE_ADMIN_AUTH=true` and the
`admin_tokens` table has zero `revoked_at_ms IS NULL`
rows, the auth middleware allows unauthenticated `POST
/api/admin/tokens`. Every other request still 401s.
Rationale: the alternative (require operators to mint a
token via direct SQLite access) is awkward and doesn't
generalize to a server behind a proxy.

### CLI + token storage

`agentdb admin-token add` returns a plaintext like
`agentdb_1oP2x...Y7k` (random base64). The operator
captures it into `AGENTDB_ADMIN_TOKEN` or equivalent.
Subsequent admin CLI calls attach it as
`Authorization: Bearer <token>`.

Token format: 32 random bytes, base64-url-encoded → 43
chars. Prefix `agentdb_` for eyeballing. Never persisted
in plaintext.

### Hash scheme

SHA-256(plaintext). Unsalted — single-use opaque tokens
don't benefit from per-row salt the way passwords do
(token space is 2^256, unguessable). Stored as base64 in
the `token_hash_b64` column for inspectability.

Could also use Argon2 / bcrypt; decided against because:
- Token entropy is huge (2^256). Brute-force is infeasible
  regardless of hash scheme.
- SHA-256 lookup is constant-time under `subtle` — no
  timing leak.
- Admin DB already stores SCRAM StoredKeys via plain
  base64 (M19 precedent).

### Scope of the middleware

Layered only on the `/api/admin` route subtree. The
existing tenant-auth middleware stays on `/api/tenants`.
`/health/*`, `/metrics`, `/openapi.json`, `/dashboard`,
`/api/stream` all sit outside both subtrees and remain
open.

`/api/sessions/:sid/fork` is currently tenant-scoped (not
admin) — auth there is covered by the existing Basic Auth
middleware; no change in this PR.

## Test plan

Unit (agentdb-meta::admin_tokens):
- `mint_returns_plaintext_and_persists_hash`
- `verify_by_hash_finds_mint`
- `verify_by_hash_rejects_revoked`
- `list_active_excludes_revoked`
- `revoke_is_idempotent`

Integration (agentdb-api):
- `admin_endpoint_without_bearer_is_401_when_auth_on`
- `bootstrap_mint_without_bearer_succeeds_when_no_tokens_exist`
- `second_mint_requires_bearer`
- `revoked_token_is_401`
- `valid_token_lets_admin_request_through`
- `health_and_metrics_always_open_regardless_of_auth`

## Rollout

`AGENTDB_REQUIRE_ADMIN_AUTH=false` (default) → no change.

Setting `true`:

1. Start the server. It comes up with the admin-auth
   middleware enabled but no tokens exist, so bootstrap
   allows the first mint.
2. `agentdb admin-token add --description "bootstrap"` →
   returns the plaintext.
3. `export AGENTDB_ADMIN_TOKEN=agentdb_...`
4. All subsequent admin CLI calls / curls carry the
   Bearer header.
5. Operators can mint additional tokens for different
   humans/services, revoke old ones, etc.
