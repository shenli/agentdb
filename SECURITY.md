# Security Policy

AgentDB is alpha software. Please do not use public GitHub issues for security
reports.

## Reporting A Vulnerability

Use GitHub private vulnerability reporting if it is enabled for the repository.
If it is not available, email the maintainer at the address listed on the GitHub
profile for this repository owner. Include:

- affected component or API;
- reproduction steps or proof of concept;
- expected impact;
- whether the issue is already public.

We will acknowledge valid reports as quickly as practical and coordinate a fix
before public disclosure.

## Security Model (current, honest)

What the current implementation actually provides — read this before
deploying anything beyond a laptop:

- **Authentication** is bearer-token based: a static installation token
  (root), plus metadata-backed tokens with `admin`, `readonly`, and
  per-tenant `tenant` scopes. Tenant-scoped tokens are enforced on every
  data route. There is no OAuth/OIDC, no per-user identity, and no
  fine-grained (sub-tenant) authorization yet.
- **A loopback dev server without a static token is open** until the
  first metadata-backed token is minted. That is a deliberate
  local-bootstrap convenience; do not bind to a non-loopback address
  without a token (the server refuses to, but proxies can defeat that).
- **Transport security is your job**: the server speaks plain HTTP.
  Terminate TLS in front of it before any token crosses a network.
- **Tenant isolation** is enforced in one process by one authorization
  layer; it is defense-in-depth-free by design at this stage. Treat a
  compromise of the process as a compromise of all tenants on it.
- **At-rest encryption** exists for metadata envelopes and content via
  the KMS adapter seam, but the default single-node deployment runs with
  identity (no-op) crypto adapters.

If your threat model needs more than this, put AgentDB behind
infrastructure that provides it, and tell us what you needed — the gap
list above is also the roadmap.

## Scope

In scope:

- authentication and authorization bypasses;
- data isolation failures between tenants/workspaces;
- remote code execution or arbitrary file access;
- exposure of secrets, tokens, or private runtime data;
- unsafe default network exposure.

Out of scope:

- denial-of-service findings that require local machine access only;
- issues in intentionally experimental cluster workflows unless they also affect
  the supported single-node path;
- benchmark-only performance regressions without a security impact.
