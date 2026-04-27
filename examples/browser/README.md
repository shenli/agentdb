# Browser-direct demo

Three terminals to wire up AgentDB pub/sub directly from a browser
page — no application backend in between.

**1. Generate the token + serve the demo page**
```
cd examples/browser
python mint_token.py > demo.html      # bundles a 1-hour HMAC JWT into index.html
python -m http.server 8000
```

**2. Run AgentDB with matching JWT + CORS env**
```
AGENTDB_TENANT_T_1_JWT_HMAC_SECRET=demo-secret \
AGENTDB_TENANT_T_1_JWT_ISSUER=https://demo.local \
AGENTDB_TENANT_T_1_CORS_ORIGINS=http://localhost:8000 \
cargo run -p agentdb-server -- serve --dev
```

**3. Open http://localhost:8000/demo.html**

Click "Append" — the new event streams in below within milliseconds.

## What this proves

- Browser → AgentDB direct, no application backend in between.
- JWT auth (HMAC, no separate auth provider needed for the demo).
- CORS allowing http://localhost:8000 to read SSE from 127.0.0.1:7878.
- Real-time fan-out (no polling).

The shape of "UI shows 1 unread when a new message arrives, no
Redis between" is the same as this demo — append on one tab,
subscribe on another, watch the event arrive.

## A note on framing

`index.html` keeps the SSE loop hand-rolled (raw `fetch` + a
mini parser) so it's a self-contained reference for what the
protocol looks like on the wire. For a higher-level client-side
shape, the TypeScript SDK in `clients/typescript/` exposes a
`Client.subscribe()` async iterable that does the same thing
plus auto-reconnect on `jwt_expired`.
