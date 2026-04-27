"""Mint a short-lived HMAC-signed JWT for the browser demo and emit a
self-contained `demo.html` that bundles the token into `index.html`.

Usage:
    python mint_token.py > demo.html

Then run AgentDB with matching JWT + CORS env (this script also
prints the recommended env vars to stderr):

    AGENTDB_TENANT_T_1_JWT_HMAC_SECRET=demo-secret \\
    AGENTDB_TENANT_T_1_JWT_ISSUER=https://demo.local \\
    AGENTDB_TENANT_T_1_CORS_ORIGINS=http://localhost:8000 \\
    cargo run -p agentdb-server -- serve --dev

Then in another terminal serve the demo:

    python -m http.server 8000

Open http://localhost:8000/demo.html.

The demo proves the friend's "browser → AgentDB direct, no broker"
shape: SSE subscription + append + JWT auth + CORS, all without an
application backend in between.
"""
import base64
import hashlib
import hmac
import json
import os
import sys
import time

SECRET = b"demo-secret"
TENANT = "t-1"
ISSUER = "https://demo.local"
TTL_SECS = 3600


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def jwt(claims: dict) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    seg1 = b64u(json.dumps(header, separators=(",", ":")).encode())
    seg2 = b64u(json.dumps(claims, separators=(",", ":")).encode())
    sig = hmac.new(SECRET, f"{seg1}.{seg2}".encode(), hashlib.sha256).digest()
    return f"{seg1}.{seg2}.{b64u(sig)}"


def main() -> None:
    claims = {
        "iss": ISSUER,
        "sub": "demo-user",
        "tenant": TENANT,
        "sessions": "*",
        "exp": int(time.time()) + TTL_SECS,
    }
    token = jwt(claims)

    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "index.html")) as f:
        html = f.read()

    inject = f'<script>window.AGENTDB_TOKEN = {json.dumps(token)};</script>'
    # Insert the token script right before the closing </head> tag
    # so the page module can read window.AGENTDB_TOKEN before its
    # subscribe() runs.
    if "</head>" in html:
        out = html.replace("</head>", f"  {inject}\n</head>")
    else:
        out = inject + html
    sys.stdout.write(out)

    print("\n--- Run AgentDB with these env vars ---", file=sys.stderr)
    print(f"AGENTDB_TENANT_T_1_JWT_HMAC_SECRET={SECRET.decode()}", file=sys.stderr)
    print(f"AGENTDB_TENANT_T_1_JWT_ISSUER={ISSUER}", file=sys.stderr)
    print("AGENTDB_TENANT_T_1_CORS_ORIGINS=http://localhost:8000", file=sys.stderr)


if __name__ == "__main__":
    main()
