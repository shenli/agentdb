"""Single-node feature demo for local AgentDB testing.

This script replaces the manual curl flow for a local single-node server:

  1. Create a workspace/tenant.
  2. Create an agent and session.
  3. Append one session event.
  4. Upload and fetch one artifact.
  5. Run artifact lifecycle verification.

Run:

    cargo run -p agentdb-server -- serve --config config/agentdb.toml
    AGENTDB_URL=http://127.0.0.1:7878 \
      python3 examples/agents/python/single_node_feature_demo.py
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

from _bootstrap import ensure_repo_python_client_on_path

ensure_repo_python_client_on_path()

from agentdb import AgentDB, Client


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
USER = os.environ.get("AGENTDB_USER", "demo")
PASSWORD = os.environ.get("AGENTDB_PASSWORD", "demo-secret")
TENANT_KEY = os.environ.get("AGENTDB_TENANT", "single-node-demo")
AGENT_ALIAS = os.environ.get("AGENTDB_AGENT", "single-node-demo-agent")
SESSION_ALIAS = os.environ.get("AGENTDB_SESSION", "single-node-demo-session")


def dump(label: str, value: Any) -> None:
    print(f"\n## {label}")
    print(json.dumps(value, indent=2, sort_keys=True))


def main() -> None:
    health = Client(base_url=SERVER_URL, auth=(USER, PASSWORD))
    if not health.health_ready():
        raise SystemExit(
            f"AgentDB not ready at {SERVER_URL}. Start it with:\n"
            f"    cargo run -p agentdb-server -- serve --config config/agentdb.toml"
        )

    db = AgentDB(base_url=SERVER_URL, auth=(USER, PASSWORD))
    workspace = db.workspace(f"{TENANT_KEY}-{int(time.time() * 1000)}")
    agent = workspace.agent(AGENT_ALIAS, config={"demo": "single-node-feature"})
    session = agent.session(SESSION_ALIAS)

    dump(
        "created runtime objects",
        {
            "tenant_id": workspace.id,
            "agent_id": agent.id,
            "session_id": session.id,
            "server_url": SERVER_URL,
        },
    )

    append = session.append(
        {
            "role": "user",
            "text": "Local single-node feature demo event.",
            "created_by": "single_node_feature_demo.py",
        },
        kind="message",
        fetch_last_n=5,
    )
    dump(
        "append-and-fetch",
        {
            "event_id": append.event_id,
            "seq": append.seq,
            "recent": [
                {"seq": event.seq, "kind": event.kind, "payload": event.payload}
                for event in append.recent
            ],
        },
    )

    blob = b"hello artifact from the single-node feature demo\n"
    artifact = workspace.artifacts.put(
        blob,
        content_type="text/plain",
        sha256=hashlib.sha256(blob).hexdigest(),
    )
    fetched = workspace.artifacts.get(artifact["artifact_id"])
    fetched_blob = workspace.artifacts.blob(artifact["artifact_id"])
    dump(
        "artifact round trip",
        {
            "artifact_id": artifact["artifact_id"],
            "bytes": artifact["bytes"],
            "content_type": artifact["content_type"],
            "sha256": artifact["sha256"],
            "metadata_matches": fetched["artifact_id"] == artifact["artifact_id"],
            "blob_matches": fetched_blob == blob,
        },
    )

    lifecycle = workspace.artifacts.verify_lifecycle()
    dump("artifact lifecycle verification", lifecycle)

    if lifecycle.get("issues"):
        raise SystemExit("artifact lifecycle verification reported issues")


if __name__ == "__main__":
    main()
