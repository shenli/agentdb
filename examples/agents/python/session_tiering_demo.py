"""Session tiering demo.

This example shows the first tiered-storage slice in a developer-friendly way:

  1. write a few events to a normal warm session
  2. seal the session into the cold tier
  3. compact the sealed history into the archive tier
  4. verify reads still work unchanged
  5. append again and observe a new warm head open on top of archived history

Run me:

    cargo run -p agentdb-server -- serve --dev
    python examples/agents/python/session_tiering_demo.py
"""

from __future__ import annotations

import os
import time
from typing import Any

from _bootstrap import ensure_repo_python_client_on_path

ensure_repo_python_client_on_path()

from agentdb import AgentDB


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
USER = os.environ.get("AGENTDB_USER", "demo")
PASSWORD = os.environ.get("AGENTDB_PASSWORD", "demo-secret")
TENANT_KEY = os.environ.get("AGENTDB_TENANT", "session-tiering-demo")


def run_demo(db: Any, *, tenant_key: str = TENANT_KEY) -> dict:
    workspace = db.workspace(f"{tenant_key}-{int(time.time() * 1000)}")
    agent = workspace.agent("tiering-demo-agent", config={"demo": "session-tiering"})
    session = agent.session("tiering-demo-session")

    for i in range(3):
        session.append({"i": i}, kind="message")

    warm_events = session.fetch(from_seq=0, limit=100)
    cold_storage = session.transition_lifecycle("cold")
    cold_events = session.fetch(from_seq=0, limit=100)
    archive_storage = session.transition_lifecycle("archive")
    archive_events = session.fetch(from_seq=0, limit=100)

    resumed = session.append({"i": 99}, kind="message")
    warm_storage = session.storage()
    merged_events = session.fetch(from_seq=0, limit=100)

    return {
        "workspace_id": workspace.id,
        "agent_id": agent.id,
        "session_id": session.id,
        "warm_event_count": len(warm_events),
        "cold_event_count": len(cold_events),
        "archive_event_count": len(archive_events),
        "merged_event_count": len(merged_events),
        "cold_storage": cold_storage,
        "archive_storage": archive_storage,
        "warm_storage": warm_storage,
        "resumed_seq": resumed.seq,
        "event_seqs": [event.seq for event in merged_events],
    }


def main() -> None:
    db = AgentDB(base_url=SERVER_URL, auth=(USER, PASSWORD))
    if not db.health_ready():
        raise SystemExit(
            f"AgentDB not ready at {SERVER_URL}. Start it with:\n"
            f"    cargo run -p agentdb-server -- serve --dev"
        )

    result = run_demo(db)
    print(f"# workspace: {result['workspace_id']}")
    print(f"# agent:     {result['agent_id']}")
    print(f"# session:   {result['session_id']}")
    print("\n# cold storage after seal")
    print(
        "  lifecycle={lifecycle} sealed_segments={segments} warm_head_from_seq={head}".format(
            lifecycle=result["cold_storage"]["lifecycle"],
            segments=result["cold_storage"]["sealed_segment_count"],
            head=result["cold_storage"]["warm_head_from_seq"],
        )
    )
    print("\n# archive storage after compact")
    print(
        "  lifecycle={lifecycle} archive_manifest={manifest} sealed_segments={segments}".format(
            lifecycle=result["archive_storage"]["lifecycle"],
            manifest=(result["archive_storage"].get("archive_manifest") or {}).get("manifest_id", "-"),
            segments=result["archive_storage"]["sealed_segment_count"],
        )
    )
    print("\n# warm storage after resume")
    print(
        "  lifecycle={lifecycle} sealed_segments={segments} warm_head_from_seq={head}".format(
            lifecycle=result["warm_storage"]["lifecycle"],
            segments=result["warm_storage"]["sealed_segment_count"],
            head=result["warm_storage"]["warm_head_from_seq"],
        )
    )
    print("\n# counts")
    print(f"  warm read before seal: {result['warm_event_count']}")
    print(f"  cold read after seal:  {result['cold_event_count']}")
    print(f"  archive read after compact: {result['archive_event_count']}")
    print(f"  merged read after append: {result['merged_event_count']}")
    print(f"\n# resumed append seq: {result['resumed_seq']}")
    print(f"# merged seqs: {result['event_seqs']}")


if __name__ == "__main__":
    main()
