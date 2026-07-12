"""Support-ticket agent demo with tiered storage.

This example combines the session-first runtime flow with the tiered-storage
flow so developers can validate a realistic harness against a deployed
AgentDB server:

  1. create a tenant, agent, and ticket session
  2. append user input, tool call, tool result, context, and model output
  3. transition the session through cold and archive
  4. restore the archive into a cold session
  5. append a follow-up after restore and confirm the session resumes cleanly

Run me:

    cargo run -p agentdb-server -- serve --config config/agentdb.toml
    PYTHONPATH=clients/python python3 examples/agents/python/ticket_agent_tiered_demo.py
"""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any

from _bootstrap import ensure_repo_python_client_on_path

ensure_repo_python_client_on_path()

from agentdb import AgentDB, context_source_refs, event_ref


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
TOKEN = os.environ.get("AGENTDB_TOKEN")
TENANT_KEY = os.environ.get("AGENTDB_TENANT", "ticket-tiered-demo")


def run_demo(db: Any, *, tenant_key: str = TENANT_KEY) -> dict:
    workspace = db.workspace(f"{tenant_key}-{int(time.time() * 1000)}")
    agent = workspace.agent(
        "ticket-tiered-demo-agent",
        config={"runtime": "ticket-demo", "tiering": "slatedb-s3"},
    )
    session = agent.session("ticket-SUP-42")

    user_event = session.append_user(
        "Summarize the checkout failure and tell me whether retry is safe.",
        metadata={"ticket_id": "SUP-42", "severity": "high"},
    )
    tool_call = session.tool_call(
        "call_collect_checkout_logs",
        "sandbox.exec",
        {"cmd": "python tools/collect_checkout_logs.py --ticket SUP-42"},
        timeout_ms=30_000,
        source_refs=[event_ref(user_event)],
        idempotency_key="ticket-demo-collect-SUP-42",
    )

    log_blob = (
        b"checkout.log\n"
        b"payment gateway timeout after 3000ms\n"
        b"retryable=true\n"
        b"orders affected=1\n"
    )
    artifact = session.artifacts.put(
        log_blob,
        content_type="text/plain",
        sha256=hashlib.sha256(log_blob).hexdigest(),
    )
    tool_result = session.tool_result(
        "call_collect_checkout_logs",
        "Collected checkout logs; one retryable gateway timeout was found.",
        artifacts=[artifact],
        output={
            "artifact_id": artifact["artifact_id"],
            "preview": "retryable=true",
            "orders_affected": 1,
        },
        duration_ms=412,
        source_refs=[event_ref(tool_call)],
    )

    context = session.context(include=["recent_events"], max_events=10)
    context_event = session.context_pack_built(
        "ctx_ticket_demo_1",
        ["recent_events"],
        max_events=10,
        selected_event_count=len(context.get("events", [])),
        source_refs=context_source_refs(context, max_refs=50),
    )
    model_event = session.model_message(
        "The failure was a retryable gateway timeout. Retry is safe for the one affected order.",
        model="demo-model",
        provider="demo",
        finish_reason="stop",
        context_pack_id="ctx_ticket_demo_1",
        source_refs=[event_ref(context_event)],
    )

    warm_events = session.fetch(from_seq=0, limit=100)
    cold_storage = session.transition_lifecycle("cold")
    archive_storage = session.transition_lifecycle("archive")
    archive_events = session.fetch(from_seq=0, limit=100)
    restored_storage = session.restore("cold")
    resumed = session.append_user(
        "Customer asked whether we should retry immediately or wait 5 minutes.",
        metadata={"ticket_id": "SUP-42", "follow_up": True},
    )
    final_storage = session.storage()
    final_events = session.fetch(from_seq=0, limit=100)

    return {
        "workspace_id": workspace.id,
        "agent_id": agent.id,
        "session_id": session.id,
        "artifact_id": artifact["artifact_id"],
        "warm_event_count": len(warm_events),
        "archive_event_count": len(archive_events),
        "final_event_count": len(final_events),
        "cold_storage": cold_storage,
        "archive_storage": archive_storage,
        "restored_storage": restored_storage,
        "final_storage": final_storage,
        "resume_seq": resumed.seq,
        "final_event_kinds": [event.kind for event in final_events],
        "final_event_seqs": [event.seq for event in final_events],
        "last_model_seq": model_event.seq,
        "last_tool_result_seq": tool_result.seq,
    }


def main() -> None:
    db = AgentDB(base_url=SERVER_URL, token=TOKEN)
    if not db.health_ready():
        raise SystemExit(
            f"AgentDB not ready at {SERVER_URL}. Start it with:\n"
            f"    cargo run -p agentdb-server -- serve --config config/agentdb.toml"
        )

    result = run_demo(db)
    print(f"# workspace: {result['workspace_id']}")
    print(f"# agent:     {result['agent_id']}")
    print(f"# session:   {result['session_id']}")
    print(f"# artifact:  {result['artifact_id']}")
    print("\n# storage transitions")
    print(
        "  cold: lifecycle={lifecycle} segments={segments}".format(
            lifecycle=result["cold_storage"]["lifecycle"],
            segments=result["cold_storage"]["sealed_segment_count"],
        )
    )
    print(
        "  archive: lifecycle={lifecycle} manifest={manifest}".format(
            lifecycle=result["archive_storage"]["lifecycle"],
            manifest=(result["archive_storage"].get("archive_manifest") or {}).get("manifest_id", "-"),
        )
    )
    print(
        "  restored: lifecycle={lifecycle} segments={segments}".format(
            lifecycle=result["restored_storage"]["lifecycle"],
            segments=result["restored_storage"]["sealed_segment_count"],
        )
    )
    print(
        "  final: lifecycle={lifecycle} warm_head_from_seq={head}".format(
            lifecycle=result["final_storage"]["lifecycle"],
            head=result["final_storage"]["warm_head_from_seq"],
        )
    )
    print("\n# event counts")
    print(f"  warm:    {result['warm_event_count']}")
    print(f"  archive: {result['archive_event_count']}")
    print(f"  final:   {result['final_event_count']}")
    print("\n# final timeline")
    print(f"  seqs:  {result['final_event_seqs']}")
    print(f"  kinds: {result['final_event_kinds']}")


if __name__ == "__main__":
    main()
