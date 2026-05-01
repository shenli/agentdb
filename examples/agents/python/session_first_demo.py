"""Session-first AgentDB demo.

This example shows AgentDB as the runtime database for an agent harness:

  1. Persist user input, tool call, tool result, artifacts, context provenance,
     and model output into one ordered session timeline.
  2. Recreate the harness from durable IDs after a simulated restart.
  3. Resume from the last seen `seq` and verify the event stream is adapter-ready.

Run me:

    # In one terminal:
    cargo run -p agentdb-server -- serve --dev

    # In another:
    pip install -e clients/python
    python examples/agents/python/session_first_demo.py
"""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any

from agentdb import (
    AgentDB,
    context_source_refs,
    event_ref,
    validate_adapter_events,
)


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
USER = os.environ.get("AGENTDB_USER", "demo")
PASSWORD = os.environ.get("AGENTDB_PASSWORD", "demo-secret")
TENANT_KEY = os.environ.get("AGENTDB_TENANT", "session-first-demo")


def run_demo(db: Any, *, tenant_key: str = TENANT_KEY) -> dict:
    """Run the session-first demo flow using a real or fake AgentDB client."""

    workspace = db.workspace(f"{tenant_key}-{int(time.time() * 1000)}")
    agent = workspace.agent(
        "session-first-demo-agent",
        config={"runtime": "demo-harness", "purpose": "session-first walkthrough"},
    )
    session = agent.session("support-ticket-42")

    user_event = session.append_user(
        "Summarize the checkout failure logs and tell me whether retry is safe.",
        metadata={"ticket_id": "SUP-42"},
    )
    tool_call_id = "call_collect_checkout_logs"
    call_event = session.tool_call(
        tool_call_id,
        "sandbox.exec",
        {"cmd": "python tools/collect_checkout_logs.py --ticket SUP-42"},
        timeout_ms=30_000,
        idempotency_key="collect-checkout-logs-SUP-42",
        source_refs=[event_ref(user_event)],
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
    session.tool_result(
        tool_call_id,
        "Collected checkout logs; one retryable gateway timeout was found.",
        artifacts=[artifact],
        output={
            "artifact_id": artifact["artifact_id"],
            "preview": "retryable=true",
        },
        duration_ms=412,
        source_refs=[event_ref(call_event)],
    )

    context = session.context(include=["recent_events"], max_events=10)
    context_pack_id = "ctx_checkout_recovery_1"
    context_event = session.context_pack_built(
        context_pack_id,
        ["recent_events"],
        max_events=10,
        selected_event_count=len(context.get("events", [])),
        source_refs=context_source_refs(context, max_refs=50),
    )
    model_event = session.model_message(
        "The checkout failure was a retryable payment gateway timeout. "
        "A retry is safe for the single affected order.",
        model="demo-model",
        provider="demo",
        finish_reason="stop",
        context_pack_id=context_pack_id,
        source_refs=[event_ref(context_event)],
    )

    last_seq_before_recovery = model_event.seq

    # Simulate a fresh harness process: only durable IDs and last seq survive.
    recovered = agent.session_by_id(session.id)
    replayed = recovered.replay(from_seq=0, limit=100)
    replayed_kinds = [event.kind for event in replayed]
    recovered_event = recovered.agent_status(
        "recovered",
        summary="Harness restored session from AgentDB replay.",
        last_seen_seq=last_seq_before_recovery,
    )

    resumed_events = []
    for event in recovered.subscribe(from_seq=last_seq_before_recovery + 1, limit=1):
        resumed_events.append(event)
        break

    validated_events = recovered.replay(from_seq=0, limit=100)
    warnings = validate_adapter_events(validated_events)
    return {
        "workspace_id": workspace.id,
        "agent_id": agent.id,
        "session_id": session.id,
        "artifact_id": artifact["artifact_id"],
        "event_kinds": [event.kind for event in validated_events],
        "replayed_kinds": replayed_kinds,
        "last_seq_before_recovery": last_seq_before_recovery,
        "recovered_seq": recovered_event.seq,
        "resumed_kinds": [event.kind for event in resumed_events],
        "warnings": warnings,
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
    print(f"# session: {result['session_id']}")
    print(f"# artifact:{result['artifact_id']}")
    print("\n# ordered session timeline")
    for index, kind in enumerate(result["event_kinds"]):
        print(f"  {index:>2}: {kind}")
    print("\n# replay after simulated restart")
    print("  " + " -> ".join(result["replayed_kinds"]))
    print(
        f"\n# resumed from seq {result['last_seq_before_recovery'] + 1}: "
        f"{', '.join(result['resumed_kinds'])}"
    )
    if result["warnings"]:
        raise SystemExit(f"adapter readiness warnings: {result['warnings']}")
    print("\n# adapter readiness: ok")


if __name__ == "__main__":
    main()
