from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Optional

from agentdb import AgentDB
from session_first_demo import run_demo


@dataclass
class FakeAppendResult:
    event_id: str
    seq: int
    recent: list


@dataclass
class FakeEvent:
    event_id: str
    seq: int
    kind: str
    payload: Any
    artifact_refs: list[dict]
    source_refs: list[dict]


class FakeClient:
    def __init__(self) -> None:
        self._tenant_counter = itertools.count(1)
        self._agent_counter = itertools.count(1)
        self._session_counter = itertools.count(1)
        self._artifact_counter = itertools.count(1)
        self._sessions: dict[str, FakeSession] = {}

    def create_tenant(self, external_key: str, policy: Optional[dict] = None) -> dict:
        del external_key, policy
        return {"tenant_id": f"ten_{next(self._tenant_counter)}"}

    def create_agent(
        self,
        tenant_id: str,
        *,
        alias: Optional[str] = None,
        config: Optional[dict] = None,
    ) -> dict:
        del tenant_id, alias, config
        return {"agent_id": f"agt_{next(self._agent_counter)}"}

    def create_session(
        self,
        tenant_id: str,
        agent_id: str,
        *,
        alias: Optional[str] = None,
    ) -> "FakeSession":
        session_id = f"ses_{next(self._session_counter)}"
        session = FakeSession(tenant_id, agent_id, session_id)
        self._sessions[session_id] = session
        del alias
        return session

    def session(
        self,
        tenant_id: str,
        session_id: Optional[str] = None,
        *,
        agent_id: Optional[str] = None,
    ) -> "FakeSession":
        del tenant_id, agent_id
        assert session_id is not None
        return self._sessions[session_id]

    def put_artifact(
        self,
        tenant_id: str,
        data: bytes,
        *,
        content_type: Optional[str] = None,
        sha256: Optional[str] = None,
    ) -> dict:
        del tenant_id
        return {
            "artifact_id": f"art_{next(self._artifact_counter)}",
            "content_type": content_type,
            "bytes": len(data),
            "sha256": sha256,
        }


class FakeSession:
    def __init__(self, tenant_id: str, agent_id: str, session_id: str) -> None:
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self.session_id = session_id
        self._events: list[FakeEvent] = []

    def append(
        self,
        payload: dict,
        *,
        kind: str = "message",
        artifact_refs: Optional[list[dict]] = None,
        source_refs: Optional[list[dict]] = None,
    ) -> FakeAppendResult:
        seq = len(self._events)
        event = FakeEvent(
            event_id=f"evt_{seq + 1}",
            seq=seq,
            kind=kind,
            payload=payload,
            artifact_refs=artifact_refs or [],
            source_refs=source_refs or [],
        )
        self._events.append(event)
        return FakeAppendResult(event_id=event.event_id, seq=event.seq, recent=[])

    def context(self, *, include: list[str], max_events: int) -> dict:
        del include
        events = [
            {
                "event_id": event.event_id,
                "seq": event.seq,
                "kind": event.kind,
                "payload": event.payload,
            }
            for event in self._events[-max_events:]
        ]
        return {"events": events, "records": [], "skills": []}

    def replay(self, *, from_seq: int = 0, limit: int = 100) -> list[FakeEvent]:
        return [event for event in self._events if event.seq >= from_seq][:limit]

    def subscribe(self, *, from_seq: int = 0, limit: int = 100):
        count = 0
        for event in self._events:
            if event.seq >= from_seq:
                yield event
                count += 1
                if count >= limit:
                    return


def test_session_first_demo_replays_recovers_and_validates() -> None:
    result = run_demo(AgentDB(_client=FakeClient()), tenant_key="offline-demo")

    assert result["event_kinds"] == [
        "user_message",
        "tool_call",
        "tool_result",
        "context_pack_built",
        "model_message",
        "agent_status",
    ]
    assert result["replayed_kinds"] == [
        "user_message",
        "tool_call",
        "tool_result",
        "context_pack_built",
        "model_message",
    ]
    assert result["resumed_kinds"] == ["agent_status"]
    assert result["recovered_seq"] == result["last_seq_before_recovery"] + 1
    assert result["warnings"] == []
