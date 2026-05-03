from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Optional

from _bootstrap import ensure_repo_python_client_on_path

ensure_repo_python_client_on_path()

from agentdb import AgentDB
from session_tiering_demo import run_demo


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
        del alias
        session_id = f"ses_{next(self._session_counter)}"
        session = FakeSession(tenant_id, agent_id, session_id)
        self._sessions[session_id] = session
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


class FakeSession:
    def __init__(self, tenant_id: str, agent_id: str, session_id: str) -> None:
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self.session_id = session_id
        self.lifecycle = "warm"
        self.warm_head_from_seq = 0
        self.sealed_segment_count = 0
        self._events: list[FakeEvent] = []

    def append(
        self,
        payload: dict,
        *,
        kind: str = "message",
        fetch_last_n: int = 0,
        artifact_refs: Optional[list[dict]] = None,
        source_refs: Optional[list[dict]] = None,
        idempotency_key: Optional[str] = None,
    ) -> FakeAppendResult:
        del fetch_last_n, idempotency_key
        if self.lifecycle != "warm":
            self.lifecycle = "warm"
            self.warm_head_from_seq = len(self._events)
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

    def fetch(self, *, from_seq: int = 0, limit: int = 100) -> list[FakeEvent]:
        return [event for event in self._events if event.seq >= from_seq][:limit]

    def transition_lifecycle(self, target: str) -> dict:
        target = target.lower()
        if target == "cold":
            self.lifecycle = "cold"
            self.sealed_segment_count = 1 if self._events else 0
            self.warm_head_from_seq = len(self._events)
        elif target == "warm":
            self.lifecycle = "warm"
        elif target == "archive":
            self.lifecycle = "archive"
        return self.storage()

    def storage(self) -> dict:
        return {
            "session_id": self.session_id,
            "lifecycle": self.lifecycle,
            "next_seq": len(self._events),
            "warm_head_from_seq": self.warm_head_from_seq,
            "sealed_segment_count": self.sealed_segment_count,
            "sealed_segments": [],
        }


def test_session_tiering_demo_seals_reads_and_resumes() -> None:
    result = run_demo(AgentDB(_client=FakeClient()), tenant_key="offline-tiering")

    assert result["warm_event_count"] == 3
    assert result["cold_event_count"] == 3
    assert result["merged_event_count"] == 4
    assert result["cold_storage"]["lifecycle"] == "cold"
    assert result["cold_storage"]["sealed_segment_count"] == 1
    assert result["warm_storage"]["lifecycle"] == "warm"
    assert result["resumed_seq"] == 3
    assert result["event_seqs"] == [0, 1, 2, 3]
