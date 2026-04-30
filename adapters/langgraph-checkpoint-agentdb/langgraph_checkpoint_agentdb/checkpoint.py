"""AgentDB-backed LangGraph checkpoint saver.

The class follows LangGraph's checkpointer method names while keeping the
implementation independent of LangGraph imports. That lets the package be
tested as mapping code, and lets applications use it with whichever LangGraph
minor version they run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional


SCHEMA_VERSION = "2026-04-28"


@dataclass(frozen=True)
class CheckpointTuple:
    config: Dict[str, Any]
    checkpoint: Dict[str, Any]
    metadata: Dict[str, Any]
    parent_config: Optional[Dict[str, Any]] = None
    pending_writes: tuple = ()


class AgentDBSaver:
    """LangGraph-compatible checkpoint saver over AgentDB sessions."""

    checkpoint_namespace = ["langgraph", "checkpoints"]
    writes_namespace = ["langgraph", "pending_writes"]

    def __init__(self, client: Any, *, tenant_id: str, agent_id: str) -> None:
        self.client = client
        self.tenant_id = tenant_id
        self.agent_id = agent_id

    def bootstrap_thread(self, thread_id: str, *, alias: Optional[str] = None) -> Dict[str, Any]:
        """Create an AgentDB session for a LangGraph thread."""
        session = self.client.create_session(self.tenant_id, self.agent_id, alias=alias or thread_id)
        return {"configurable": {"thread_id": thread_id, "agentdb_session_id": session.session_id}}

    def put(
        self,
        config: Mapping[str, Any],
        checkpoint: Mapping[str, Any],
        metadata: Optional[Mapping[str, Any]] = None,
        new_versions: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        configurable = _configurable(config)
        thread_id = _required(configurable, "thread_id")
        session_id = _required(configurable, "agentdb_session_id")
        checkpoint_id = str(checkpoint.get("id") or checkpoint.get("checkpoint_id") or "")
        if not checkpoint_id:
            raise ValueError("checkpoint must contain id or checkpoint_id")

        payload = {
            "schema_version": SCHEMA_VERSION,
            "thread_id": thread_id,
            "checkpoint_id": checkpoint_id,
            "checkpoint": dict(checkpoint),
            "metadata": dict(metadata or {}),
            "new_versions": dict(new_versions or {}),
        }
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        result = session.append(
            payload,
            kind="langgraph_checkpoint",
            idempotency_key=f"langgraph-checkpoint:{thread_id}:{checkpoint_id}",
        )
        self.client.write_record(
            self.tenant_id,
            namespace=self.checkpoint_namespace,
            text=_compact(payload),
            metadata={**payload, "seq": result.seq},
            tags=["langgraph", "checkpoint", f"thread:{thread_id}", f"checkpoint:{checkpoint_id}"],
            source={"session_id": session_id, "seq": result.seq},
        )
        return {
            "configurable": {
                **configurable,
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
                "agentdb_session_id": session_id,
            }
        }

    def put_writes(
        self,
        config: Mapping[str, Any],
        writes: Iterable[Any],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = _configurable(config)
        thread_id = _required(configurable, "thread_id")
        session_id = _required(configurable, "agentdb_session_id")
        checkpoint_id = configurable.get("checkpoint_id")
        payload = {
            "schema_version": SCHEMA_VERSION,
            "thread_id": thread_id,
            "checkpoint_id": checkpoint_id,
            "task_id": task_id,
            "task_path": task_path,
            "writes": list(writes),
        }
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        result = session.append(
            payload,
            kind="langgraph_pending_writes",
            idempotency_key=f"langgraph-writes:{thread_id}:{checkpoint_id}:{task_id}:{task_path}",
        )
        self.client.write_record(
            self.tenant_id,
            namespace=self.writes_namespace,
            text=_compact(payload),
            metadata={**payload, "seq": result.seq},
            tags=["langgraph", "pending_writes", f"thread:{thread_id}", f"task:{task_id}"],
            source={"session_id": session_id, "seq": result.seq},
        )

    def get_tuple(self, config: Mapping[str, Any]) -> Optional[CheckpointTuple]:
        checkpoint_id = _configurable(config).get("checkpoint_id")
        for item in self.list(config, limit=1000):
            if checkpoint_id is None or item.config["configurable"].get("checkpoint_id") == checkpoint_id:
                return item
        return None

    def list(
        self,
        config: Mapping[str, Any],
        *,
        limit: int = 10,
        before: Optional[Mapping[str, Any]] = None,
        filter: Optional[Mapping[str, Any]] = None,
    ) -> Iterator[CheckpointTuple]:
        del before, filter
        configurable = _configurable(config)
        session_id = _required(configurable, "agentdb_session_id")
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        events = session.replay(limit=limit)
        for event in reversed(events):
            if event.kind != "langgraph_checkpoint":
                continue
            payload = event.payload or {}
            checkpoint_id = payload.get("checkpoint_id")
            yield CheckpointTuple(
                config={
                    "configurable": {
                        **configurable,
                        "checkpoint_id": checkpoint_id,
                        "agentdb_session_id": session_id,
                    }
                },
                checkpoint=dict(payload.get("checkpoint") or {}),
                metadata=dict(payload.get("metadata") or {}),
            )


def _configurable(config: Mapping[str, Any]) -> Dict[str, Any]:
    return dict(config.get("configurable") or {})


def _required(configurable: Mapping[str, Any], key: str) -> str:
    value = configurable.get(key)
    if not value:
        raise ValueError(f"configurable.{key} is required")
    return str(value)


def _compact(value: Any) -> str:
    import json

    return json.dumps(value, sort_keys=True, separators=(",", ":"))
