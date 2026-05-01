"""AgentDB-backed LangGraph checkpoint saver.

The class follows LangGraph's checkpointer method names while keeping the
implementation independent of LangGraph imports. That lets the package be
tested as mapping code, and lets applications use it with whichever LangGraph
minor version they run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Iterable, Iterator, Mapping, Optional

from agentdb import SessionEventKind, agent_status, validate_adapter_events


SCHEMA_VERSION = "2026-04-28"


@dataclass(frozen=True)
class CheckpointTuple:
    config: Dict[str, Any]
    checkpoint: Dict[str, Any]
    metadata: Dict[str, Any]
    parent_config: Optional[Dict[str, Any]] = None
    pending_writes: tuple = ()


@dataclass(frozen=True)
class AdapterReadinessReport:
    warnings: tuple[str, ...]
    event_count: int
    checkpoint_count: int
    last_seq: Optional[int]

    @property
    def ready(self) -> bool:
        return not self.warnings


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

    def validate_readiness(self, config: Mapping[str, Any], *, limit: int = 1000) -> AdapterReadinessReport:
        """Check the LangGraph checkpoint timeline against adapter-readiness rules.

        This is intentionally advisory. It verifies the LangGraph-specific
        mapping and then runs the shared AgentDB convention validator over a
        recovered-status proof event so adapter tests exercise the same resume
        cursor contract as harnesses.
        """

        configurable = _configurable(config)
        session_id = _required(configurable, "agentdb_session_id")
        thread_id = _required(configurable, "thread_id")
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        events = list(session.replay(limit=limit))
        warnings: list[str] = []
        checkpoint_count = 0
        last_seq: Optional[int] = None

        for event in events:
            seq = _event_field(event, "seq")
            if seq is None:
                warnings.append("langgraph_event_missing_seq")
            elif last_seq is not None and int(seq) <= last_seq:
                warnings.append("langgraph_non_monotonic_seq")
            if seq is not None:
                last_seq = int(seq)

            if _event_field(event, "kind") != "langgraph_checkpoint":
                continue
            checkpoint_count += 1
            payload = _event_field(event, "payload") or {}
            if _event_field(payload, "thread_id") != thread_id:
                warnings.append("langgraph_checkpoint_thread_mismatch")
            if not _event_field(payload, "checkpoint_id"):
                warnings.append("langgraph_checkpoint_missing_checkpoint_id")

        if not events:
            warnings.append("langgraph_no_session_events")
        if checkpoint_count == 0:
            warnings.append("langgraph_no_checkpoint_events")

        if last_seq is not None:
            warnings.extend(
                validate_adapter_events(
                    [
                        {
                            "event_id": "evt_langgraph_readiness_recovered",
                            "seq": last_seq,
                            "kind": SessionEventKind.AGENT_STATUS.value,
                            "payload": agent_status("recovered", last_seen_seq=last_seq),
                        }
                    ]
                )
            )
        return AdapterReadinessReport(
            warnings=tuple(dict.fromkeys(warnings)),
            event_count=len(events),
            checkpoint_count=checkpoint_count,
            last_seq=last_seq,
        )

    def assert_ready(self, config: Mapping[str, Any], *, limit: int = 1000) -> AdapterReadinessReport:
        report = self.validate_readiness(config, limit=limit)
        if not report.ready:
            raise AssertionError("\n".join(report.warnings))
        return report


class AgentDBAsyncSaver:
    """Agent Server compatible async checkpointer backed by AgentDB.

    LangSmith Agent Server loads custom checkpointers through async
    `BaseCheckpointSaver` methods. This wrapper keeps the tested synchronous
    mapping above intact while adding the async method names Agent Server
    expects: `aput`, `aput_writes`, `aget_tuple`, `alist`, and
    `adelete_thread`.
    """

    checkpoint_namespace = AgentDBSaver.checkpoint_namespace
    writes_namespace = AgentDBSaver.writes_namespace
    thread_namespace = ["langgraph", "threads"]
    replay_limit = 10_000

    def __init__(self, client: Any, *, tenant_id: str, agent_id: str) -> None:
        self.client = client
        self.tenant_id = tenant_id
        self.agent_id = agent_id

    async def aput(
        self,
        config: Mapping[str, Any],
        checkpoint: Mapping[str, Any],
        metadata: Optional[Mapping[str, Any]] = None,
        new_versions: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        configurable = _configurable(config)
        thread_id = _required(configurable, "thread_id")
        checkpoint_ns = str(configurable.get("checkpoint_ns") or "")
        parent_checkpoint_id = configurable.get("checkpoint_id")
        session_id = await self._ensure_session_id(configurable)
        checkpoint_id = str(checkpoint.get("id") or checkpoint.get("checkpoint_id") or "")
        if not checkpoint_id:
            raise ValueError("checkpoint must contain id or checkpoint_id")

        payload = {
            "schema_version": SCHEMA_VERSION,
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "checkpoint": dict(checkpoint),
            "metadata": dict(metadata or {}),
            "new_versions": dict(new_versions or {}),
        }
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        result = await session.append(
            payload,
            kind="langgraph_checkpoint",
            idempotency_key=f"langgraph-checkpoint:{thread_id}:{checkpoint_ns}:{checkpoint_id}",
        )
        await self.client.write_record(
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
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "agentdb_session_id": session_id,
            }
        }

    async def aput_writes(
        self,
        config: Mapping[str, Any],
        writes: Iterable[Any],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = _configurable(config)
        thread_id = _required(configurable, "thread_id")
        checkpoint_ns = str(configurable.get("checkpoint_ns") or "")
        session_id = await self._ensure_session_id(configurable)
        checkpoint_id = configurable.get("checkpoint_id")
        payload = {
            "schema_version": SCHEMA_VERSION,
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "task_id": task_id,
            "task_path": task_path,
            "writes": list(writes),
        }
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        result = await session.append(
            payload,
            kind="langgraph_pending_writes",
            idempotency_key=f"langgraph-writes:{thread_id}:{checkpoint_ns}:{checkpoint_id}:{task_id}:{task_path}",
        )
        await self.client.write_record(
            self.tenant_id,
            namespace=self.writes_namespace,
            text=_compact(payload),
            metadata={**payload, "seq": result.seq},
            tags=["langgraph", "pending_writes", f"thread:{thread_id}", f"task:{task_id}"],
            source={"session_id": session_id, "seq": result.seq},
        )

    async def aget_tuple(self, config: Mapping[str, Any]) -> Optional[CheckpointTuple]:
        configurable = _configurable(config)
        session_id = await self._ensure_session_id(configurable)
        events = await self._thread_events(session_id)
        if self._is_deleted(events):
            return None

        checkpoint_ns = str(configurable.get("checkpoint_ns") or "")
        checkpoint_id = configurable.get("checkpoint_id")
        checkpoint_events = [
            event
            for event in events
            if event.kind == "langgraph_checkpoint"
            and (event.payload or {}).get("checkpoint_ns", "") == checkpoint_ns
        ]
        if checkpoint_id is not None:
            for event in checkpoint_events:
                if (event.payload or {}).get("checkpoint_id") == checkpoint_id:
                    return self._tuple_from_event(event, events, session_id)
            return None
        if checkpoint_events:
            return self._tuple_from_event(checkpoint_events[-1], events, session_id)
        return None

    async def aget(self, config: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        value = await self.aget_tuple(config)
        return value.checkpoint if value else None

    async def alist(
        self,
        config: Optional[Mapping[str, Any]],
        *,
        limit: int = 10,
        before: Optional[Mapping[str, Any]] = None,
        filter: Optional[Mapping[str, Any]] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        configurable = _configurable(config or {})
        if "thread_id" not in configurable:
            return
        session_id = await self._ensure_session_id(configurable)
        events = await self._thread_events(session_id)
        if self._is_deleted(events):
            return

        checkpoint_ns = str(configurable.get("checkpoint_ns") or "")
        before_id = _configurable(before or {}).get("checkpoint_id")
        metadata_filter = dict(filter or {})
        checkpoint_events = [
            event
            for event in events
            if event.kind == "langgraph_checkpoint"
            and (event.payload or {}).get("checkpoint_ns", "") == checkpoint_ns
        ]
        if before_id is not None:
            before_index = None
            for idx, event in enumerate(checkpoint_events):
                if (event.payload or {}).get("checkpoint_id") == before_id:
                    before_index = idx
                    break
            checkpoint_events = checkpoint_events[: before_index or 0]

        emitted = 0
        max_items = limit if limit is not None else len(checkpoint_events)
        for event in reversed(checkpoint_events):
            payload = event.payload or {}
            metadata = payload.get("metadata") or {}
            if any(metadata.get(key) != value for key, value in metadata_filter.items()):
                continue
            if emitted >= max_items:
                break
            yield self._tuple_from_event(event, events, session_id)
            emitted += 1

    async def adelete_thread(self, thread_id: str) -> None:
        configurable = {"thread_id": thread_id}
        session_id = await self._ensure_session_id(configurable)
        payload = {"schema_version": SCHEMA_VERSION, "thread_id": thread_id}
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        result = await session.append(
            payload,
            kind="langgraph_thread_deleted",
            idempotency_key=f"langgraph-thread-delete:{thread_id}",
        )
        await self.client.write_record(
            self.tenant_id,
            namespace=self.thread_namespace,
            text=_compact({**payload, "session_id": session_id, "deleted": True}),
            metadata={**payload, "session_id": session_id, "deleted": True, "seq": result.seq},
            tags=["langgraph", "thread", f"thread:{thread_id}", "deleted"],
            source={"session_id": session_id, "seq": result.seq},
        )

    async def _ensure_session_id(self, configurable: Mapping[str, Any]) -> str:
        existing = configurable.get("agentdb_session_id")
        if existing:
            return str(existing)

        thread_id = _required(configurable, "thread_id")
        rows = await self.client.search_records(
            self.tenant_id,
            namespace=self.thread_namespace,
            tags=[f"thread:{thread_id}"],
            limit=1,
        )
        if rows:
            metadata = rows[0].get("metadata") or {}
            session_id = metadata.get("session_id")
            if session_id:
                return str(session_id)

        try:
            session = await self.client.create_session(self.tenant_id, self.agent_id, alias=thread_id)
        except Exception:
            rows = await self.client.search_records(
                self.tenant_id,
                namespace=self.thread_namespace,
                tags=[f"thread:{thread_id}"],
                limit=1,
            )
            if rows:
                metadata = rows[0].get("metadata") or {}
                session_id = metadata.get("session_id")
                if session_id:
                    return str(session_id)
            raise

        session_id = session.session_id
        payload = {"schema_version": SCHEMA_VERSION, "thread_id": thread_id, "session_id": session_id}
        await self.client.write_record(
            self.tenant_id,
            namespace=self.thread_namespace,
            text=_compact(payload),
            metadata=payload,
            tags=["langgraph", "thread", f"thread:{thread_id}", f"session:{session_id}"],
        )
        return session_id

    async def _thread_events(self, session_id: str) -> list:
        session = self.client.session(self.tenant_id, session_id, agent_id=self.agent_id)
        return await session.replay(limit=self.replay_limit)

    def _is_deleted(self, events: Iterable[Any]) -> bool:
        return any(event.kind == "langgraph_thread_deleted" for event in events)

    def _tuple_from_event(self, event: Any, events: Iterable[Any], session_id: str) -> CheckpointTuple:
        payload = event.payload or {}
        thread_id = payload.get("thread_id")
        checkpoint_ns = payload.get("checkpoint_ns", "")
        checkpoint_id = payload.get("checkpoint_id")
        parent_checkpoint_id = payload.get("parent_checkpoint_id")
        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "agentdb_session_id": session_id,
            }
        }
        parent_config = None
        if parent_checkpoint_id:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_checkpoint_id,
                    "agentdb_session_id": session_id,
                }
            }
        return CheckpointTuple(
            config=config,
            checkpoint=dict(payload.get("checkpoint") or {}),
            metadata=dict(payload.get("metadata") or {}),
            parent_config=parent_config,
            pending_writes=self._pending_writes(events, checkpoint_ns, checkpoint_id),
        )

    def _pending_writes(
        self,
        events: Iterable[Any],
        checkpoint_ns: str,
        checkpoint_id: Optional[str],
    ) -> tuple:
        writes = []
        seen = set()
        for event in events:
            if event.kind != "langgraph_pending_writes":
                continue
            payload = event.payload or {}
            if payload.get("checkpoint_ns", "") != checkpoint_ns:
                continue
            if payload.get("checkpoint_id") != checkpoint_id:
                continue
            task_id = payload.get("task_id")
            for channel, value in payload.get("writes") or []:
                key = (task_id, channel)
                if key in seen:
                    continue
                seen.add(key)
                writes.append((task_id, channel, value))
        return tuple(writes)


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


def _event_field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)
