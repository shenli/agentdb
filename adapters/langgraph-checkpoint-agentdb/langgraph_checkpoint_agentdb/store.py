"""AgentDB-backed LangGraph store mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

from .checkpoint import SCHEMA_VERSION, _compact


@dataclass(frozen=True)
class StoreItem:
    namespace: Tuple[str, ...]
    key: str
    value: Any
    metadata: Mapping[str, Any]
    record: Mapping[str, Any]


class AgentDBStore:
    """LangGraph namespaced store over AgentDB searchable records."""

    root_namespace = ("langgraph", "store")

    def __init__(self, client: Any, *, tenant_id: str) -> None:
        self.client = client
        self.tenant_id = tenant_id

    def put(
        self,
        namespace: Sequence[str],
        key: str,
        value: Any,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
        tags: Optional[Iterable[str]] = None,
        embedding: Optional[Sequence[float]] = None,
    ) -> Mapping[str, Any]:
        ns = self._namespace(namespace)
        body = {
            "schema_version": SCHEMA_VERSION,
            "key": key,
            "value": value,
            "metadata": dict(metadata or {}),
        }
        return self.client.write_record(
            self.tenant_id,
            namespace=list(ns),
            text=_compact(body),
            metadata=body,
            tags=["langgraph", "store", f"key:{key}", *list(tags or [])],
            embedding=list(embedding) if embedding is not None else None,
        )

    def search(
        self,
        namespace: Sequence[str],
        *,
        query: Optional[str] = None,
        tags: Optional[Iterable[str]] = None,
        embedding: Optional[Sequence[float]] = None,
        limit: int = 10,
    ) -> list[StoreItem]:
        ns = self._namespace(namespace)
        records = self.client.search_records(
            self.tenant_id,
            namespace=list(ns),
            text=query,
            tags=list(tags or []),
            embedding=list(embedding) if embedding is not None else None,
            limit=limit,
        )
        return [self._item(ns, record) for record in records]

    def get(self, namespace: Sequence[str], key: str) -> Optional[StoreItem]:
        rows = self.search(namespace, tags=[f"key:{key}"], limit=1)
        return rows[0] if rows else None

    def _namespace(self, namespace: Sequence[str]) -> Tuple[str, ...]:
        return (*self.root_namespace, *tuple(str(part) for part in namespace))

    def _item(self, namespace: Tuple[str, ...], record: Mapping[str, Any]) -> StoreItem:
        metadata = record.get("metadata") or {}
        return StoreItem(
            namespace=namespace,
            key=str(metadata.get("key") or ""),
            value=metadata.get("value"),
            metadata=metadata.get("metadata") or {},
            record=record,
        )
