"""Synchronous Python SDK for the AgentDB V2 native API."""

from __future__ import annotations

import dataclasses
import json as jsonlib
import uuid
from typing import Any, Callable, Iterator, List, Optional, Sequence

import requests

from ._broadcast import BroadcastMessage
from ._sse import parse_lines
from .errors import AgentDBError, SubscriptionClosed, from_response

TenantId = str
AgentId = str
SessionId = str
EventId = str
ArtifactId = str
RecordId = str
SkillId = str


@dataclasses.dataclass
class Event:
    tenant_id: TenantId
    agent_id: AgentId
    session_id: SessionId
    event_id: EventId
    seq: int
    kind: str
    payload: Any
    created_at_ms: int = 0

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        return cls(
            tenant_id=d["tenant_id"],
            agent_id=d["agent_id"],
            session_id=d["session_id"],
            event_id=d["event_id"],
            seq=d["seq"],
            kind=d["kind"],
            payload=d.get("payload"),
            created_at_ms=d.get("created_at_ms", 0),
        )


@dataclasses.dataclass
class AppendResult:
    event_id: EventId
    seq: int
    recent: List[Event]

    @classmethod
    def from_dict(cls, d: dict) -> "AppendResult":
        return cls(
            event_id=d["event_id"],
            seq=d["seq"],
            recent=[Event.from_dict(e) for e in d.get("recent") or []],
        )


@dataclasses.dataclass
class WatchItem:
    cursor: str
    event: Event

    @classmethod
    def from_dict(cls, d: dict) -> "WatchItem":
        return cls(cursor=d["cursor"], event=Event.from_dict(d["event"]))


class Client:
    def __init__(
        self,
        base_url: str = "",
        *,
        auth: Optional[tuple] = None,
        token: Optional[str] = None,
        get_token: Optional[Callable[[], str]] = None,
        timeout: float = 30.0,
        _http: Optional[requests.Session] = None,
    ) -> None:
        if token is not None and get_token is not None:
            raise ValueError("pass `token` or `get_token`, not both")
        self.base_url = base_url.rstrip("/")
        self._auth = auth
        self._token = token
        self._get_token = get_token
        self.timeout = timeout
        self._http = _http or requests.Session()

    @property
    def authenticated(self) -> bool:
        return self._auth is not None

    def health_ready(self) -> bool:
        try:
            return (
                self._http.get(f"{self.base_url}/health/ready", timeout=self.timeout).status_code
                == 200
            )
        except requests.exceptions.RequestException:
            return False

    def create_tenant(self, external_key: str, policy: Optional[dict] = None) -> dict:
        return self._request(
            "POST",
            "/api/tenants",
            json={"external_key": external_key, "policy": policy or {}},
        )

    def get_tenant(self, tenant_id: TenantId) -> dict:
        return self._request("GET", f"/api/tenants/{_quote(tenant_id)}")

    def create_agent(
        self, tenant_id: TenantId, *, alias: Optional[str] = None, config: Optional[dict] = None
    ) -> dict:
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents",
            json={"alias": alias, "config": config or {}},
        )

    def create_session(
        self, tenant_id: TenantId, agent_id: AgentId, *, alias: Optional[str] = None
    ) -> "Session":
        data = self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents/{_quote(agent_id)}/sessions",
            json={"alias": alias},
        )
        return Session(self, tenant_id, data["session_id"], data["agent_id"])

    def session(
        self, tenant_id: TenantId, session_id: Optional[SessionId] = None, *, agent_id: Optional[AgentId] = None
    ) -> "Session":
        if session_id is None:
            session_id = tenant_id
        return Session(self, tenant_id, session_id, agent_id)

    def put_artifact(
        self,
        tenant_id: TenantId,
        data: bytes,
        *,
        content_type: Optional[str] = None,
        sha256: Optional[str] = None,
    ) -> dict:
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        if sha256:
            headers["X-Sha256"] = sha256
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/artifacts",
            data=data,
            headers=headers,
        )

    def get_artifact(self, tenant_id: TenantId, artifact_id: ArtifactId) -> dict:
        return self._request(
            "GET", f"/api/tenants/{_quote(tenant_id)}/artifacts/{_quote(artifact_id)}"
        )

    def get_artifact_blob(self, tenant_id: TenantId, artifact_id: ArtifactId) -> bytes:
        resp = self._raw_request(
            "GET", f"/api/tenants/{_quote(tenant_id)}/artifacts/{_quote(artifact_id)}/blob"
        )
        if 200 <= resp.status_code < 300:
            return resp.content
        raise from_response(resp.status_code, _response_body(resp))

    def write_record(self, tenant_id: TenantId, **body: Any) -> dict:
        return self._request("POST", f"/api/tenants/{_quote(tenant_id)}/records", json=body)

    def search_records(self, tenant_id: TenantId, **body: Any) -> list:
        data = self._request("POST", f"/api/tenants/{_quote(tenant_id)}/records/search", json=body)
        return data.get("records", [])

    def register_skill(
        self,
        tenant_id: TenantId,
        *,
        name: str,
        version: str,
        manifest: Optional[dict] = None,
        artifact_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/skills",
            json={
                "name": name,
                "version": version,
                "manifest": manifest or {},
                "artifact_refs": artifact_refs or [],
            },
            headers=_idem_header(idempotency_key),
        )

    def bind_skill(
        self,
        tenant_id: TenantId,
        agent_id: AgentId,
        skill_id: SkillId,
        *,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents/{_quote(agent_id)}/skills/bind",
            json={"skill_id": skill_id},
            headers=_idem_header(idempotency_key),
        )

    def watch(
        self,
        tenant_id: TenantId,
        *,
        scope: str = "tenant",
        session_id: Optional[SessionId] = None,
        agent_id: Optional[AgentId] = None,
        from_cursor: Optional[str] = None,
        limit: int = 100,
    ) -> List[WatchItem]:
        params = {
            "scope": scope,
            "limit": str(limit),
        }
        if session_id:
            params["session_id"] = session_id
        if agent_id:
            params["agent_id"] = agent_id
        if from_cursor:
            params["from_cursor"] = from_cursor
        data = self._request("GET", f"/api/tenants/{_quote(tenant_id)}/watch", params=params)
        return [WatchItem.from_dict(item) for item in data]

    def broadcast(self, *, channel: str, payload: Any) -> None:
        resp = self._raw_request(
            "POST",
            f"/api/channels/{_quote(channel)}/publish",
            json=payload,
            channel_auth=True,
        )
        if resp.status_code == 204:
            return
        raise from_response(resp.status_code, _response_body(resp))

    def subscribe_channel(self, *, channel: str, no_echo: bool = False) -> Iterator[BroadcastMessage]:
        params = {"no_echo": "true"} if no_echo else None
        resp = self._raw_request(
            "GET",
            f"/api/channels/{_quote(channel)}/subscribe",
            params=params,
            stream=True,
            channel_auth=True,
            headers={"Accept": "text/event-stream"},
        )
        try:
            if resp.status_code != 200:
                raise from_response(resp.status_code, _response_body(resp))
            for frame in parse_lines(resp.iter_lines(decode_unicode=True)):
                if frame.event and frame.event != "message":
                    continue
                if frame.data:
                    yield BroadcastMessage.from_envelope(jsonlib.loads(frame.data))
        finally:
            resp.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = self._raw_request(method, path, **kwargs)
        return _decode(resp)

    def _raw_request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        data: Optional[bytes] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        stream: bool = False,
        channel_auth: bool = False,
    ) -> requests.Response:
        hdrs = dict(headers or {})
        token = self._bearer_token() if channel_auth else None
        if token is not None:
            hdrs["Authorization"] = f"Bearer {token}"
        kwargs: dict = {"timeout": (self.timeout, None) if stream else self.timeout}
        if params:
            kwargs["params"] = params
        if hdrs:
            kwargs["headers"] = hdrs
        if self._auth is not None and "Authorization" not in hdrs:
            kwargs["auth"] = self._auth
        if json is not None:
            kwargs["json"] = json
        if data is not None:
            kwargs["data"] = data
        if stream:
            kwargs["stream"] = True
        return self._http.request(method, f"{self.base_url}{path}", **kwargs)

    def _bearer_token(self) -> Optional[str]:
        if self._get_token is not None:
            return self._get_token()
        return self._token


class Session:
    def __init__(
        self,
        client: Client,
        tenant_id: TenantId,
        session_id: SessionId,
        agent_id: Optional[AgentId] = None,
    ) -> None:
        self._client = client
        self.tenant_id = tenant_id
        self.session_id = session_id
        self.agent_id = agent_id

    def append(
        self,
        payload: Any,
        *,
        kind: str = "message",
        agent_id: Optional[AgentId] = None,
        fetch_last_n: int = 0,
        artifact_refs: Optional[list] = None,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> AppendResult:
        if fetch_last_n > 0:
            return self.append_and_fetch(
                payload,
                kind=kind,
                agent_id=agent_id,
                last=fetch_last_n,
                artifact_refs=artifact_refs,
                source_refs=source_refs,
                idempotency_key=idempotency_key,
            )
        data = self._client._request(
            "POST",
            self._events_path(),
            json=self._append_body(kind, payload, agent_id, artifact_refs, source_refs),
            headers=_idem_header(idempotency_key),
        )
        return AppendResult(event_id=data["event_id"], seq=data["seq"], recent=[])

    def append_and_fetch(
        self,
        payload: Any,
        *,
        kind: str = "message",
        agent_id: Optional[AgentId] = None,
        last: int = 20,
        max_bytes: int = 1_000_000,
        artifact_refs: Optional[list] = None,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> AppendResult:
        body = self._append_body(kind, payload, agent_id, artifact_refs, source_refs)
        body["last"] = last
        body["max_bytes"] = max_bytes
        data = self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/append-and-fetch",
            json=body,
            headers=_idem_header(idempotency_key),
        )
        return AppendResult.from_dict(data)

    def fetch(self, *, from_seq: int = 0, limit: int = 100) -> List[Event]:
        data = self._client._request(
            "GET", self._events_path(), params={"from_seq": str(from_seq), "limit": str(limit)}
        )
        return [Event.from_dict(e) for e in data]

    def replay(self, *, from_seq: Optional[int] = None, limit: int = 100) -> List[Event]:
        params = {"limit": str(limit)}
        if from_seq is not None:
            params["from_seq"] = str(from_seq)
        data = self._client._request(
            "GET",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/replay",
            params=params,
        )
        return [Event.from_dict(e) for e in data.get("events", [])]

    def fork(self, child_session_id: Optional[str] = None, *, at_seq: int = 0, alias: Optional[str] = None) -> "Session":
        data = self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/fork",
            json={"at_seq": at_seq, "alias": alias or child_session_id},
        )
        return Session(self._client, self.tenant_id, data["session_id"], data["agent_id"])

    def fetch_recent(self, *, last_n: int = 10) -> List[Event]:
        events = self.fetch(from_seq=0, limit=1000)
        return [] if last_n <= 0 else events[-last_n:]

    def subscribe(self, *, from_seq: int = 0, limit: int = 100) -> Iterator[Event]:
        resp = self._client._raw_request(
            "GET",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/subscribe",
            params={"from_seq": str(from_seq), "limit": str(limit)},
            stream=True,
            headers={"Accept": "text/event-stream"},
        )
        try:
            if resp.status_code != 200:
                raise from_response(resp.status_code, _response_body(resp))
            for frame in parse_lines(resp.iter_lines(decode_unicode=True)):
                if frame.event == "error":
                    body = jsonlib.loads(frame.data) if frame.data else {}
                    raise SubscriptionClosed(body.get("error", "unknown"), last_seq=body.get("last_seq"))
                if frame.data:
                    yield Event.from_dict(jsonlib.loads(frame.data))
        finally:
            resp.close()

    def context(
        self,
        *,
        max_events: int = 20,
        max_bytes: int = 1_000_000,
        include: Optional[Sequence[str]] = None,
        record_query: Optional[dict] = None,
    ) -> dict:
        return self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/context",
            json={
                "max_events": max_events,
                "max_bytes": max_bytes,
                "include": list(include or ["recent_events"]),
                "record_query": record_query,
            },
        )

    def _append_body(
        self,
        kind: str,
        payload: Any,
        agent_id: Optional[AgentId],
        artifact_refs: Optional[list],
        source_refs: Optional[list],
    ) -> dict:
        aid = agent_id or self.agent_id
        if aid is None:
            raise ValueError("agent_id is required for append")
        return {
            "agent_id": aid,
            "kind": kind,
            "payload": payload,
            "artifact_refs": artifact_refs or [],
            "source_refs": source_refs or [],
        }

    def _events_path(self) -> str:
        return f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/events"


def _decode(resp: requests.Response) -> Any:
    body = _response_body(resp)
    if 200 <= resp.status_code < 300:
        return body
    raise from_response(resp.status_code, body)


def _response_body(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return resp.text


def _quote(s: str) -> str:
    from urllib.parse import quote

    return quote(s, safe="-_.~")


def _idem_header(key: Optional[str]) -> dict:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


def append_one(
    base_url: str,
    *,
    tenant_id: TenantId,
    agent_id: AgentId,
    session_id: SessionId,
    payload: Any,
    auth: Optional[tuple] = None,
) -> AppendResult:
    client = Client(base_url, auth=auth)
    return client.session(tenant_id, session_id, agent_id=agent_id).append(payload)
