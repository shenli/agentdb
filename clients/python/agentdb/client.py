"""Synchronous Python SDK for the AgentDB native API."""

from __future__ import annotations

import dataclasses
import json as jsonlib
import os
from typing import Any, Callable, Iterator, List, Optional, Sequence

import requests

from ._secrets import validate_no_raw_secret_fields
from .conventions import (
    SessionEventKind,
    agent_status as agent_status_payload,
    context_pack_built as context_pack_built_payload,
    model_message as model_message_payload,
    tool_call as tool_call_payload,
    tool_result as tool_result_payload,
    user_message as user_message_payload,
)
from .errors import AgentDBError, from_response

TenantId = str
AgentId = str
SessionId = str
EventId = str
ArtifactId = str
RecordId = str
_RECORD_COMMIT_SEQ_HEADER = "X-AgentDB-Record-Commit-Seq"


@dataclasses.dataclass
class Event:
    tenant_id: TenantId
    agent_id: AgentId
    session_id: SessionId
    event_id: EventId
    seq: int
    kind: str
    payload: Any
    runtime_version: str = ""
    artifact_refs: List[dict] = dataclasses.field(default_factory=list)
    source_refs: List[dict] = dataclasses.field(default_factory=list)
    credential_refs: List[dict] = dataclasses.field(default_factory=list)
    metadata_key_id: Optional[str] = None
    metadata_key_version: Optional[str] = None
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
            runtime_version=_runtime_version_from_dict(d),
            artifact_refs=list(d.get("artifact_refs") or []),
            source_refs=list(d.get("source_refs") or []),
            credential_refs=list(d.get("credential_refs") or []),
            metadata_key_id=d.get("metadata_key_id"),
            metadata_key_version=d.get("metadata_key_version"),
            created_at_ms=d.get("created_at_ms", 0),
        )

    def ref(self) -> dict:
        return {"event_id": self.event_id}


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

    def ref(self) -> dict:
        return {"event_id": self.event_id}

def _runtime_version_from_dict(d: dict) -> str:
    runtime_version = d.get("runtime_version")
    if isinstance(runtime_version, str):
        return runtime_version
    return ""


@dataclasses.dataclass
class WatchItem:
    cursor: str
    event: Event

    @classmethod
    def from_dict(cls, d: dict) -> "WatchItem":
        return cls(cursor=d["cursor"], event=Event.from_dict(d["event"]))


class AgentDB:
    """Developer-friendly AgentDB entry point.

    `Client` remains the low-level tenant-scoped API. `AgentDB` binds that
    tenant once as a workspace so normal agent code does not thread `tenant_id`
    through every call.
    """

    def __init__(
        self,
        base_url: str = "",
        *,
        auth: Optional[tuple] = None,
        token: Optional[str] = None,
        get_token: Optional[Callable[[], str]] = None,
        timeout: float = 30.0,
        _client: Optional["Client"] = None,
    ) -> None:
        self.client = _client or Client(
            base_url,
            auth=auth,
            token=token,
            get_token=get_token,
            timeout=timeout,
        )

    def health_ready(self) -> bool:
        return self.client.health_ready()

    def storage_summary(self) -> dict:
        return self.client.get_fleet_storage_summary()

    def workspace(
        self,
        name: Optional[str] = None,
        *,
        tenant_id: Optional[TenantId] = None,
        policy: Optional[dict] = None,
    ) -> "Workspace":
        """Create a workspace or bind an existing tenant id.

        Pass `tenant_id` when the caller already has a tenant from auth/admin
        setup. Pass `name` for local/dev flows that bootstrap a tenant by
        external key.
        """

        if tenant_id is not None:
            return Workspace(self.client, tenant_id, name=name)
        external_key = name or os.environ.get("AGENTDB_WORKSPACE") or "default"
        user_info = getattr(self.client, "user_info", None)
        if callable(user_info):
            try:
                info = user_info()
            except AgentDBError as error:
                if error.status_code != 404:
                    raise
            else:
                resolved = _workspace_from_user_info(info, name)
                if resolved is not None:
                    resolved_id, resolved_name, data = resolved
                    return Workspace(
                        self.client, resolved_id, name=resolved_name or name, data=data
                    )
                if _user_info_is_tenant_bound(info):
                    # The credential IS bound to a workspace, just not the
                    # requested one — surfacing that beats silently creating
                    # a tenant the token cannot reach.
                    raise ValueError(
                        "authenticated user-info did not include an allowed workspace"
                    )
                # Unbound principals (root/admin/readonly, or a dev server
                # with auth open) fall through to the list/create bootstrap
                # flow below.

        for tenant in _optional_list(self.client, "list_tenants"):
            if tenant.get("external_key") == external_key:
                return Workspace(
                    self.client,
                    tenant["tenant_id"],
                    name=external_key,
                    data=tenant,
                )
        try:
            tenant = self.client.create_tenant(external_key, policy=policy)
        except AgentDBError as error:
            if error.status_code != 409:
                raise
            tenant = next(
                (
                    candidate
                    for candidate in _optional_list(self.client, "list_tenants")
                    if candidate.get("external_key") == external_key
                ),
                None,
            )
            if tenant is None:
                raise
        return Workspace(
            self.client,
            tenant["tenant_id"],
            name=external_key,
            data=tenant,
        )

    def session(
        self,
        session_id: SessionId,
        *,
        tenant_id: TenantId,
        agent_id: Optional[AgentId] = None,
    ) -> "SessionHandle":
        return Workspace(self.client, tenant_id).session(session_id, agent_id=agent_id)

    def session_by_id(
        self,
        session_id: SessionId,
        *,
        tenant_id: TenantId,
        agent_id: Optional[AgentId] = None,
    ) -> "SessionHandle":
        return self.session(session_id, tenant_id=tenant_id, agent_id=agent_id)


class Workspace:
    def __init__(
        self,
        client: "Client",
        tenant_id: TenantId,
        *,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        self._client = client
        self.tenant_id = tenant_id
        self.name = name
        self.data = data or {"tenant_id": tenant_id}
        self.artifacts = ArtifactCollection(self)
        self.records = RecordCollection(self)

    @property
    def id(self) -> TenantId:
        return self.tenant_id

    def agent(
        self,
        alias: Optional[str] = None,
        *,
        agent_id: Optional[AgentId] = None,
        config: Optional[dict] = None,
    ) -> "AgentHandle":
        """Create an agent by alias, or bind an existing canonical agent id."""

        if agent_id is not None:
            return AgentHandle(self, agent_id, alias=alias)
        if alias is not None:
            existing = _find_by_alias(
                _optional_list(self._client, "list_agents", self.tenant_id), alias
            )
            if existing is not None:
                return AgentHandle(self, existing["agent_id"], alias=alias, data=existing)
        try:
            data = self._client.create_agent(self.tenant_id, alias=alias, config=config)
        except AgentDBError as error:
            if alias is None or error.status_code != 409:
                raise
            existing = _find_by_alias(
                _optional_list(self._client, "list_agents", self.tenant_id), alias
            )
            if existing is None:
                raise
            data = existing
        return AgentHandle(self, data["agent_id"], alias=alias, data=data)

    def agent_by_id(self, agent_id: AgentId, *, alias: Optional[str] = None) -> "AgentHandle":
        return self.agent(alias=alias, agent_id=agent_id)

    def session(
        self,
        session_id: SessionId,
        *,
        agent_id: Optional[AgentId] = None,
    ) -> "SessionHandle":
        return SessionHandle(self, self._client.session(self.tenant_id, session_id, agent_id=agent_id))

    def session_by_id(
        self,
        session_id: SessionId,
        *,
        agent_id: Optional[AgentId] = None,
    ) -> "SessionHandle":
        return self.session(session_id, agent_id=agent_id)

    def watch(
        self,
        *,
        scope: str = "tenant",
        session_id: Optional[SessionId] = None,
        agent_id: Optional[AgentId] = None,
        from_cursor: Optional[str] = None,
        limit: int = 100,
    ) -> List[WatchItem]:
        return self._client.watch(
            self.tenant_id,
            scope=scope,
            session_id=session_id,
            agent_id=agent_id,
            from_cursor=from_cursor,
            limit=limit,
        )

    def storage_summary(self) -> dict:
        return self._client.get_tenant_storage_summary(self.tenant_id)


class AgentHandle:
    def __init__(
        self,
        workspace: Workspace,
        agent_id: AgentId,
        *,
        alias: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        self.workspace = workspace
        self.agent_id = agent_id
        self.alias = alias
        self.data = data or {"agent_id": agent_id}

    @property
    def id(self) -> AgentId:
        return self.agent_id

    def session(
        self,
        alias: Optional[str] = None,
        *,
        session_id: Optional[SessionId] = None,
    ) -> "SessionHandle":
        """Create a session by alias, or bind an existing canonical session id."""

        if session_id is not None:
            session = self.workspace._client.session(
                self.workspace.tenant_id,
                session_id,
                agent_id=self.agent_id,
            )
            return SessionHandle(self.workspace, session, agent=self)
        if alias is not None:
            existing = _find_by_alias(
                _optional_list(
                    self.workspace._client,
                    "list_sessions",
                    self.workspace.tenant_id,
                ),
                alias,
            )
            if existing is not None and existing.get("agent_id") == self.agent_id:
                session = self.workspace._client.session(
                    self.workspace.tenant_id,
                    existing["session_id"],
                    agent_id=self.agent_id,
                )
                return SessionHandle(self.workspace, session, agent=self)
        try:
            session = self.workspace._client.create_session(
                self.workspace.tenant_id,
                self.agent_id,
                alias=alias,
            )
        except AgentDBError as error:
            if alias is None or error.status_code != 409:
                raise
            existing = _find_by_alias(
                _optional_list(
                    self.workspace._client,
                    "list_sessions",
                    self.workspace.tenant_id,
                ),
                alias,
            )
            if existing is None or existing.get("agent_id") != self.agent_id:
                raise
            session = self.workspace._client.session(
                self.workspace.tenant_id,
                existing["session_id"],
                agent_id=self.agent_id,
            )
        return SessionHandle(self.workspace, session, agent=self)

    def session_by_id(self, session_id: SessionId) -> "SessionHandle":
        return self.session(session_id=session_id)


class SessionHandle:
    def __init__(
        self,
        workspace: Workspace,
        session: "Session",
        *,
        agent: Optional[AgentHandle] = None,
    ) -> None:
        self.workspace = workspace
        self._session = session
        self.agent = agent
        self.artifacts = ArtifactCollection(workspace)
        self.records = RecordCollection(workspace)

    @property
    def id(self) -> SessionId:
        return self._session.session_id

    @property
    def session_id(self) -> SessionId:
        return self._session.session_id

    @property
    def agent_id(self) -> Optional[AgentId]:
        return self._session.agent_id

    def append(self, payload: Any, **kwargs: Any) -> AppendResult:
        return self._session.append(payload, **kwargs)

    def append_and_fetch(self, payload: Any, **kwargs: Any) -> AppendResult:
        return self._session.append_and_fetch(payload, **kwargs)

    def append_user(
        self,
        text: str,
        *,
        role: str = "user",
        user_id: Optional[str] = None,
        channel: Optional[str] = None,
        metadata: Optional[dict] = None,
        fetch_last_n: int = 0,
        idempotency_key: Optional[str] = None,
    ) -> AppendResult:
        return self.append(
            user_message_payload(
                text,
                role=role,
                user_id=user_id,
                channel=channel,
                metadata=metadata,
            ),
            kind=SessionEventKind.USER_MESSAGE.value,
            fetch_last_n=fetch_last_n,
            idempotency_key=idempotency_key,
        )

    def model_message(
        self,
        text: str,
        *,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
        **kwargs: Any,
    ) -> AppendResult:
        return self.append(
            model_message_payload(text, **kwargs),
            kind=SessionEventKind.MODEL_MESSAGE.value,
            source_refs=source_refs,
            idempotency_key=idempotency_key,
        )

    def tool_call(
        self,
        tool_call_id: str,
        tool: str,
        input: dict,
        *,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
        **kwargs: Any,
    ) -> AppendResult:
        return self.append(
            tool_call_payload(tool_call_id, tool, input, **kwargs),
            kind=SessionEventKind.TOOL_CALL.value,
            source_refs=source_refs,
            idempotency_key=idempotency_key,
        )

    def tool_result(
        self,
        tool_call_id: str,
        summary: str,
        *,
        artifacts: Optional[Sequence[dict]] = None,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
        **kwargs: Any,
    ) -> AppendResult:
        return self.append(
            tool_result_payload(tool_call_id, summary, **kwargs),
            kind=SessionEventKind.TOOL_RESULT.value,
            artifact_refs=[artifact_ref(artifact) for artifact in artifacts or []],
            source_refs=source_refs,
            idempotency_key=idempotency_key,
        )

    def agent_status(self, status: str, **kwargs: Any) -> AppendResult:
        return self.append(
            agent_status_payload(status, **kwargs),
            kind=SessionEventKind.AGENT_STATUS.value,
        )

    def context_pack_built(
        self,
        context_pack_id: str,
        include: Sequence[str],
        *,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
        **kwargs: Any,
    ) -> AppendResult:
        return self.append(
            context_pack_built_payload(context_pack_id, include, **kwargs),
            kind=SessionEventKind.CONTEXT_PACK_BUILT.value,
            source_refs=source_refs,
            idempotency_key=idempotency_key,
        )

    def fetch(self, **kwargs: Any) -> List[Event]:
        return self._session.fetch(**kwargs)

    def fetch_recent(self, **kwargs: Any) -> List[Event]:
        return self._session.fetch_recent(**kwargs)

    def replay(self, **kwargs: Any) -> List[Event]:
        return self._session.replay(**kwargs)

    def materialize_replay_prefix(self, **kwargs: Any) -> List[Event]:
        return self._session.materialize_replay_prefix(**kwargs)

    def fork(self, child_session_id: Optional[str] = None, **kwargs: Any) -> "SessionHandle":
        return SessionHandle(self.workspace, self._session.fork(child_session_id, **kwargs), agent=self.agent)

    def context(self, **kwargs: Any) -> dict:
        return self._session.context(**kwargs)

    def raw(self) -> "Session":
        return self._session

    def storage(self) -> dict:
        return self._session.storage()

    def transition_lifecycle(self, target: str) -> dict:
        return self._session.transition_lifecycle(target)

    def restore(self, target: str = "cold") -> dict:
        return self._session.restore(target)


class ArtifactCollection:
    def __init__(self, workspace: Workspace) -> None:
        self._workspace = workspace

    def put(
        self,
        data: bytes,
        *,
        content_type: Optional[str] = None,
        sha256: Optional[str] = None,
    ) -> dict:
        return self._workspace._client.put_artifact(
            self._workspace.tenant_id,
            data,
            content_type=content_type,
            sha256=sha256,
        )

    def get(self, artifact_id: ArtifactId) -> dict:
        return self._workspace._client.get_artifact(self._workspace.tenant_id, artifact_id)

    def blob(self, artifact_id: ArtifactId) -> bytes:
        return self._workspace._client.get_artifact_blob(self._workspace.tenant_id, artifact_id)

    def verify_lifecycle(self) -> dict:
        return self._workspace._client.verify_artifact_lifecycle(self._workspace.tenant_id)


class RecordCollection:
    def __init__(self, workspace: Workspace) -> None:
        self._workspace = workspace

    def write(self, **body: Any) -> dict:
        return self._workspace._client.write_record(self._workspace.tenant_id, **body)

    def search(self, **body: Any) -> list:
        return self._workspace._client.search_records(self._workspace.tenant_id, **body)


def artifact_ref(artifact: dict) -> dict:
    ref = {"artifact_id": artifact["artifact_id"]}
    for key in ("content_type", "bytes", "sha256"):
        if artifact.get(key) is not None:
            ref[key] = artifact[key]
    return ref


def event_ref(event: Any) -> dict:
    if hasattr(event, "ref"):
        return event.ref()
    if isinstance(event, dict):
        return {"event_id": event["event_id"]}
    return {"event_id": event.event_id}


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
        self._record_commit_seq_by_tenant: dict[str, int] = {}

    @property
    def authenticated(self) -> bool:
        return self._auth is not None or self._token is not None or self._get_token is not None

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

    def user_info(self) -> dict:
        """Return authenticated principal/workspace metadata.

        Hosted deployments should expose this endpoint so application code can
        derive the tenant scope from authentication instead of passing
        `tenant_id` through normal agent code.
        """

        return self._request("GET", "/api/me")

    def list_tenants(self) -> list:
        data = self._request("GET", "/api/tenants")
        return data.get("tenants", [])

    def create_agent(
        self, tenant_id: TenantId, *, alias: Optional[str] = None, config: Optional[dict] = None
    ) -> dict:
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents",
            json={"alias": alias, "config": config or {}},
        )

    def list_agents(self, tenant_id: TenantId) -> list:
        data = self._request("GET", f"/api/tenants/{_quote(tenant_id)}/agents")
        return data.get("agents", [])

    def create_session(
        self, tenant_id: TenantId, agent_id: AgentId, *, alias: Optional[str] = None
    ) -> "Session":
        data = self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents/{_quote(agent_id)}/sessions",
            json={"alias": alias},
        )
        return Session(self, tenant_id, data["session_id"], data["agent_id"])

    def list_sessions(self, tenant_id: TenantId) -> list:
        data = self._request("GET", f"/api/tenants/{_quote(tenant_id)}/sessions")
        return data.get("sessions", [])

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

    def verify_artifact_lifecycle(self, tenant_id: TenantId) -> dict:
        return self._request(
            "GET", f"/api/tenants/{_quote(tenant_id)}/artifacts/verify-lifecycle"
        )

    def write_record(self, tenant_id: TenantId, **body: Any) -> dict:
        resp = self._raw_request("POST", f"/api/tenants/{_quote(tenant_id)}/records", json=body)
        data = _decode(resp)
        self._capture_record_commit_seq(tenant_id, resp)
        return data

    def search_records(self, tenant_id: TenantId, **body: Any) -> list:
        body = self._with_record_commit_seq(tenant_id, body)
        data = self._request("POST", f"/api/tenants/{_quote(tenant_id)}/records/search", json=body)
        return data.get("matches", [])

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

    def get_session_storage(self, tenant_id: TenantId, session_id: SessionId) -> dict:
        return self._request(
            "GET",
            f"/api/tenants/{_quote(tenant_id)}/sessions/{_quote(session_id)}/storage",
        )

    def get_tenant_storage_summary(self, tenant_id: TenantId) -> dict:
        return self._request(
            "GET",
            f"/api/tenants/{_quote(tenant_id)}/storage-summary",
        )

    def get_fleet_storage_summary(self) -> dict:
        return self._request("GET", "/api/storage/summary")

    def restore_session_storage(
        self, tenant_id: TenantId, session_id: SessionId, target: str = "cold"
    ) -> dict:
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/sessions/{_quote(session_id)}/restore",
            json={"target": target},
        )

    def transition_session_lifecycle(
        self, tenant_id: TenantId, session_id: SessionId, target: str
    ) -> dict:
        return self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/sessions/{_quote(session_id)}/lifecycle",
            json={"target": target},
        )

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
    ) -> requests.Response:
        hdrs = dict(headers or {})
        token = self._bearer_token()
        if token is not None and "Authorization" not in hdrs:
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

    def _capture_record_commit_seq(self, tenant_id: TenantId, resp: requests.Response) -> None:
        raw = getattr(resp, "headers", {}).get(_RECORD_COMMIT_SEQ_HEADER)
        if not raw:
            return
        try:
            seq = int(raw)
        except ValueError:
            return
        if seq > 0:
            self._record_commit_seq_by_tenant[tenant_id] = max(
                self._record_commit_seq_by_tenant.get(tenant_id, 0), seq
            )

    def _with_record_commit_seq(self, tenant_id: TenantId, body: dict) -> dict:
        seq = self._record_commit_seq_by_tenant.get(tenant_id)
        if seq is None or "min_visible_record_commit_seq" in body:
            return body
        return {**body, "min_visible_record_commit_seq": seq}


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

    @property
    def id(self) -> SessionId:
        return self.session_id

    def append(
        self,
        payload: Any,
        *,
        kind: str = "message",
        agent_id: Optional[AgentId] = None,
        fetch_last_n: int = 0,
        artifact_refs: Optional[list] = None,
        source_refs: Optional[list] = None,
        credential_refs: Optional[list] = None,
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
                credential_refs=credential_refs,
                idempotency_key=idempotency_key,
            )
        data = self._client._request(
            "POST",
            self._events_path(),
            json=self._append_body(
                kind, payload, agent_id, artifact_refs, source_refs, credential_refs
            ),
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
        credential_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> AppendResult:
        body = self._append_body(
            kind, payload, agent_id, artifact_refs, source_refs, credential_refs
        )
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

    def materialize_replay_prefix(self, *, from_seq: Optional[int] = None, limit: int = 100) -> List[Event]:
        params = {"limit": str(limit)}
        if from_seq is not None:
            params["from_seq"] = str(from_seq)
        data = self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/replay-prefix",
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

    def storage(self) -> dict:
        return self._client.get_session_storage(self.tenant_id, self.session_id)

    def transition_lifecycle(self, target: str) -> dict:
        return self._client.transition_session_lifecycle(
            self.tenant_id, self.session_id, target
        )

    def restore(self, target: str = "cold") -> dict:
        return self._client.restore_session_storage(
            self.tenant_id, self.session_id, target
        )

    def _append_body(
        self,
        kind: str,
        payload: Any,
        agent_id: Optional[AgentId],
        artifact_refs: Optional[list],
        source_refs: Optional[list],
        credential_refs: Optional[list],
    ) -> dict:
        aid = agent_id or self.agent_id
        if aid is None:
            raise ValueError("agent_id is required for append")
        validate_no_raw_secret_fields(payload)
        return {
            "agent_id": aid,
            "kind": kind,
            "payload": payload,
            "artifact_refs": artifact_refs or [],
            "source_refs": source_refs or [],
            "credential_refs": credential_refs or [],
        }

    def _events_path(self) -> str:
        return f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/events"


def _workspace_from_user_info(
    info: dict, requested_name: Optional[str]
) -> Optional[tuple[str, Optional[str], dict]]:
    if requested_name is None and isinstance(info.get("tenant_id"), str):
        return info["tenant_id"], None, info

    workspaces = [item for item in info.get("workspaces", []) if isinstance(item, dict)]
    target = requested_name or (
        info.get("default_workspace")
        if isinstance(info.get("default_workspace"), str)
        else None
    )
    if target is None and len(workspaces) == 1:
        workspace = workspaces[0]
        tenant_id = _workspace_tenant_id(workspace)
        if tenant_id is not None:
            return tenant_id, _workspace_name(workspace), workspace
    if target is not None:
        for workspace in workspaces:
            if target in _workspace_names(workspace):
                tenant_id = _workspace_tenant_id(workspace)
                if tenant_id is not None:
                    return tenant_id, _workspace_name(workspace), workspace

    current = info.get("workspace")
    if isinstance(current, dict) and (
        requested_name is None or requested_name in _workspace_names(current)
    ):
        tenant_id = _workspace_tenant_id(current)
        if tenant_id is not None:
            return tenant_id, _workspace_name(current), current
    return None


def _user_info_is_tenant_bound(info: dict) -> bool:
    """True when the principal carries any workspace binding at all."""

    return (
        isinstance(info.get("tenant_id"), str)
        or bool(info.get("workspaces"))
        or isinstance(info.get("workspace"), dict)
    )


def _workspace_tenant_id(workspace: dict) -> Optional[str]:
    value = workspace.get("tenant_id", workspace.get("id"))
    return value if isinstance(value, str) else None


def _workspace_name(workspace: dict) -> Optional[str]:
    for key in ("name", "workspace", "external_key"):
        value = workspace.get(key)
        if isinstance(value, str):
            return value
    return None


def _workspace_names(workspace: dict) -> set[str]:
    return {
        value
        for key in ("name", "workspace", "external_key")
        if isinstance((value := workspace.get(key)), str)
    }


def _find_by_alias(items: list, alias: str) -> Optional[dict]:
    return next(
        (item for item in items if isinstance(item, dict) and item.get("alias") == alias),
        None,
    )


def _optional_list(target: Any, method_name: str, *args: Any) -> list:
    """Call a discovery method when a compatible client/test double has it."""

    method = getattr(target, method_name, None)
    return method(*args) if callable(method) else []


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
    return {"Idempotency-Key": key} if key else {}


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
