"""V2 shape regression tests — verify that:

- `Client` constructor does NOT accept `tenant`.
- `client.session()` does NOT accept `tenant`.
- Append uses v2 URL shapes (no `/api/tenants/` prefix).
- `broadcast()` and `subscribe_channel()` use v2 channel URLs.
- `subscribe()` uses v2 tenant-wide URL (no tenant kwarg required).

These are pure-Python unit tests (no network) — always run on `pytest`.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agentdb import Client


# ---------------------------------------------------------------------------
# Constructor shape
# ---------------------------------------------------------------------------


def test_constructor_no_tenant_kwarg():
    """tenant must no longer be a valid constructor kwarg."""
    Client(base_url="http://x", auth=("u", "p"))  # OK
    with pytest.raises(TypeError):
        Client(base_url="http://x", auth=("u", "p"), tenant="t")  # type: ignore[call-arg]


def test_constructor_accepts_auth_tuple():
    c = Client(base_url="http://x", auth=("alice", "secret"))
    assert c.authenticated is True


def test_constructor_accepts_token():
    c = Client(base_url="http://x", token="jwt-value")
    assert c.authenticated is False  # token is for channels, not Basic


def test_constructor_unauthenticated():
    c = Client(base_url="http://x")
    assert c.authenticated is False


def test_constructor_rejects_token_and_get_token_together():
    with pytest.raises(ValueError):
        Client(base_url="http://x", token="t", get_token=lambda: "t")


# ---------------------------------------------------------------------------
# session() shape
# ---------------------------------------------------------------------------


def test_session_method_no_tenant_kwarg():
    """session() must no longer accept tenant=..."""
    c = Client(base_url="http://x", auth=("u", "p"))
    s = c.session("s1")  # OK
    assert s is not None
    with pytest.raises(TypeError):
        c.session("s1", tenant="t")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# URL shapes — append
# ---------------------------------------------------------------------------


def _make_ok_response(body=None):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body or {"seq": 1, "recent": []}
    resp.raise_for_status.return_value = None
    return resp


def test_append_uses_v2_url():
    """Append must hit /api/sessions/{s}/append_and_fetch, not /api/tenants/..."""
    captured = []

    def fake_request(method, url, **kwargs):
        captured.append(url)
        return _make_ok_response()

    c = Client(base_url="http://x", auth=("u", "p"))
    with patch.object(c._http, "request", side_effect=fake_request):
        c.session("s1").append({"hello": "world"})

    assert any("/api/sessions/s1/append_and_fetch" in u for u in captured), (
        f"expected v2 append URL; captured: {captured}"
    )
    assert not any("/api/tenants/" in u for u in captured), (
        f"v1 URL leaked: {captured}"
    )


def test_fork_uses_v2_url():
    """fork() must hit /api/sessions/{s}/fork."""
    captured = []

    def fake_request(method, url, **kwargs):
        captured.append(url)
        return _make_ok_response({})

    c = Client(base_url="http://x", auth=("u", "p"))
    with patch.object(c._http, "request", side_effect=fake_request):
        c.session("s1").fork("s2")

    assert any("/api/sessions/s1/fork" in u for u in captured), (
        f"expected v2 fork URL; captured: {captured}"
    )
    assert not any("/api/tenants/" in u for u in captured), (
        f"v1 URL leaked: {captured}"
    )


# ---------------------------------------------------------------------------
# URL shapes — subscribe
# ---------------------------------------------------------------------------


def test_session_subscribe_uses_v2_url():
    """Per-session subscribe must use /api/sessions/{s}/subscribe."""
    c = Client(base_url="http://x", auth=("u", "p"))
    # subscribe() is lazy — inspect the path it would build without hitting network
    sess = c.session("s1")
    # Access the private method to check the path computation
    path = sess._subscribe_path()
    assert "/api/sessions/s1/subscribe" in path, f"unexpected path: {path}"
    assert "/api/tenants/" not in path, f"v1 URL leaked: {path}"


def test_client_subscribe_uses_v2_url():
    """Tenant-wide subscribe must use /api/subscribe (no tenant in path)."""
    c = Client(base_url="http://x", auth=("u", "p"))
    # subscribe() builds a path internally; verify via _subscribe_impl interception
    paths = []

    original = c._subscribe_impl

    def capturing_subscribe_impl(*, path, **kwargs):
        paths.append(path)
        return iter([])  # don't actually connect

    with patch.object(c, "_subscribe_impl", side_effect=capturing_subscribe_impl):
        try:
            next(iter(c.subscribe()))
        except StopIteration:
            pass

    assert paths == ["/api/subscribe"], f"unexpected path: {paths}"


# ---------------------------------------------------------------------------
# URL shapes — broadcast / subscribe_channel
# ---------------------------------------------------------------------------


def test_broadcast_uses_v2_channel_url():
    """broadcast() must hit /api/channels/{c}/publish."""
    captured = []

    def fake_request(method, url, **kwargs):
        captured.append(url)
        resp = MagicMock()
        resp.status_code = 204
        return resp

    c = Client(base_url="http://x", token="jwt-value")
    with patch.object(c._http, "request", side_effect=fake_request):
        c.broadcast(channel="my-chan", payload={"x": 1})

    assert any("/api/channels/my-chan/publish" in u for u in captured), (
        f"expected v2 channel publish URL; captured: {captured}"
    )
    assert not any("/api/tenants/" in u for u in captured), (
        f"v1 URL leaked: {captured}"
    )


def test_subscribe_channel_uses_v2_url():
    """subscribe_channel() must use /api/channels/{c}/subscribe."""
    c = Client(base_url="http://x", token="jwt-value")
    paths = []

    def fake_request(method, url, **kwargs):
        paths.append(url)
        resp = MagicMock()
        resp.status_code = 200
        resp.iter_lines.return_value = iter([])
        resp.close.return_value = None
        return resp

    with patch.object(c._http, "request", side_effect=fake_request):
        try:
            next(iter(c.subscribe_channel(channel="my-chan")))
        except StopIteration:
            pass

    assert any("/api/channels/my-chan/subscribe" in u for u in paths), (
        f"expected v2 channel subscribe URL; captured: {paths}"
    )
    assert not any("/api/tenants/" in u for u in paths), (
        f"v1 URL leaked: {paths}"
    )
