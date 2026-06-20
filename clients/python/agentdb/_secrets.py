"""Client-side guards for keeping raw credentials out of event payloads."""

from __future__ import annotations

from typing import Any

_RAW_SECRET_FIELD_NAMES = {
    "accesstoken",
    "apikey",
    "authtoken",
    "authorization",
    "bearertoken",
    "clientsecret",
    "openaiapikey",
    "password",
    "passwd",
    "personalaccesstoken",
    "privatekey",
    "pwd",
    "refreshtoken",
    "secret",
    "secretaccesskey",
    "token",
}


def validate_no_raw_secret_fields(payload: Any) -> None:
    path = _first_raw_secret_field_path(payload, [], set())
    if path is not None:
        raise ValueError(
            "append payload field "
            f"`{path}` looks like a raw secret; use credential_refs for opaque "
            "vault / broker references"
        )


def _first_raw_secret_field_path(
    value: Any, path: list[str], seen: set[int]
) -> str | None:
    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in seen:
            return None
        seen.add(obj_id)
        for key, child in value.items():
            key_text = str(key)
            path.append(key_text)
            if _is_raw_secret_field_name(key_text):
                return _format_path(path)
            found = _first_raw_secret_field_path(child, path, seen)
            if found is not None:
                return found
            path.pop()
    elif isinstance(value, (list, tuple)):
        obj_id = id(value)
        if obj_id in seen:
            return None
        seen.add(obj_id)
        for idx, child in enumerate(value):
            path.append(f"[{idx}]")
            found = _first_raw_secret_field_path(child, path, seen)
            if found is not None:
                return found
            path.pop()
    return None


def _format_path(path: list[str]) -> str:
    out = ""
    for part in path:
        if part.startswith("["):
            out += part
        elif out:
            out += f".{part}"
        else:
            out = part
    return out


def _is_raw_secret_field_name(key: str) -> bool:
    normalized = "".join(ch.lower() for ch in key if ch.isascii() and ch.isalnum())
    return normalized in _RAW_SECRET_FIELD_NAMES
