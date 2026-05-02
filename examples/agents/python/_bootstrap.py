"""Helpers for running examples directly from a repo checkout."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_python_client_on_path() -> None:
    """Expose `clients/python` when examples run from a source checkout.

    Installed environments already resolve `agentdb` normally. This fallback
    keeps `python session_first_demo.py` working from `examples/agents/python/`.
    """

    repo_root = Path(__file__).resolve().parents[3]
    client_root = repo_root / "clients" / "python"
    package_init = client_root / "agentdb" / "__init__.py"
    if not package_init.exists():
        return

    client_root_str = str(client_root)
    if client_root_str not in sys.path:
        sys.path.insert(0, client_root_str)
