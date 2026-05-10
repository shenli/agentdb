"""Helpers for running examples directly from a repo checkout."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_python_client_on_path() -> None:
    """Expose repo-local Python packages when examples run from a checkout.

    Installed environments already resolve `agentdb` normally. This fallback
    keeps `python session_first_demo.py` and adapter examples working from
    `examples/agents/python/`.
    """

    repo_root = Path(__file__).resolve().parents[3]
    package_roots = [
        (repo_root / "clients" / "python", "agentdb"),
        (
            repo_root / "adapters" / "openai-agents-agentdb",
            "openai_agents_agentdb",
        ),
    ]
    for package_root, package_name in package_roots:
        package_init = package_root / package_name / "__init__.py"
        if not package_init.exists():
            continue
        package_root_str = str(package_root)
        if package_root_str not in sys.path:
            sys.path.insert(0, package_root_str)
