#!/usr/bin/env python3
"""Scan all local Git refs and the publishable working tree, with redacted output."""

import pathlib
import shutil
import subprocess
import tempfile

root = pathlib.Path(__file__).resolve().parents[1]
common = ["gitleaks", "--redact", "--timeout=120", "--config", str(root / ".gitleaks.toml")]
subprocess.run(common + ["git", "--log-opts=--all", str(root)], check=True)
# Include uncommitted fixes/new scripts, but not ignored caches, developer data,
# or deleted historical artifacts. Avoid following symlinks outside the checkout.
paths = subprocess.check_output(
    ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
).decode().split("\0")
with tempfile.TemporaryDirectory(prefix="agentdb-secret-scan-") as directory:
    snapshot = pathlib.Path(directory)
    for name in set(paths) - {""}:
        source = root / name
        if source.is_file() and not source.is_symlink():
            target = snapshot / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    subprocess.run(common + ["dir", str(snapshot)], check=True)
print("PASS: secret scan of all local refs and publishable working tree")
