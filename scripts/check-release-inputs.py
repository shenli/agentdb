#!/usr/bin/env python3
"""Catch missing/stale workspace manifests in Docker's dependency-cache stage."""

import pathlib
import re
import tomllib

root = pathlib.Path(__file__).resolve().parents[1]
workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
expected = {f"{member}/Cargo.toml" for member in workspace["members"]}
dockerfile = (root / "Dockerfile").read_text().split("FROM deps AS builder")[0]
copied = set(re.findall(r"^COPY ((?:bin|crates|tools)/\S+/Cargo\.toml) ", dockerfile, re.M))
errors = []
for manifest in sorted(expected - copied):
    errors.append(f"Docker dependency stage is missing {manifest}")
for manifest in sorted(copied - expected):
    errors.append(f"Docker dependency stage has stale workspace member {manifest}")
for manifest in sorted(copied):
    if not (root / manifest).is_file():
        errors.append(f"Docker COPY input does not exist: {manifest}")
if errors:
    raise SystemExit("\n".join(errors))
print(f"PASS: Docker includes all {len(expected)} workspace manifests")
