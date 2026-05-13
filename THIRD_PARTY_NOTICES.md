# Third-Party Notices

AgentDB's own source code is licensed under the MIT License. Third-party
dependencies keep their original licenses and are not relicensed by AgentDB.

When distributing AgentDB source archives, binaries, containers, SDK packages,
or hosted artifacts, include the license and notice files required by bundled
or vendored third-party dependencies. In particular:

- keep the root [LICENSE](LICENSE) file for AgentDB's MIT license;
- preserve dependency copyright and license notices;
- preserve any Apache-2.0 `NOTICE` content when a distributed dependency
  provides one;
- run `scripts/license-check.sh` before publishing release artifacts;
- run with `LICENSE_CHECK_STRICT=1` for release validation so missing audit
  tools fail the check instead of being treated as warnings.

The repository does not vendor most Rust, Python, or npm dependencies. Package
managers resolve those dependencies from their registries, where each package
declares its own license metadata. Release artifacts that bundle dependencies
should generate a concrete notice inventory from the exact lockfile and build
inputs used for that release.
