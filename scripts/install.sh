#!/usr/bin/env sh
# AgentDB install script.
#
# Detects platform, downloads the matching tarball from a GitHub
# Release, verifies the SHA-256, and installs the binary into a
# directory on PATH.
#
# Usage:
#
#   curl -sSL https://raw.githubusercontent.com/shenli/agentdb/main/scripts/install.sh | sh
#
# Customize:
#
#   AGENTDB_VERSION=v0.1.0  # default: latest GH Release
#   AGENTDB_PREFIX=$HOME/.local/bin  # install dir; default: ~/.local/bin
#                                    # (or /usr/local/bin if writable + exists in PATH)
#   AGENTDB_REPO=shenli/agentdb      # alternate fork
#
# Exit codes:
#   0 on success
#   1 on platform unsupported / download failed / checksum mismatch
#
# This script is intentionally POSIX-shell (no bash-isms) so it
# works on macOS's default `/bin/sh` and the BusyBox shells in
# minimal Linux containers.

set -eu

REPO="${AGENTDB_REPO:-shenli/agentdb}"
VERSION="${AGENTDB_VERSION:-}"
PREFIX="${AGENTDB_PREFIX:-}"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
warn() { printf '\033[33m! %s\033[0m\n' "$*" >&2; }
fail() { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }
info() { printf '  %s\n' "$*"; }

# --------- platform detection ---------

detect_platform() {
    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)      fail "unsupported OS: $(uname -s). Currently we ship binaries for Linux and macOS." ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)        arch="x86_64" ;;
        aarch64|arm64)       arch="aarch64" ;;
        *)                   fail "unsupported architecture: $(uname -m)" ;;
    esac
    PLATFORM="${os}-${arch}"
}

# --------- helpers ---------

# `curl --fail-with-body` is preferred for clean error surfacing
# but it's curl ≥ 7.76 only. Fall back to `--fail` on older Linux.
http_get() {
    if curl --help 2>&1 | grep -q -- "--fail-with-body"; then
        curl --fail-with-body --silent --show-error --location "$@"
    else
        curl --fail --silent --show-error --location "$@"
    fi
}

resolve_version() {
    if [ -n "$VERSION" ]; then return; fi
    bold "Resolving latest release..."
    # Hit the GitHub API for the latest release tag. Anonymous
    # rate limit is 60/h per IP — fine for an install script.
    api_url="https://api.github.com/repos/${REPO}/releases/latest"
    if ! VERSION="$(http_get "$api_url" | sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"; then
        fail "could not query GitHub API for the latest release. Set AGENTDB_VERSION explicitly."
    fi
    if [ -z "$VERSION" ]; then
        fail "GitHub API returned no tag_name. Has the repository cut a release yet?"
    fi
    info "latest = $VERSION"
}

resolve_prefix() {
    if [ -n "$PREFIX" ]; then return; fi
    if [ -d "/usr/local/bin" ] && [ -w "/usr/local/bin" ]; then
        PREFIX="/usr/local/bin"
    else
        PREFIX="$HOME/.local/bin"
        mkdir -p "$PREFIX"
    fi
}

# --------- the work ---------

main() {
    detect_platform
    resolve_version
    resolve_prefix

    asset="agentdb-${VERSION}-${PLATFORM}.tar.gz"
    base="https://github.com/${REPO}/releases/download/${VERSION}"
    bold "Installing AgentDB ${VERSION} for ${PLATFORM} → ${PREFIX}/agentdb"

    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT INT TERM

    info "downloading ${asset}"
    if ! http_get -o "${tmpdir}/${asset}" "${base}/${asset}"; then
        fail "download failed for ${base}/${asset}. Check that ${PLATFORM} is in the release assets."
    fi
    info "downloading ${asset}.sha256"
    if ! http_get -o "${tmpdir}/${asset}.sha256" "${base}/${asset}.sha256"; then
        warn "no .sha256 file found; skipping integrity check"
    else
        # The .sha256 file is "<hex>  <name>" (two spaces).
        # `shasum -a 256 -c` is portable; on Linux without it we
        # fall back to coreutils' `sha256sum -c`.
        if command -v shasum >/dev/null 2>&1; then
            (cd "$tmpdir" && shasum -a 256 -c "${asset}.sha256") \
                || fail "SHA-256 mismatch — refusing to install."
        elif command -v sha256sum >/dev/null 2>&1; then
            (cd "$tmpdir" && sha256sum -c "${asset}.sha256") \
                || fail "SHA-256 mismatch — refusing to install."
        else
            warn "no shasum/sha256sum tool found; skipping integrity check"
        fi
    fi

    info "extracting"
    tar -xzf "${tmpdir}/${asset}" -C "$tmpdir"
    # Tarball layout: one top-level directory containing the binary.
    extracted_dir="$(find "$tmpdir" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    if [ -z "$extracted_dir" ] || [ ! -f "${extracted_dir}/agentdb" ]; then
        fail "extracted tarball didn't contain ./agentdb"
    fi

    info "installing → ${PREFIX}/agentdb"
    install -m 0755 "${extracted_dir}/agentdb" "${PREFIX}/agentdb" 2>/dev/null \
        || cp "${extracted_dir}/agentdb" "${PREFIX}/agentdb" \
        && chmod 0755 "${PREFIX}/agentdb"

    bold "Installed."
    info "Try it:"
    info "  ${PREFIX}/agentdb serve --dev"
    case ":${PATH}:" in
        *":${PREFIX}:"*) ;;
        *)
            warn "${PREFIX} is not on your PATH yet. Add it to your shell profile:"
            warn "    echo 'export PATH=\"${PREFIX}:\$PATH\"' >> ~/.profile"
            ;;
    esac
}

main "$@"
