#!/usr/bin/env bash
# Install / refresh Python dependencies via uv, from the checked-in lockfile.
# Idempotent: safe to run repeatedly.
#
# `--locked` (rather than a bare `uv sync`) makes a stale uv.lock a hard
# failure instead of a silent re-resolution, so CI and laptops install the
# exact same versions. If this fails with "the lockfile is not up-to-date",
# run `uv lock` and commit the result.
#
# Required env vars: none.
# Required commands:  uv
#
# Usage: scripts/ci/setup.sh

set -euo pipefail
# shellcheck source=scripts/ci/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

cd "${repo_root}"

require_cmd uv

banner "Syncing Python dependencies (uv sync --locked)"
uv sync --locked

log "setup complete"
