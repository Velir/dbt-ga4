#!/usr/bin/env bash
# Run the pytest suite against BigQuery, either on the locked dependency set
# or on one of the pinned version-matrix lanes.
#
# This is the single entrypoint that both local development and GitHub Actions
# invoke. Whatever this script does locally is exactly what CI does.
#
# Usage:
#   scripts/ci/test.sh [<lane>] [-- <pytest args>]
#
# Examples:
#   scripts/ci/test.sh                       # locked lane (uv.lock)
#   scripts/ci/test.sh latest                # newest dbt-core/dbt-bigquery <2.0.0
#   scripts/ci/test.sh 1_11_0                # pinned dbt 1.11.x
#   scripts/ci/test.sh -- -k derived_user    # locked lane, extra pytest args
#   scripts/ci/test.sh 1_10_0 -- -x -q
#
# Required env vars:
#   BIGQUERY_PROJECT   GCP project the throwaway test datasets are created in.
#                      Read from the repo-root .env if not already exported
#                      (same file pytest-dotenv reads).
#
# Credentials: dbt-bigquery uses `method: oauth`, i.e. Google Application
# Default Credentials. Locally run `gcloud auth application-default login`;
# in CI google-github-actions/auth exports GOOGLE_APPLICATION_CREDENTIALS.
# Not validated here — dbt surfaces a clearer error than we could.
#
# Required commands: uv

set -euo pipefail
# shellcheck source=scripts/ci/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

cd "${repo_root}"

require_cmd uv

usage="usage: scripts/ci/test.sh [<lane>] [-- <pytest args>]
lanes: (none, uses uv.lock) | latest | 1_12_0 | 1_11_0 | 1_10_0"

lane=""
pytest_args=()

if (( $# > 0 )) && [[ "$1" != "--" ]]; then
  lane="$1"
  shift
fi

if (( $# > 0 )); then
  [[ "$1" == "--" ]] || die "unexpected argument: $1
${usage}"
  shift
  pytest_args=("$@")
fi

# Pinned lanes run with --isolated --no-project, which bypasses pyproject.toml
# entirely, so their test deps must be named explicitly here. KEEP THIS LIST IN
# SYNC WITH pyproject.toml's [dependency-groups] dev group — if they drift, the
# matrix lanes silently stop matching the locked lane.
test_deps=(--with pytest --with pytest-dotenv)

# Every lane pins BOTH dbt-core and dbt-bigquery. Pinning only the adapter lets
# dbt-core float (onto a prerelease, in the worst case) — that is the exact bug
# that broke the dbt_artifacts matrix.
case "${lane}" in
  "")
    lane_desc="locked (uv.lock)"
    cmd=(uv run pytest .)
    ;;
  latest)
    lane_desc="latest (<2.0.0, floating)"
    cmd=(uv run --isolated --no-project
      --with "dbt-core<2.0.0" --with "dbt-bigquery<2.0.0"
      "${test_deps[@]}" pytest .)
    ;;
  1_12_0)
    lane_desc="dbt 1.12.x"
    cmd=(uv run --isolated --no-project
      --with "dbt-core~=1.12.0" --with "dbt-bigquery~=1.12.0"
      "${test_deps[@]}" pytest .)
    ;;
  1_11_0)
    lane_desc="dbt 1.11.x"
    cmd=(uv run --isolated --no-project
      --with "dbt-core~=1.11.0" --with "dbt-bigquery~=1.11.0"
      "${test_deps[@]}" pytest .)
    ;;
  1_10_0)
    lane_desc="dbt 1.10.x (matrix floor, matches the current lock)"
    cmd=(uv run --isolated --no-project
      --with "dbt-core~=1.10.0" --with "dbt-bigquery~=1.10.3"
      "${test_deps[@]}" pytest .)
    ;;
  *)
    die "unknown lane: ${lane}
${usage}"
    ;;
esac

# .env is the documented local contract; export it so require_env below sees
# the same value pytest-dotenv will.
load_dotenv
require_env BIGQUERY_PROJECT

banner "Running tests — lane: ${lane_desc}, project: ${BIGQUERY_PROJECT}"

"${cmd[@]}" ${pytest_args[@]+"${pytest_args[@]}"}

log "test.sh complete: lane ${lane:-locked}"
