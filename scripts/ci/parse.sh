#!/usr/bin/env bash
# Install package dependencies and parse the dbt DAG in the harness project.
#
# `dbt parse` compiles every model, macro and .yml in this package without
# connecting to the warehouse, so this runs with NO credentials — that is the
# whole point: it is the structural gate fork PRs get.
#
# It catches Jinja syntax errors, bad ref()/source(), malformed .yml and
# missing macro namespaces.
#
# Required env vars: none. No BigQuery credentials needed.
# Required commands:  uv
#
# Usage: scripts/ci/parse.sh [-- <extra dbt parse args>]

set -euo pipefail
# shellcheck source=scripts/ci/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

cd "${repo_root}"

require_cmd uv

project_dir="integration_test_project"

if [[ ! -f "${repo_root}/${project_dir}/dbt_project.yml" ]]; then
  die "harness project not found: ${project_dir}/dbt_project.yml
This script parses the package through the minimal consumer project in
${project_dir}/ (see specs/ci-rework/README.md §6.1). The package's own
dbt_project.yml declares no vars: and cannot be parsed on its own."
fi

extra_args=()
if (( $# > 0 )); then
  [[ "$1" == "--" ]] || die "usage: scripts/ci/parse.sh [-- <extra dbt parse args>]"
  shift
  extra_args=("$@")
fi

banner "dbt deps (${project_dir})"
uv run dbt deps \
  --project-dir "${project_dir}" \
  --profiles-dir "${project_dir}"

banner "dbt parse (${project_dir})"
uv run dbt parse \
  --project-dir "${project_dir}" \
  --profiles-dir "${project_dir}" \
  ${extra_args[@]+"${extra_args[@]}"}

log "parse.sh complete"
