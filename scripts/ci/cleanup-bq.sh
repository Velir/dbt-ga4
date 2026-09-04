#!/usr/bin/env bash
# Delete leaked dbt test datasets from the BigQuery test project.
#
# dbt's adapter-test fixture creates a per-test-class dataset named
# test{unix_micros}{rand:04}_{module} and drops it on teardown. A hard-killed
# runner (timeout, cancelled run, ^C) leaks it. This script finds those
# orphans and removes them.
#
# SAFETY — this touches a real GCP project, so it is deliberately conservative:
#   * --dry-run is the DEFAULT. Nothing is deleted without --delete.
#   * Only datasets matching ^test[0-9]{16,}_ are ever considered. Anything
#     else in the project is invisible to this script.
#   * Only datasets older than the age gate (default 24h) are considered, so a
#     run in flight right now is never touched.
#
# Required env vars:
#   BIGQUERY_PROJECT   project to clean (read from the repo-root .env if unset;
#                      overridable with --project).
# Required commands:  bq  (Google Cloud SDK), python3
#
# Credentials: the bq CLI uses gcloud / Application Default Credentials, the
# same auth the test suite uses.
#
# Usage:
#   scripts/ci/cleanup-bq.sh                        # dry run, 24h gate
#   scripts/ci/cleanup-bq.sh --delete               # actually delete
#   scripts/ci/cleanup-bq.sh --max-age-hours 48 --delete
#   scripts/ci/cleanup-bq.sh --project my-sandbox

set -euo pipefail
# shellcheck source=scripts/ci/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

cd "${repo_root}"

require_cmd bq python3

# Datasets dbt's fixture creates. Anything not matching this is never touched.
dataset_pattern='^test[0-9]{16,}_'

usage="usage: scripts/ci/cleanup-bq.sh [--delete] [--dry-run] [--max-age-hours N] [--project ID]"

do_delete=0
max_age_hours=24
project="${BIGQUERY_PROJECT:-}"

while (( $# > 0 )); do
  case "$1" in
    --delete)
      do_delete=1
      shift
      ;;
    --dry-run)
      do_delete=0
      shift
      ;;
    --max-age-hours)
      [[ -n "${2:-}" ]] || die "--max-age-hours requires a value"
      max_age_hours="$2"
      shift 2
      ;;
    --project)
      [[ -n "${2:-}" ]] || die "--project requires a value"
      project="$2"
      shift 2
      ;;
    -h|--help)
      printf '%s\n' "${usage}"
      exit 0
      ;;
    *)
      die "unknown argument: $1
${usage}"
      ;;
  esac
done

[[ "${max_age_hours}" =~ ^[0-9]+$ ]] || die "--max-age-hours must be an integer: ${max_age_hours}"

if [[ -z "${project}" ]]; then
  load_dotenv
  project="${BIGQUERY_PROJECT:-}"
fi
[[ -n "${project}" ]] || die "missing required env vars: BIGQUERY_PROJECT (or pass --project)"

mode="DRY RUN (nothing will be deleted; pass --delete to act)"
(( do_delete )) && mode="DELETE"

banner "cleanup-bq — project: ${project}, older than: ${max_age_hours}h, mode: ${mode}"

# List every dataset in the project, one id per line.
all_datasets="$(bq --project_id="${project}" ls --datasets --max_results=10000 --format=json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin) or []; print("\n".join(x["datasetReference"]["datasetId"] for x in d))')"

now_epoch="$(date +%s)"
cutoff_epoch=$(( now_epoch - max_age_hours * 3600 ))

candidates=()
while IFS= read -r dataset; do
  [[ -n "${dataset}" ]] || continue
  # Hard guard: only fixture-generated names are ever eligible.
  [[ "${dataset}" =~ ${dataset_pattern} ]] || continue

  created_ms="$(bq --project_id="${project}" show --format=json "${project}:${dataset}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("creationTime","0"))')"
  created_epoch=$(( created_ms / 1000 ))
  age_hours=$(( (now_epoch - created_epoch) / 3600 ))

  if (( created_epoch > cutoff_epoch )); then
    log "skip   ${dataset} (age ${age_hours}h < ${max_age_hours}h)"
    continue
  fi

  log "match  ${dataset} (age ${age_hours}h)"
  candidates+=("${dataset}")
done <<< "${all_datasets}"

if (( ${#candidates[@]} == 0 )); then
  log "no leaked datasets to clean up"
  exit 0
fi

banner "${#candidates[@]} dataset(s) eligible for deletion"
printf '%s\n' "${candidates[@]}"

if (( ! do_delete )); then
  log ""
  log "dry run: nothing deleted. Re-run with --delete to remove the ${#candidates[@]} dataset(s) above."
  exit 0
fi

failed=0
for dataset in "${candidates[@]}"; do
  # Belt and braces: re-check the pattern immediately before the destructive call.
  [[ "${dataset}" =~ ${dataset_pattern} ]] || die "refusing to delete non-matching dataset: ${dataset}"
  log "deleting ${project}:${dataset}"
  if ! bq --project_id="${project}" rm -r -f -d "${project}:${dataset}"; then
    log "warning: failed to delete ${dataset}"
    failed=$(( failed + 1 ))
  fi
done

(( failed == 0 )) || die "${failed} dataset(s) failed to delete"

log "cleanup-bq.sh complete: deleted ${#candidates[@]} dataset(s)"
