# shellcheck shell=bash
# Shared helpers for scripts/ci/*.sh. Source, do not execute.
#
# Conventions:
#   - All scripts `set -euo pipefail`.
#   - All scripts `cd` to the repo root via `repo_root` before doing work.
#   - All scripts validate their env contract via `require_env` before running.
#   - No GitHub-Actions-isms: nothing here reads ${GITHUB_*} or branches on CI.

# Resolve the repo root from this file's location. Works regardless of the
# caller's cwd. Realpath via a portable shell idiom (no `realpath` binary).
_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${_lib_dir}/../.." && pwd)"

# Print a banner. Used to visually separate phases.
banner() {
  printf '\n================================================\n'
  printf '%s\n' "$*"
  printf '================================================\n'
}

# Print to stderr.
log() {
  printf '%s\n' "$*" >&2
}

# Abort with a message.
die() {
  log "error: $*"
  exit 1
}

# Validate that each named env var is set and non-empty.
# Usage: require_env VAR1 VAR2 VAR3
require_env() {
  local missing=()
  local var
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then
      missing+=("${var}")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    die "missing required env vars: ${missing[*]}"
  fi
}

# Require that a command is on PATH.
# Usage: require_cmd uv bq
require_cmd() {
  local missing=()
  local cmd
  for cmd in "$@"; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
      missing+=("${cmd}")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    die "missing required commands: ${missing[*]}"
  fi
}

# Load KEY=VALUE pairs from the repo-root .env if present, without clobbering
# values already exported in the environment (CI wins over a local .env).
# pytest gets the same file via pytest-dotenv; this makes shell-level checks
# such as `require_env BIGQUERY_PROJECT` agree with what the tests will see.
load_dotenv() {
  local env_file="${repo_root}/.env"
  [[ -f "${env_file}" ]] || return 0
  local line key value
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"          # ltrim
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    [[ "${line}" == export\ * ]] && line="${line#export }"
    [[ "${line}" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"             # rtrim key
    value="${value#"${value%%[![:space:]]*}"}"       # ltrim value
    value="${value%"${value##*[![:space:]]}"}"       # rtrim value
    # Strip one layer of matching quotes.
    if [[ "${value}" == \"*\" || "${value}" == \'*\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    if [[ -z "${!key:-}" ]]; then
      export "${key}=${value}"
    fi
  done < "${env_file}"
}
