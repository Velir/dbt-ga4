# scripts/ci/

Single source of truth for how `dbt-ga4` is set up, parsed, tested and cleaned
up.

GitHub Actions workflows in `.github/workflows/` are thin shells that
`checkout → setup → invoke a script here`. Local development invokes the
**same scripts**. There is no second implementation that "almost matches" CI.

See [`specs/ci-rework/README.md`](../../specs/ci-rework/README.md) for the design
rationale and the three-tier CI model these scripts feed into.

## Entry points

| Script | Purpose | Needs BigQuery? |
|---|---|---|
| `setup.sh` | `uv sync --locked` — install Python deps from `uv.lock`. Idempotent. | No |
| `parse.sh` | `dbt deps` + `dbt parse` in `integration_test_project/`. | No |
| `test.sh [<lane>] [-- <pytest args>]` | Run the pytest suite against BigQuery. | Yes |
| `cleanup-bq.sh [--delete]` | Drop leaked `test…` datasets older than 24h. | Yes |

`_lib.sh` is sourced, never executed: `banner`, `log`, `die`, `require_env`,
`require_cmd`, `load_dotenv`, and the `${repo_root}` resolution every script
`cd`s to.

## Prerequisites

- **`uv`** — Python toolchain (<https://docs.astral.sh/uv/>). Python 3.11, pinned
  by `.python-version`.
- **A `.env` at the repo root** containing the GCP project the tests create their
  throwaway datasets in (copy `.env.example`):

  ```
  BIGQUERY_PROJECT=your-gcp-project
  ```

  `pytest-dotenv` loads it for the tests; `_lib.sh`'s `load_dotenv` loads it for
  the shell-level checks, so `test.sh` fails with
  `missing required env vars: BIGQUERY_PROJECT` rather than a dbt stack trace.
  An already-exported `BIGQUERY_PROJECT` always wins over `.env` (that is how CI
  supplies it).
- **Application Default Credentials** — the dbt profile uses `method: oauth`, so
  locally:

  ```bash
  gcloud auth application-default login \
    --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
  ```

  In CI, `google-github-actions/auth` mints a short-lived Workload Identity
  Federation credential and exports `GOOGLE_APPLICATION_CREDENTIALS`; ADC picks
  it up. There are no static service account keys anywhere.
- **`bq`** (Google Cloud SDK) and **`python3`** — only for `cleanup-bq.sh`.

## Quick start

```bash
# One-time setup
./scripts/ci/setup.sh

# Structural gate — no credentials required (this is what fork PRs run)
./scripts/ci/parse.sh
uv run pytest . --collect-only -q

# Tests against BigQuery, locked dependency set
./scripts/ci/test.sh

# One test file
./scripts/ci/test.sh -- unit_tests/test_stg_ga4__derived_user_properties.py
```

## `test.sh` lanes

`test.sh [<lane>] [-- <pytest args>]`. With no lane it runs the locked
dependency set from `uv.lock`; any other lane runs an isolated, pinned
environment.

| Lane | Command it runs |
|---|---|
| *(none)* | `uv run pytest .` |
| `latest` | `uv run --isolated --no-project --with "dbt-core<2.0.0" --with "dbt-bigquery<2.0.0" --with pytest --with pytest-dotenv pytest .` |
| `1_12_0` | same, with `dbt-core~=1.12.0` / `dbt-bigquery~=1.12.0` |
| `1_11_0` | same, with `dbt-core~=1.11.0` / `dbt-bigquery~=1.11.0` |
| `1_10_0` | same, with `dbt-core~=1.10.0` / `dbt-bigquery~=1.10.3` (matrix floor, matches the current lock) |

Anything after `--` is forwarded to pytest verbatim, e.g.
`./scripts/ci/test.sh 1_11_0 -- -x -k sessions`.

Two things about the pinned lanes that are easy to get wrong:

- **`--no-project` is required.** Without it, `pyproject.toml`'s dependency floor
  conflicts with an older pin and resolution fails.
- The consequence is that pinned lanes **bypass `pyproject.toml` entirely**, so
  the test-only deps (`pytest`, `pytest-dotenv`) are named explicitly in
  `test.sh`. Keep that list in sync with `pyproject.toml`'s dev dependency
  group — if they drift, the matrix lanes silently stop matching the locked
  lane.
- **Every lane pins both `dbt-core` and `dbt-bigquery`.** Pinning only the
  adapter lets core float, potentially onto a prerelease. Do not "simplify" this.

Tier 2 (post-merge) runs the locked lane only; Tier 3 (release candidates,
weekly schedule) runs all four.

## `cleanup-bq.sh`

dbt's test fixture creates a dataset per test class named
`test{unix_micros}{rand:04}_{module}` and drops it on teardown. A hard-killed
runner leaks it. This script deletes the orphans.

```bash
./scripts/ci/cleanup-bq.sh                          # dry run (default), 24h gate
./scripts/ci/cleanup-bq.sh --delete                 # actually delete
./scripts/ci/cleanup-bq.sh --max-age-hours 48 --delete
./scripts/ci/cleanup-bq.sh --project some-sandbox   # override BIGQUERY_PROJECT
```

Safety properties, deliberate and worth preserving:

- **`--dry-run` is the default.** Deletion requires an explicit `--delete`.
- Only datasets matching `^test[0-9]{16,}_` are ever considered, and the pattern
  is re-checked immediately before each destructive call.
- Only datasets older than the age gate (default 24h) are eligible, so a run in
  flight is never touched.

Do **not** replace the fixture's unique dataset names with a stable one — that
per-class uniqueness is what lets matrix lanes run in parallel without
colliding.

## Conventions for editing these scripts

- `set -euo pipefail` at the top of every script; every script is `chmod +x`.
- `source _lib.sh` for shared helpers and `cd "${repo_root}"` before doing work.
  Never assume a cwd.
- Document the env contract in the header comment and validate it up front with
  `require_env` / `require_cmd`.
- **No GitHub-Actions-isms.** No `${GITHUB_*}` reads, no conditional-on-CI
  branches. These scripts behave identically on a laptop and on a runner; that
  parity is the point.
- `trap`-based cleanup wherever anything is created.
- `bash -n` every script before committing; `shellcheck` if you have it.
