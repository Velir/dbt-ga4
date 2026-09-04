# Contributing to dbt-ga4

Thanks for contributing. This document covers local setup, what CI will and
won't do for you, and how a change gets from a branch to a release.

## Local setup

```bash
git clone https://github.com/Velir/dbt-ga4.git
cd dbt-ga4
./scripts/ci/setup.sh                  # installs the locked dependency set via uv
```

You'll need [uv](https://docs.astral.sh/uv/). Everything Python goes through it
— **don't invoke bare `python`, `pytest`, or `dbt`**, or you'll get whatever
happens to be on your PATH instead of the versions this package is tested
against.

To run the test suite you also need BigQuery access:

```bash
cp .env.example .env                   # then set BIGQUERY_PROJECT
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```

`BIGQUERY_PROJECT` must be a project you're happy writing to — the suite creates
and drops datasets in it. Use a sandbox, not anything holding client data.

## The commands

Everything CI runs is a script in `scripts/ci/`, so anything CI does you can do
identically on your laptop. There is no GitHub-Actions-specific glue.

| Command | What it does | Needs BigQuery? |
|---|---|---|
| `./scripts/ci/setup.sh` | `uv sync --locked` | No |
| `./scripts/ci/parse.sh` | `dbt parse` the whole package via the harness project | No |
| `./scripts/ci/test.sh` | the test suite, locked dbt version | **Yes** |
| `./scripts/ci/test.sh 1_11_0` | the suite against a pinned dbt version | **Yes** |
| `./scripts/ci/test.sh -- -k some_test` | anything after `--` is passed to pytest | **Yes** |
| `./scripts/ci/cleanup-bq.sh` | drop orphaned test datasets (manual housekeeping) | **Yes** |

## What CI will and won't do on your PR

**This is the part that surprises people.** Your PR will run four checks:
dependency resolution, test collection, `dbt parse`, and workflow linting. It
will **not** run the test suite.

That's deliberate. This package is BigQuery-only, and every test builds real
BigQuery datasets — which needs credentials. PR builds have no access to
repository secrets by design, including PRs from forks. There is no way to run
these tests on an untrusted PR without handing warehouse credentials to
unreviewed code, which is exactly the vulnerability this CI setup was built to
remove.

So:

- **A green PR means "it resolves, imports, and parses."** Not "it works."
- **If your change touches SQL behaviour, run `./scripts/ci/test.sh` locally.**
  CI will not catch a broken model until after merge.
- If you don't have a BigQuery project to test against, say so in the PR
  description — a maintainer will run the suite for you.

The full suite runs after merge, and the full dbt version matrix runs before
each release.

## Adding a test

Tests use dbt's adapter-testing framework: each spins up a throwaway dbt project
against real BigQuery, loads a JSON fixture, runs the model, and compares
against expected CSV. See `unit_tests/README.md`.

The one non-obvious step: **file paths are centralised in `definitions.py`**. A
test that reads a model or macro from disk needs an entry in `TEST_FILE_PATHS`
first, keyed by the test's filename stem. `get_test_configs(__file__)` resolves
it. Convention: `actual` → `models/`, `macro_to_test` → `macros/`.

Running `./scripts/ci/test.sh -- --collect-only` catches a missing entry
immediately, without touching BigQuery.

## Conventions

- SQL follows the [Brooklyn Data style guide](https://github.com/brooklyn-data/co/blob/main/sql_style_guide.md)
  — leading commas, lowercase keywords, a CTE per step with a comment. Not
  currently machine-enforced.
- Macros use the `adapter.dispatch` + `default__` pattern so consumers can
  override them. Keep it when adding macros — it's the package's public
  extension point.
- Models call package macros with the `ga4.` prefix (`{{ ga4.unnest_key(...) }}`).
- New models under `models/staging/recommended_events/` ship with
  `config(enabled = false)` — they're opt-in for cost reasons.
- **If a model is disabled by default, any unit test targeting it must be
  disabled too.** dbt treats a unit test against a disabled model as a hard
  parsing error, which breaks `dbt parse` for every consumer of the package.
  This shipped in 6.2.0 and is worth not repeating.
- Each model should have a sibling `.yml` with column descriptions and tests.

## Releasing (maintainers)

```bash
uv run scripts/release/cut-candidate.py --minor      # or --patch / --major
```

This bumps the version in `dbt_project.yml` **and** the README install range,
commits, and pushes a `release-candidate/X.Y.Z` branch. That push triggers the
full dbt version matrix.

Use the script rather than editing versions by hand — the two locations have to
agree, nothing checks that they do, and the failure is silent: a package that
installs under a version range it doesn't claim.

When the release-candidate branch is green, tag from its HEAD:

```bash
git tag X.Y.Z && git push origin X.Y.Z
```
