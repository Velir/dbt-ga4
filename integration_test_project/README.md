# `integration_test_project`

A minimal dbt **consumer project** that exists so CI has something to parse the
`ga4` package *against*.

**This is not a test fixture.** It contains no models, no seeds and no
assertions, and it is never run against real data. The package's actual tests
live in `unit_tests/` and run against real BigQuery — see `unit_tests/README.md`.

## Why it exists

`dbt-ga4` is a package, not a project. Its own `dbt_project.yml` deliberately
declares no `vars:`, so `dbt parse` in the repo root fails standalone. The Tier 1
`dbt-parse` CI job needs a consumer project that supplies the required
`vars: ga4:` block. See `specs/ci-rework/README.md` §6 and §6.1.

`dbt parse` does **not** connect to the warehouse, so this stays credential-free:
that is what lets the check run on fork PRs where no secrets are available.

It catches Jinja syntax errors, bad `ref()`/`source()`, malformed `.yml`, and
missing `ga4.`-namespaced macro references.

## Running it

```bash
cd integration_test_project
uv run dbt deps
uv run dbt parse --profiles-dir .
```

No GCP credentials, no `.env`, no `gcloud auth` required. `profiles.yml` reads
`BIGQUERY_PROJECT` with a placeholder fallback precisely so this holds.

## How it is wired

- `packages.yml` installs the package with `local: ..`, so it always validates
  the working-tree code rather than a Package Hub release.
- The package's own `dbt_project.yml` sets `name: 'ga4'`, which is what makes the
  `{{ ga4.unnest_key(...) }}` namespaced macro calls in the models resolve.
- `models: ga4: +enabled: true` turns on the models the package ships disabled by
  default (`stg_ga4__client_keys`, `stg_ga4__users`, the user-export base models,
  and everything under `models/staging/recommended_events/`). This maximises the
  parsed surface area — and is also *required*, because `stg_ga4__users.yml`
  declares a dbt unit test against a model that ships `enabled = false`, which
  dbt rejects as a parsing error.
- `package-lock.yml` is committed so the parse job is deterministic.

## Future

This directory is the intended home of the **end-to-end DAG test** described in
`specs/ci-rework/README.md` §12.2 (seed synthetic GA4 export shards, `dbt build`
the package, assert row counts, re-run to exercise the incremental
`insert_overwrite` branch), and of dbt-templater sqlfluff linting if §12.1 ever
happens.

**Do not create a second harness project** — extend this one.
