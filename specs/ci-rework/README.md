# CI Rework — security-driven redesign of dbt-ga4 CI

**Status:** Spec approved, pre-implementation
**Author:** Michael Carlone
**Last updated:** 2026-08-14
**Prior art:** `dbt_artifacts/specs/ci-rework/README.md` — this spec reuses that
design's tier model, script layer, and hardening posture. Sections below call out
where dbt-ga4 **must** diverge, and there are more divergences than expected.

---

## 0. Decisions

All open questions from the draft are resolved. Recorded here so the rationale
survives the implementation.

| # | Question | Decision | Why |
|---|---|---|---|
| 1 | Version matrix mechanism | **uv-native**, no tox | Spike confirmed `uv run --isolated --no-project --with ...` resolves and collects cleanly on pinned lanes. One tool, no `tox.ini`. |
| 2 | Matrix coverage | **latest + 1.12 + 1.11 + 1.10** | 1.10 is the currently-locked version, so nothing that works today loses coverage. 1.9 is 15 months old, past dbt Labs' ~12-month window. |
| 3 | `require-dbt-version` | **Ceiling only**, `[">=1.0.0", "<2.0.0"]`, ships in 6.3.0 | Non-breaking. Guards against dbt 2.0 (already on PyPI as `2.0.0b1`). Tightening the floor to 1.10 is deferred to 7.0.0. |
| 4 | CI GCP project | **Reuse the existing sandbox** the old keyfile pointed at, switched to WIF | Confirmed by the maintainer to be a sandbox with nothing significant at risk. |
| 5 | sqlfluff | **Deferred entirely**, out of this rework | Measured: 1,488 violations, 49/69 files won't template under jinja. Not a mechanical sweep — a project. See §5.1. |
| 6 | Orphaned-dataset cleanup | **Manual script only** — no scheduled workflow, no table expiration | Housekeeping for a sandbox, unrelated to the security work. Volume is ~10–20 stray datasets/month with no cost or quota impact, so a daily workflow is not worth the trigger surface. Table expiration was rejected separately: BigQuery has no project-wide default, and per-dataset would mean overriding the test fixture. |
| 7 | Tier 4 (on-tag workflow) | **Skipped** | Package Hub indexes tags directly. Residual drift risk on hand-cut releases is accepted — see §10.1. |
| 8 | E2E DAG test / BQ emulator | **Both deferred** to their own specs | Keeps this rework scoped to security + restoring signal. The harness project still ships in Step 1 (parse-only). |
| 9 | CI auth method | **Reuse the existing `GCP_BIGQUERY_USER_KEYFILE`**; WIF deferred | Revised 2026-08-14. WIF requires creating a service account, pool and provider — IAM rights the maintainers do not hold. Blocking all test signal on an IT request was the worse trade. The key already exists in repo secrets and needs no permissions to use. WIF stays the target state; the setup runbook is kept locally, outside the repo. |
| 10 | `CODEOWNERS` | **Skipped** | Small team; the review it would force already happens. Consequence: §4's sensitive-path list is guidance for reviewers, not something GitHub enforces. Revisit if the contributor base grows. |
| 11 | Release cutter delivery | **Local script only**, no `workflow_dispatch` wrapper | A workflow that bumps and pushes needs `contents: write` — the most dangerous permission in this repo, granted to a workflow that writes to branches. Not worth it to save one local command. |

---

## 1. Background

`dbt-ga4` is an open-source dbt **package** (published to the dbt Package Hub as
`Velir/ga4`) consumed by downstream dbt projects. It has no CI today.

The single workflow that existed, `.github/workflows/run_unit_tests_on_pr.yml`,
was deleted in `49d365d` (2026-03-02) over a security issue. The deleted file:

```yaml
on: [pull_request_target, workflow_dispatch]
env:
  BIGQUERY_PROJECT: ${{ secrets.BIGQUERY_PROJECT }}
steps:
  - uses: actions/checkout@v3
    with:
      ref: ${{ github.event.pull_request.head.sha }}
  - name: Authenticate using service account
    run: 'echo "$KEYFILE" > ./unit_tests/dbt-service-account.json'
    env:
      KEYFILE: ${{ secrets.GCP_BIGQUERY_USER_KEYFILE }}
  - run: python -m pytest unit_tests
```

This is the textbook `pull_request_target` vulnerability, the same one found in
`dbt_artifacts`:

1. `pull_request_target` runs in the **base repo's** context, with secrets present
   in the runner environment.
2. The job then checks out `github.event.pull_request.head.sha` — **attacker-controlled
   code**.
3. It writes a long-lived GCP **service account JSON key** to disk and runs
   `pytest`, which imports `conftest.py` and every `unit_tests/test_*.py` from the
   PR head.

Any fork PR could have replaced a test file (or `definitions.py`, or
`pyproject.toml`) with code that read the key. No approval gate stood in the way —
`pull_request_target` fires automatically. Deleting the workflow closed the hole
but left the package with **zero CI signal**.

### 1.1. Key rotation — hygiene, not incident response

The workflow was live from `d1f8da0` until `49d365d`. The maintainer has
**confirmed the target GCP project is a sandbox and nothing significant was at
risk**, so this is cleanup rather than an incident:

**Revised 2026-08-14 (decision 9).** The original plan was to delete the key
outright, because WIF would have made it redundant. WIF turned out to need IAM
rights the maintainers do not have, so the key stays **in service** — see §7.

- [ ] **Rotate** (not delete) the `GCP_BIGQUERY_USER_KEYFILE` key. Requires IT:
      the maintainers cannot create or replace service account keys. Low
      priority — sandbox project, nothing significant at risk — but it is a real
      outstanding task, not a closed one. Raise it alongside any other IAM ask.
- [ ] Keep `BIGQUERY_PROJECT` — still needed, not sensitive.

Worth separating two things that are easy to conflate: **the vulnerability is
closed regardless.** The exposure was never that a key existed, it was
`pull_request_target` handing that key to untrusted fork code. Tier 2 runs only
post-merge on reviewed code, so the key is no longer reachable from a fork PR.
Rotation is hygiene on a credential that was *potentially* observed, not a fix
for an open hole.

**Also worth establishing:** who holds IAM admin on the CI project. Not for this
decision — for the next time a key needs rotating or a permission changing, so
it isn't rediscovered under time pressure.

---

## 2. Goals and non-goals

### Goals

- **No `pull_request_target` anywhere, ever.** Documented in `CLAUDE.md` and
  `CONTRIBUTING.md`. Not machine-enforced — see decision 10.
- **No static service account keys.** Cloud auth moves to GCP **Workload Identity
  Federation** (short-lived, OIDC-minted, repo/ref-scoped).
- **A test suite that actually runs.** It does not today — see §3.
- **Contributors get honest signal.** Fork PRs get everything checkable without
  credentials, and the docs say plainly that this is structural, not a bug.
- **Local ↔ CI parity.** Every CI step is `./scripts/ci/<something>.sh`, runnable
  identically on a laptop.
- **A repeatable release process** on the `release-candidate/X.Y.Z` convention
  already in use (`release-candidate/6.2.0` exists).

### Non-goals

- Changing where contributors PR. They continue to target `main`.
- Non-BigQuery adapter support. This package is BigQuery-only by design.
- `make`/`task` as a tooling layer. Shell scripts only.
- Branch protection as code. Configured by the repo admin.
- sqlfluff / style enforcement (§5.1).

---

## 3. Pre-existing breakage found while speccing

Two problems that have nothing to do with security but **block every tier**. Both
must land in Step 1.

### 3.1. 🚨 The test suite does not run

On the checked-in `uv.lock`, `uv run pytest .` fails at collection:

```
Defining 'pytest_plugins' in a non-top-level conftest is no longer supported:
  /unit_tests/conftest.py
```

pytest 9 (pinned via `pytest>=9.0.1`) removed support for `pytest_plugins` in a
non-root `conftest.py`. Zero tests run today.

**Verified fix:** *move* `unit_tests/conftest.py` → `conftest.py` at the repo
root. Collection then succeeds with **13 tests**. Copying is not enough — the file
must move, or the non-root copy re-triggers the error. This also explains how
`from definitions import get_test_configs` resolves: the root `conftest.py` puts
the repo root on `sys.path`, which is currently happening by accident.

Consequence: the commands in `CLAUDE.md`, `unit_tests/README.md`, and
`.github/pull_request_template.md` are all currently broken.

### 3.2. No `.env`, no `.env.example`

`unit_tests/README.md` requires a `.env` with `BIGQUERY_PROJECT`, but the repo
ships no template. A fresh clone cannot run tests even after the §3.1 fix. Add
`.env.example`.

### 3.3. 🚨 Shipped consumer-facing parse break (found by the Tier 1 parse harness)

Building the §6.1 harness surfaced a **released bug in 6.2.0**, unrelated to CI.

`models/staging/stg_ga4__users.sql` ships `config(enabled = false)`, but
`models/staging/stg_ga4__users.yml` declares a dbt `unit_tests:` entry targeting
it. dbt treats a unit test against a disabled model as a **hard parsing error**:

```
Parsing Error
  Unable to find model 'stg_ga4__users' for unit test
  'test_stg_ga4__users_audiences' in models/staging/stg_ga4__users.yml
```

Independently reproduced against a plain consumer project (no
`+enabled: true` override). **`models/staging/stg_ga4__client_keys.sql` has the
identical defect** — same disabled model, same self-targeting unit test. It is
currently masked because dbt fails on the first one.

Confirmed in-scope for consumers: the `unit_tests:` blocks arrived in `d14736b`
("User single site", #355), and `git tag --contains` puts that commit in **6.2.0**
— the current release. Any consumer on dbt ≥1.8 (when unit tests landed) fails to
parse the package unless they enable those models.

✅ **Fixed.** All four affected unit tests (two per file) now carry
`config: enabled: false`, matching the models they target.

Dropping `enabled = false` from the models was rejected: those models are
opt-in for cost reasons, and enabling them by default would silently start
building tables for every consumer — a behaviour change, not a bug fix. Gating
the tests is the minimal change that does not alter consumer-visible behaviour.

The harness re-enables them (`unit_tests: ga4: +enabled: true`), since it also
force-enables the models, so all 6 unit tests stay parsed and runnable there
rather than becoming dead code.

**Verified both directions:** a plain consumer project with default configs now
parses (previously a hard error), and the harness still parses 60 models with
6 unit tests enabled.

**Still to do:** this needs releasing as **6.2.1** — the fix is on the branch but
consumers stay broken until a tag exists. Version bumps touch `dbt_project.yml`
and the README install snippet together; `scripts/release/cut-candidate.py`
(Step 5) is what writes both.

Note the harness masks this class of bug by design — it force-enables everything
to maximise parsed surface area, so Tier 1's `dbt-parse` job would **not** have
caught it. A second parse with default configs is tracked in §12.5.

### 3.4. The lock is two minors stale

`uv.lock` resolves `dbt-core 1.10.15` / `dbt-bigquery 1.10.3`. Upstream is at
1.12. The floating "latest" lane (§7) will therefore test a combination the
package has never been verified against — which is exactly its job, but expect
the first run to be informative.

---

## 4. Threat model

Trust boundary: **code a maintainer has reviewed and merged into `main`.**

| Code location | Trusted? | Secrets reachable? |
|---|---|---|
| Fork PR branch | No | No — structurally absent under `pull_request` |
| Internal feature branch (pre-merge) | No | No |
| `main` | Yes | Yes |
| `release-candidate/**` | Yes (maintainer-created off `main`) | Yes |

The key property: under `pull_request` rather than `pull_request_target`, secrets
are **absent from the runner environment**, not merely gated behind a human
approval click. Arbitrary code execution in Tier 1 gains an attacker nothing.

### Sensitive paths

These execute in a credentialed context the moment they land on `main`.
These warrant careful review. `CODEOWNERS` was considered and skipped
(decision 10), so this list is guidance for reviewers rather than something
GitHub blocks on:

- `.github/**`
- `scripts/ci/**`, `scripts/release/**`
- `pyproject.toml`, `uv.lock`, `.python-version`
- `dbt_project.yml`, `packages.yml`, `package-lock.yml`
- `conftest.py`, `definitions.py` — imported by pytest under credentials, so
  effectively CI plumbing

---

## 5. The structural difference from dbt_artifacts

`dbt_artifacts` supports six warehouses, **three of which run in Docker**
(Postgres, Trino, SQL Server). That is what makes its Tier 1 valuable: fork PRs
run real integration tests with no secrets at all.

**dbt-ga4 has no such option.** It is BigQuery-only at a deep level — `UNNEST`,
`STRUCT`, `_TABLE_SUFFIX` wildcard scans, sharded-table sources,
`insert_overwrite` on date partitions. Every test calls
`project.adapter.upload_file(...)` into a real BigQuery dataset, then
`run_dbt(["build"])`.

Consequences:

1. **Tier 1 cannot run the test suite.** No credentials → no BigQuery → no tests.
   The alternatives are `pull_request_target` (forbidden) or an emulator
   (deferred, §12).
2. **Tier 1 is a fast structural gate, not a test gate.** It must be genuinely
   useful — see §6 — but we do not pretend it is equivalent.
3. **Tier 2 carries more weight here.** It is the *first* tier that runs any test
   at all. Branch protection on `main` (PR + approving review) is not a
   nice-to-have; it is the only thing between an untrusted diff and BigQuery
   credentials.

We accept this. The alternative — giving fork PRs warehouse credentials — is the
exact thing being removed.

### 5.1. Why sqlfluff is not part of this rework

The draft assumed Tier 1 could lint with sqlfluff's `jinja` templater, no
credentials needed. Measured against the real codebase (69 files, 2,582 lines):

```
69 files linted → 69 with violations, 1,488 total
215 templating/parse failures across 49 of 69 files
  155 × "Undefined jinja template variable: 'ga4'"
    8 × "'VarEmulator' object cannot be interpreted as an int"
    2 × undefined 'env_var'    2 × undefined 'flags'
sqlfluff fix resolves ~362 (24%); 1,126 remain
```

The blocker is structural. Every `{{ ga4.unnest_key(...) }}` call — the
namespaced-macro convention `CLAUDE.md` calls the package's public extension
point — is invisible to the jinja templater. Making it work means shimming every
package macro in `.sqlfluff` config and keeping that shim in sync forever. And
because templating fails, `--fix` cannot touch those 49 files, so this is not a
mechanical sweep.

The dbt templater would resolve `ga4.*` properly but needs the §6.1 harness for
`vars:`, and it is unverified whether dbt-bigquery's adapter initialises without
credentials (in `dbt_artifacts`, the Snowflake adapter opened a session at init —
that is what forced lint into Tier 2 there).

**Decision:** sqlfluff gets its own spec once the harness exists. It is not a
dependency of this work.

---

## 6. CI tier design

### Tier 1 — PR structural gate (untrusted, no secrets)

| | |
|---|---|
| **Trigger** | `pull_request` → `main` (any source, including forks) |
| **Permissions** | `contents: read`. No `id-token`. |
| **Secrets** | None — structurally unavailable |
| **Runtime target** | Under 3 minutes |

| Job | Command | Catches |
|---|---|---|
| `lockfile` | `uv sync --locked` | `pyproject.toml` edited without re-locking; broken resolution on a clean runner |
| `collect` | `uv run pytest . --collect-only -q` | Import errors, missing `definitions.py::TEST_FILE_PATHS` entries, tests referencing a model/macro path that no longer exists — **the highest-value offline check here**, since `get_test_configs()` raises on a missing key and `read_file()` on a missing path. It is also what would have caught §3.1. |
| `dbt-parse` | `dbt deps && dbt parse` in the harness (§6.1) | Jinja syntax errors, bad `ref()`/`source()`, malformed `.yml`, missing macro namespaces |
| `lint-yaml` | `actionlint` on `.github/workflows/**` | Workflow syntax errors; a guard rail on the security model itself |

### Tier 2 — Post-merge integration (trusted, BigQuery)

| | |
|---|---|
| **Trigger** | `push` → `main`, plus `workflow_dispatch` |
| **Permissions** | `contents: read`; `id-token: write` **job-scoped** |
| **Secrets** | `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`, `BIGQUERY_PROJECT` |
| **Jobs** | Everything in Tier 1, **plus** `./scripts/ci/test.sh` (the locked default lane) against BigQuery |
| **Concurrency** | Group by ref, `cancel-in-progress: false` — every merge deserves its own validation |

### Tier 3 — Release validation (trusted, full matrix)

| | |
|---|---|
| **Trigger** | `push` → `release-candidate/**`; `workflow_dispatch`; weekly `schedule` |
| **Permissions / secrets** | Same as Tier 2 |
| **Jobs** | Everything in Tier 2, **plus** all four version lanes (§7) |
| **max-parallel** | 4, to bound BigQuery DDL quota (§9.2) |

The weekly scheduled run executes against the default branch and catches drift
from upstream dbt-bigquery releases between package releases. In `dbt_artifacts`
this is what surfaced the `dbt-core==2.0.0a1` prerelease leak.

### Summary

| Event | Tiers | Secrets | Notes |
|---|---|---|---|
| `pull_request` → `main` | 1 | No | Fork-safe. No BigQuery. |
| `push` → `main` | 1 + 2 | Yes | First tier that runs tests |
| `push` → `release-candidate/**` | 1 + 2 + 3 | Yes | Release gate |
| `schedule` (weekly) | 3 | Yes | Upstream-drift regression on `main` |
| `workflow_dispatch` | 2 or 3 | Yes | Maintainer escape hatch |

No Tier 4 — see decision 7.

### 6.1. The harness project

Tier 1's `dbt-parse` job needs something to parse: this repo's `dbt_project.yml`
declares no `vars:`, and `CLAUDE.md` notes `dbt run` here fails without a consumer
project.

Add a minimal `integration_test_project/` (name mirrors `dbt_artifacts`) with a
`dbt_project.yml` installing this package by local path, the required
`vars: ga4:` block, and a `profiles.yml` BigQuery target. `dbt parse` **does not
connect to the warehouse**, so this stays credential-free.

Scope is **parse-only** (decision 8). It is also the future home of the E2E DAG
test (§11.2) and, if it ever happens, dbt-templater lint (§5.1).

---

## 7. Auth: keep the key, drop the CI-specific code path

**Revised 2026-08-14 — decision 9.** This section originally specified Workload
Identity Federation. WIF requires creating a service account, a pool and a
provider; the maintainers do not hold those IAM rights, and blocking the
restoration of *all* test signal behind an IT request was the worse trade. WIF
remains the target state. A step-by-step setup runbook exists but is kept
**outside version control** (it carries GCP project specifics); ask a maintainer
for it when someone with IAM access is in the loop.

### What actually changes

The old design wrote `GCP_BIGQUERY_USER_KEYFILE` to disk with
`echo "$KEYFILE" > ./unit_tests/dbt-service-account.json` and pointed
`conftest.py` at that path through a `GITHUB_ACTIONS` branch.

The credential is the same. **Two things change:**

1. **The `GITHUB_ACTIONS` branch is deleted from `conftest.py`.**
   `method: oauth` resolves through Application Default Credentials, which
   covers both environments:
   - **Locally:** `gcloud auth application-default login` (already in `CLAUDE.md`)
   - **In CI:** `google-github-actions/auth` writes a credentials file and
     exports `GOOGLE_APPLICATION_CREDENTIALS`; ADC picks it up

   ```python
   @pytest.fixture(scope="class")
   def dbt_profile_target():
       return {
           'type': 'bigquery',
           'method': 'oauth',
           'threads': 4,
           'timeout_seconds': 300,
           'project': os.environ['BIGQUERY_PROJECT'],
       }
   ```

   This is the part worth keeping regardless of auth method: no CI-specific
   logic in test code, and local and CI exercise the identical path. It is also
   what makes the eventual WIF migration a **single-step diff** — `conftest.py`
   does not care which kind of credential ADC found.

2. **`google-github-actions/auth` replaces the `echo > file.json` line.** It
   keeps the key out of shell command construction, writes the file outside the
   workspace so a later step cannot read it back as a repo file, and deletes it
   at job end (`cleanup_credentials` defaults true).

### What this does and does not fix

The vulnerability is closed either way. The exposure was never that a key
existed — it was `pull_request_target` handing that key to untrusted fork code.
Tier 2 runs post-merge on reviewed code only, so no fork PR can reach it.

What remains open is **hygiene**: a credential that was potentially observable
during the vulnerable window stays in service until IT rotates it (§1.1).
Accepted because the project is a sandbox.

### Migrating to WIF later

Create a service account, workload identity pool and provider (attribute
condition scoped to `assertion.repository == 'Velir/dbt-ga4'` **and** to
`main` / `release-candidate/**` refs), bind `roles/iam.workloadIdentityUser`,
then add `GCP_WORKLOAD_IDENTITY_PROVIDER` + `GCP_SERVICE_ACCOUNT` secrets. The
detailed runbook is held locally rather than in the repo.

In `main.yml` and `release.yml`, swap `credentials_json:` for
`workload_identity_provider:` + `service_account:` and re-add job-scoped
`id-token: write`. Roughly ten lines per workflow. No change to `conftest.py`,
the scripts, or the tests — they resolve credentials through ADC and do not care
which kind was minted.

---

## 8. Version matrix

**Mechanism: uv-native** (decision 1). Spike-verified — both a 1.9 and a 1.11 lane
resolved cleanly and collected all 13 tests:

```bash
uv run --isolated --no-project \
  --with "dbt-core==1.11.*" --with "dbt-bigquery==1.11.*" \
  --with pytest --with pytest-dotenv \
  pytest .
```

`--no-project` is **required**: without it, `pyproject.toml`'s floor conflicts
with an older pin. The consequence is that pinned lanes bypass `pyproject.toml`
entirely, so their test deps (`pytest`, `pytest-dotenv`) are named in
`scripts/ci/test.sh`. Slightly less tidy than a lockfile, but no tox and no second
config file. If `test.sh`'s dep list and `pyproject.toml`'s dev group drift, the
lanes silently diverge — keep them adjacent and commented.

**Lanes** (decision 2):

| Lane | dbt-core | dbt-bigquery | Notes |
|---|---|---|---|
| `latest` | floating `<2.0.0` | floating `<2.0.0` | Early warning for new releases. Currently resolves to 1.12. |
| `1_12_0` | `~=1.12.0` | `~=1.12.0` | Newest published |
| `1_11_0` | `~=1.11.0` | `~=1.11.0` | |
| `1_10_0` | `~=1.10.0` | `~=1.10.3` | Matches the current lock — the floor |

Tier 2 runs the locked default lane only; Tier 3 runs all four.

**Pin `dbt-core` alongside `dbt-bigquery` in every lane.** This is the exact bug
that broke the `dbt_artifacts` matrix: pinning only the adapter let `dbt-core`
float onto a prerelease.

**Add a `<2.0.0` ceiling to `pyproject.toml` in Step 1.** `dbt-core 2.0.0b1`
published on 2026-08-10. Resolvers skip prereleases by default, so nothing is
broken today — but the day 2.0.0 goes final, the current unbounded
`dbt-core>=1.10.15` pulls it in silently.

**Python:** `.python-version` pins 3.11. Pin the runner's Python explicitly via
`setup-uv` rather than inheriting the runner default.

---

## 9. Script layer — single source of truth

Workflows stay thin: `checkout → setup → invoke a script`.

```
scripts/ci/
  _lib.sh          # banner / log / die / require_env / require_cmd  (port from dbt_artifacts)
  setup.sh         # uv sync --locked
  parse.sh         # dbt deps + dbt parse in integration_test_project/
  test.sh          # USAGE: test.sh [<lane>] [-- <pytest args>]
                   #   no arg    -> uv run pytest .            (locked lane)
                   #   1_11_0    -> uv run --isolated --no-project --with ... pytest .
                   # Validates BIGQUERY_PROJECT up front; forwards extra args to pytest.
  cleanup-bq.sh    # manual: drop orphaned test datasets (§10.2). Not run by CI.
scripts/release/
  cut-candidate.py # bump version, create release-candidate/X.Y.Z, push
```

Design rules, carried over because they are what makes the prior art work:

- **No GitHub-Actions-isms.** No `${GITHUB_*}` reads, no conditional-on-CI logic.
- `set -euo pipefail`; scripts `cd` to the repo root themselves.
- Env contract validated up front via `require_env`, so a contributor sees
  `missing required env vars: BIGQUERY_PROJECT`, not a dbt stack trace.
- `trap`-based cleanup wherever anything is created.

`test.sh` is what contributors will actually use. It must work with nothing but
`.env` + `gcloud auth application-default login`.

---

## 10. BigQuery-specific operational concerns

No analogue in the `dbt_artifacts` spec — this is where most of the new thinking
went.

### 10.1. Dataset isolation — already safe, do not break it

dbt's fixture builds a per-test-class schema
`test{unix_micros}{rand:04}_{module}` (`dbt/tests/fixtures/project.py`). Unique
per class per session, so **parallel matrix lanes cannot collide** — we do not
have the shared-schema bug that bit `dbt_artifacts`. Do not "improve" this by
pinning a stable dataset name.

### 10.2. Orphaned datasets — housekeeping only

**Terminology note:** this section is about stray BigQuery *datasets*, and has
nothing to do with the credential exposure in §1. An earlier draft called these
"leaked" datasets, sitting a few sections from a genuinely leaked key, which made
routine housekeeping read as incident cleanup. It is not.

The fixture drops its schema on teardown, but a hard-killed runner (timeout,
cancelled run) skips teardown and strands the dataset.

Volume: Tier 3 creates ~52 datasets per release run (4 lanes × 13 test classes),
same again on the weekly schedule. Only the crashed/cancelled fraction strands, so
expect ~10–20 a month. Fixtures are a few rows each, BigQuery imposes no
dataset-count limit, and the target project is a sandbox — so this is **clutter,
not cost and not risk**.

`scripts/ci/cleanup-bq.sh` (decision 6) exists as a **manual maintenance tool**:
lists datasets matching `^test\d{16,}_` older than 24h, `--dry-run` by default,
explicit `--delete` required to act. Run it when the sandbox gets untidy.

**No scheduled workflow.** An earlier draft specified a daily cron; that was
over-built for tidying a sandbox and added a trigger, a permissions surface, and a
recurring destructive job for no real benefit. If stray datasets ever become a
genuine nuisance, wiring the existing script to a schedule is a ten-minute change.

Table expiration was rejected separately: BigQuery has no project-wide default,
and setting it per-dataset would mean overriding the test fixture — putting CI
plumbing back into test code, which §7 exists to remove.

### 10.3. Cost and quota

Each test class creates a dataset, uploads a JSON fixture, and runs `dbt build`.
Volumes are tiny, so **slot cost is negligible**; the real constraint is
BigQuery's per-project DDL quota, which a wide matrix multiplies. Hence
`max-parallel: 4` on Tier 3 and a single lane on Tier 2. Revisit after a month.

### 10.4. Test suite parallelism

13 tests, serial today. `pytest-xdist` is tempting since each class has its own
dataset, but it multiplies concurrent DDL. Defer; measure Tier 2 wall-clock first.

---

## 11. Implementation order

| Step | What | Secrets? |
|---|---|---|
| **0** | Revoke the old SA key, delete the secret (§1.1). | — |
| **1** | ✅ **Unbreak + foundation.** Delivered — see §11.4. | No |
| **2** | ✅ **Tier 1** `.github/workflows/pr.yml`. Delivered — see §11.5. | No |
| **3** | ✅ **Tier 2** `main.yml` + reusable `_checks.yml`. Authenticates with the existing `GCP_BIGQUERY_USER_KEYFILE` secret (decision 9), so nothing is blocked on IAM. Not yet run — first live credential use. | Yes |
| **4** | ✅ **Tier 3** `release.yml` — four lanes + weekly schedule. Delivered; lanes verified to resolve and collect. Not yet run against BigQuery. | Yes |
| **5** | ✅ `dependabot.yml` + `scripts/release/cut-candidate.py`. `CODEOWNERS` skipped (decision 10); `cleanup-bq.sh` shipped in Step 1. | No |
| **6** | `require-dbt-version` ceiling → release as **6.3.0** (decision 3). | No |
| **7** | Docs: `CONTRIBUTING.md` (new), `docs/dev-workflow.md` (new), README badges, `unit_tests/README.md` (`pip`→`uv`), PR template (`python -m pytest` → `./scripts/ci/test.sh`), `CLAUDE.md` CI-model section. | No |

Steps 1–2 touch no secrets and can merge conservatively. Step 3 is where the
trust-bearing CI lights up.

### 11.1. Release process, and the accepted risk from decision 7

Releases stay manual: cut `release-candidate/X.Y.Z` → Tier 3 goes green → tag.
With no on-tag workflow, nothing enforces that the tag, `dbt_project.yml`'s
`version:`, and the README install snippet (README:54) agree.
`scripts/release/cut-candidate.py` writes all of them together, so **it must be
documented as the only supported path** — not merely the convenient one. A
hand-cut release or hotfix can still drift.

### 11.2. Shared hardening (every workflow file)

- Top-level `permissions: contents: read`; `id-token: write` opted into at the
  **job** level only.
- Third-party actions **pinned to commit SHAs**, human-readable tag in a trailing
  comment.
- `concurrency`: cancel-in-progress on PRs, never on `main`/release.
- `timeout-minutes` on every job; `fail-fast: false` on matrices.

### 11.3. ⚠ Do NOT copy the Node 20 workaround

`dbt_artifacts` sets `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION: "true"` in all four
workflows to keep its Node 20-based pinned actions alive. **That escape hatch
stops working on 2026-09-16** — about a month from this spec's date. We pin
**Node 24-compatible releases** from day one and never introduce the env var here.

Resolved 2026-08-14. Every one of these was verified to declare `using: node24`
in its `action.yml` at the pinned tag — not assumed from the version number:

```yaml
- uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1              # v7.0.1
- uses: astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d           # v10.0.1
- uses: google-github-actions/auth@7c6bc770dae815cd3e89ee6cdf493a5fab2cc093   # v3.0.0
```

Two notes for whoever implements Step 2:

- **These majors are far ahead of `dbt_artifacts`'s pins** (checkout v4,
  setup-uv v3, auth v2). Do not copy that repo's SHAs — they are Node 20 and will
  break on 2026-09-16. Input names may also have changed across those majors;
  check each action's README rather than assuming the reference repo's `with:`
  blocks transfer.
- **`setup-uv` v10.0.1 was published 2026-08-14**, the same day this was
  resolved. If it proves flaky, drop to the previous patch rather than reaching
  for a tag — the pin must stay a SHA.

To re-resolve later: `gh api repos/<owner>/<repo>/commits/<tag> --jq .sha`, then
confirm `using:` in that tag's `action.yml`. Dependabot (§11, Step 5) will open
grouped PRs as new versions land.

---

### 11.4. Step 1 as delivered

Commits `67eb368`, `adeac55`, `128a83a`. Built by three parallel agents with
disjoint file ownership (test suite / scripts / harness).

- `unit_tests/conftest.py` → `conftest.py`, `GITHUB_ACTIONS` branch removed,
  `.env.example` added, `<2.0.0` ceilings added (resolution unchanged: 108
  packages, dbt-core 1.10.15), `package-lock.yml` tracked.
- `scripts/ci/{_lib,setup,test,parse,cleanup-bq}.sh` + README. One addition
  beyond spec: `load_dotenv` in `_lib.sh`, so the shell-level
  `require_env BIGQUERY_PROJECT` check agrees with what `pytest-dotenv` gives
  the tests — otherwise the documented local setup fails its own precondition.
- `integration_test_project/` harness. The four mandatory vars were derived from
  unguarded `var()` call sites, not guessed, and match README lines 82–85.

**Verified:** 13 tests collect; `uv lock --check` clean; all scripts pass
`bash -n` and `shellcheck`; `dbt parse` succeeds with credentials provably absent
(`CLOUDSDK_CONFIG` pointed at an empty dir), parsing 60 models.

**One interaction bug, found only after merging the three streams.** `dbt deps`
installs the package into `integration_test_project/dbt_packages/ga4/` — a full
copy of the repo including the new root `conftest.py`. pytest recursed into it,
found a `pytest_plugins` declaration below the rootdir, and aborted collection —
reintroducing the exact error Step 1 had just fixed. Neither change is wrong
alone. Fixed with `norecursedirs` in `pyproject.toml` (`128a83a`).

Worth recording as a process note: disjoint file ownership makes parallel agents
safe to run, but it structurally cannot surface defects that live in the
*interaction* between their outputs. Budget for an integration pass afterwards.

### 11.5. Step 2 as delivered

Commit `0fc5c51`. Three jobs, no secrets: `collect` (`uv sync --locked` +
`pytest --collect-only`), `parse` (`./scripts/ci/parse.sh`), and `actionlint`.

`actionlint` runs from PyPI (`actionlint-py`) through `uv` rather than a
third-party action — no extra SHA to pin, no additional supply-chain surface.

**Verified:** `actionlint` clean on the workflow itself; `act --list` enumerates
all three jobs at stage 0. Docker was unavailable locally, so the steps have
**not** executed on a runner — first real signal comes when this lands on `main`
and a PR fires it.

Note the lockfile check and collection are one job, not two: `uv sync --locked`
*is* the lockfile check, and collection needs a synced environment anyway.

## 12. Deferred — each needs its own spec

### 12.1. sqlfluff / style enforcement

See §5.1. Blocked on the harness project making the dbt templater viable, and on
triaging ~1,488 violations. Decision 5.

### 12.2. End-to-end DAG test

Coverage today is per-model unit tests over hand-written fixtures. Nothing
exercises the **full DAG** — `base_ga4__events` → `stg_ga4__events` → marts — or
the incremental `insert_overwrite` path, which is where the subtle bugs live. The
"no window functions in `base_ga4__events`" rule in `CLAUDE.md` exists precisely
because that class of bug is invisible to per-model tests.

The §6.1 harness is the natural home: seed synthetic GA4 export shards, `dbt
build` the package, assert row counts, re-run to exercise the incremental branch.

### 12.3. Multi-property / `combined_dataset` coverage

`combine_property_data()` runs as a pre-hook that **clones shards across
projects**. Entirely untested, and the hardest thing in the package to get right.
Needs a second dataset in the CI project.

### 12.4. Tighten `require-dbt-version` floor

Decision 3 ships the ceiling in 6.3.0. Raising the floor to `>=1.10.0` to match
the tested matrix is consumer-breaking and belongs in 7.0.0 with a deprecation
note.

### 12.5. Smaller items

- `unit_tests/requirements.txt` is redundant with `pyproject.toml`'s dev group —
  delete once docs stop referencing it.
- `unit_tests/test_stg_Ga4__user_id_mapping.py` has an inconsistent capital `G`.
  Works only because `definitions.py` resolves by lowercased stem.
- `unit_tests/test_stg_ga4__events.todo` / `.example` — dead files; finish or
  delete.
- `TODO.md` at the repo root should be reconciled with whatever lands here.
- `uv.lock` is two minors stale (§3.4). Bumping it is a deliberate change, not a
  drive-by.
- **Second `dbt parse` with default configs.** The harness force-enables every
  model, which is what maximises Tier 1's parsed surface area — but it also means
  Tier 1 cannot see the §3.3 class of bug, where the *default* config set is what
  breaks. A second parse invocation without the `+enabled: true` override would
  model a plain consumer install. Cheap; add it once §3.3 is fixed, so the job
  starts green.
- **`package-lock.yml` divergence.** The repo root pins `dbt_utils 1.3.2`; the
  harness's generated lock resolved `1.4.1` (same range, later resolution date).
  Align them, or the parse job validates against a different dbt_utils than the
  tests use.
- **`integration_test_project/.user.yml`** (dbt's anonymous-usage id) is written
  on every parse. Covered by the harness's own `.gitignore`; confirm it stays
  ignored if that file is ever restructured.

---

## 13. Considered and parked

- **BigQuery emulator** (`ghcr.io/goccy/bigquery-emulator`) to move real tests into
  Tier 1. This is *the* thing that would close the §5 fork-contributor gap, and
  it is more attractive here than in `dbt_artifacts` because BigQuery is our only
  target. But this package leans hard on `UNNEST` over repeated `STRUCT`s,
  wildcard `_TABLE_SUFFIX` scans, and partition-level `insert_overwrite` — exactly
  the emulator's weak spots. Timeboxed spike someday; not a dependency. Decision 8.
- **Running the suite against a consumer's real GA4 export.** Non-starter — client
  data must not enter CI.
- **Self-hosted runners inside the GCP perimeter** to avoid WIF. More
  infrastructure than the problem warrants.
