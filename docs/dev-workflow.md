# Development workflow

How a change travels from a branch to a released version, and which CI runs at
each stage.

## The tiers

CI is layered. Each tier runs everything the previous one runs, plus its own
additions. The dividing line is **credentials**: the boundary sits exactly where
code stops being untrusted.

| Tier | Fires on | Runs | Credentials |
|---|---|---|---|
| 1 | `pull_request` → `main` | resolution, test collection, `dbt parse`, workflow lint | **None** |
| 2 | `push` → `main` | Tier 1 + the test suite against BigQuery | Yes |
| 3 | `push` → `release-candidate/**`, weekly, manual | Tier 2 + every supported dbt version | Yes |

Tier 1 has no secrets **structurally** — not gated behind an approval button,
but absent from the runner environment, because the `pull_request` event does
not provide them. That is what makes it safe to run on a fork PR: arbitrary code
execution there gains an attacker nothing.

The consequence is that Tier 1 cannot run a single test, since every test in this
package builds real BigQuery datasets. That is a real limitation, not an
oversight, and it is why local testing matters more here than in a project whose
tests run in-memory.

## Stage 1 — feature branch

Branch off `main`, make your change, and run locally:

```bash
./scripts/ci/parse.sh    # fast, no credentials
./scripts/ci/test.sh     # the real signal, needs BigQuery
```

`parse.sh` catches Jinja errors, bad `ref()`s, and malformed `.yml`. `test.sh` is
the one that tells you whether the SQL is right.

## Stage 2 — pull request

Open a PR against `main`. Tier 1 runs.

**A green PR does not mean the tests pass — it means the package resolves,
imports, and parses.** If your change touches SQL behaviour and you haven't run
`test.sh` locally, nothing has verified it yet.

Merging requires review. That review is load-bearing: it is the only thing
standing between a diff and BigQuery credentials, because the moment a merge
lands on `main`, Tier 2 runs that code with access to a warehouse.

## Stage 3 — merge to main

Tier 2 runs: everything from Tier 1, plus the full test suite against BigQuery
on the locked dbt version.

This is the first execution of any test code for that change. If it fails, fix
forward on a new PR — `main` is not a place to leave red.

## Stage 4 — release candidate

When `main` is in a shape worth releasing:

```bash
uv run scripts/release/cut-candidate.py --minor      # or --patch / --major
```

This bumps the version in `dbt_project.yml` and the README install range
together, commits, and pushes `release-candidate/X.Y.Z`. The push fires Tier 3,
which runs the suite against every supported dbt version — the locked default
plus 1.12, 1.11 and 1.10.

Tier 3 is where version-specific breakage surfaces. It is not run per-merge
because adapter-version problems are a release concern, and running four lanes
on every merge would multiply BigQuery usage for little signal.

## Stage 5 — tag

When Tier 3 is green on the release-candidate branch, tag from its HEAD:

```bash
git tag X.Y.Z && git push origin X.Y.Z
```

dbt Package Hub indexes from tags. There is no publish workflow — tagging is the
release.

Merge the release-candidate branch back into `main` so the version bump isn't
stranded, then delete the branch.

## Hotfixes

Same path, no shortcut: branch off `main`, PR into `main`, then cut a
release-candidate with `--patch`. The tiers are what make a hotfix safe to ship
quickly; skipping them to save time defeats the point.

## The weekly run

Tier 3 also runs Mondays at 06:00 UTC against `main`. Nothing about this repo
changes between releases, but the dbt adapters it depends on do — a new
`dbt-bigquery` can break the package without anyone touching it. The weekly run
means we find out on a Monday rather than mid-release.

If it fails and nobody has merged anything, suspect upstream first.
