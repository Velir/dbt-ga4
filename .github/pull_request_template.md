## Description & motivation
<!---
Describe your changes, and why you're making them.
-->

## Checklist
- [ ] I have verified that these changes work locally
- [ ] I have updated the `README.md` (if applicable)
- [ ] I have added tests & descriptions to my models (and macros if applicable)
- [ ] I have run `./scripts/ci/test.sh` and `./scripts/ci/parse.sh`

<!---
On CI, if you're wondering why your PR shows fewer checks than you expected:

PR CI runs structural checks only — dependency resolution, test collection,
`dbt parse`, and workflow linting. It does NOT run the test suite.

That is deliberate, not a gap. Every test in this package builds real BigQuery
datasets, which needs credentials, and PR builds have no access to secrets by
design — including PRs from forks. The tests run after merge instead.

So if your change touches SQL behaviour, run `./scripts/ci/test.sh` locally
before opening the PR. CI will not catch it for you at this stage. You need a
BigQuery project and `gcloud` credentials — see `unit_tests/README.md`.

If you don't have a BigQuery project to test against, say so in the PR
description and a maintainer will run the suite for you.
-->
