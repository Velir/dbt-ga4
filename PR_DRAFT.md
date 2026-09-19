# PR title

Prevent clone backfill analysis from breaking project-wide compilation

# PR body

## Description & motivation

`analyses/clone_backfill.sql`, introduced in 6.3.0, is compiled as part of project-wide commands such as `dbt compile` and `dbt docs generate`. Previously, the analysis always required `clone_property_id`, `clone_start_date`, and `source_project`, and it always invoked `ga4.get_source_tables_to_clone(...)`. As a result, consumers that were not using the analysis failed on missing vars, while placeholder vars could still trigger a BigQuery metadata query during compilation. Package analyses cannot be disabled independently from the consuming project without also disabling the package's models.

This change makes the analysis a no-op when `clone_property_id` is not supplied. The compiled output explains that the analysis must be compiled directly with its documented vars. It also guards source-table discovery with `execute`, so parse-only phases cannot query BigQuery. The existing usage instructions, variable documentation, and generated backfill SQL remain unchanged when the required vars are supplied.

## Validation

- Updated the consumer reproduction project's `packages.yml` to reference this checkout as a local package.
- Confirmed a project-wide compile with no clone vars exits successfully and compiles `clone_backfill.sql` to the no-op guidance comment instead of raising `Required var 'clone_property_id' not found`.
- Compiled `clone_backfill` with the documented clone vars against a connection-free BigQuery test double and confirmed the expected script header, destination schema statement, and no-tables warning are generated.
- Rendered the pre-change and post-change analysis with representative intraday and daily source tables and confirmed that all nonblank generated SQL lines are unchanged.
- Could not run `./scripts/ci/test.sh` or validate the live BigQuery table-discovery/query path, including the dummy-vars case, because no BigQuery credentials are available in this environment. A maintainer with BigQuery access should verify that path.
- The normal `dbt deps` registry download could not be completed in this environment because outbound access to Package Hub is unavailable; cached dependencies were used for the compile checks.

## Checklist

- [x] I have verified that these changes work locally
- [ ] I have updated the `README.md` (not applicable)
- [ ] I have added tests & descriptions to my models (and macros if applicable; not applicable)
- [ ] I have run `./scripts/ci/test.sh` and `./scripts/ci/parse.sh` (BigQuery credentials are unavailable)
