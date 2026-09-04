# Unit Testing

The dbt-ga4 package treats each model and macro as a 'unit' of code. If we fix the input to each unit, we can test that we received the expected output. To do this, we use the `pytest` framework as described here:

- https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
- https://github.com/dbt-labs/dbt-core/discussions/4455#discussioncomment-2766503

## Requirements

From the root of the repository:

```bash
./scripts/ci/setup.sh                  # uv sync --locked
cp .env.example .env                   # then set BIGQUERY_PROJECT
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```

`BIGQUERY_PROJECT` must name a project you are comfortable writing to — the
tests create and drop datasets in it. Authentication uses `method: oauth`, which
resolves through Google Application Default Credentials, so the `gcloud` login
above is what makes it work. CI uses the identical code path with credentials
supplied by the runner, which is why there is no CI-specific branch in
`conftest.py`.

> `unit_tests/requirements.txt` is left in place for now but is redundant with
> `pyproject.toml`'s dev dependency group, and `pip install -r` will not give you
> the locked versions CI uses. Prefer `./scripts/ci/setup.sh`.

## Configuration for file-based references
New tests that require file references need to be configured in the `definitions.py` file in the root of the repository.
1. Add a new key to the `TEST_FILE_PATHS` dictionary with a new dictionary value.
2. Each key of the inner dictionary should have a corresponding value that leverages the `_construct_filepaths()` function.
3. The provided filepath is relative to the project root.

Try to keep things consistent. For example a key of `macro_to_test` should reference the `macros/` folder, and `actual` should reference something in the `models/` folder.

To leverage these configurations, create a new testing file in `unit_tests/` that imports the defintions file and gets the test configurations.
```python
from definitions import get_test_configs

TEST_CONFIGS = get_test_configs(__file__)
```

This will parse based on the file name and allow you to leverage the defined dictionary of file-based references.


## Running Tests

Use `scripts/ci/test.sh`. It is the same entry point CI uses, so a green run
locally means the same thing it means in CI.

```bash
./scripts/ci/test.sh                          # whole suite, locked dbt version
./scripts/ci/test.sh -- unit_tests/test_x.py  # one file
./scripts/ci/test.sh -- -k derived_user       # anything after `--` goes to pytest
```

To reproduce a specific dbt version — the lanes CI's release matrix runs:

```bash
./scripts/ci/test.sh 1_12_0
./scripts/ci/test.sh 1_11_0
./scripts/ci/test.sh 1_10_0
```

These build real BigQuery datasets, so `BIGQUERY_PROJECT` and working
credentials are required (see Requirements above). Each test class creates and
drops its own uniquely-named dataset, so parallel runs cannot collide.

`uv run pytest .` also works and is what the script calls underneath. Prefer the
script: it validates `BIGQUERY_PROJECT` up front with a clear error rather than
failing deep inside dbt.

> **Note:** these commands were previously documented as `python -m pytest .`.
> That no longer works — pytest 9 requires `pytest_plugins` to be declared in a
> root `conftest.py`, and the suite would not even reach collection. Always
> invoke Python through `uv` in this repo so you get the locked environment.
