# Unit Testing

The dbt-ga4 package treats each model and macro as a 'unit' of code. If we fix the input to each unit, we can test that we received the expected output. To do this, we use the `pytest` framework as described here:

- https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
- https://github.com/dbt-labs/dbt-core/discussions/4455#discussioncomment-2766503

## Requirements
You'll need to install pytest, pytest-dotenv and create a `.env` file with a `BIGQUERY_PROJECT` key containing the name of your BigQuery project. An 'oauth' connection method is assumed for local development.

Installing pytest & pytest-dotenv can be done using the `unit_tests/requirements.txt` file or by using `uv` at the root of the repository.
```bash
pip install -r unit_tests/requirements.txt
```
or
```bash
uv sync
```

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
To run the folder's suite of tests, simply run at the root of the repository:
```bash
python -m pytest .
```

To run a specific test:
```bash
python -m pytest path/to/test.py
```
