# Integration Testing

These integration tests use the `pytest` framework as described here:

- https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
- https://github.com/dbt-labs/dbt-core/discussions/4455#discussioncomment-2766503

You'll need to install pytest, pytest-dotenv and create a `.env` file with a `BQ_PROJECT` key containing the name of your BigQuery project. 

Installing pytest & pytest-dotenv can be done using the requirements.txt file. Navigate to the `integration_tests` folder and run 

```
pip install -r requirements.txt
```

To run the folder's suite of tests, navigate to the `integration_tests` folder in the command line and run:

```
python -m pytest .
```

To run a specific test:

```
python -m pytest path/to/test.py
```