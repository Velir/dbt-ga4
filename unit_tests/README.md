# Unit Testing

The dbt-ga4 package treats each model and macro as a 'unit' of code. If we fix the input to each unit, we can test that we received the expected output. To do this, we use the `pytest` framework as described here:

- https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
- https://github.com/dbt-labs/dbt-core/discussions/4455#discussioncomment-2766503

You'll need to install pytest, pytest-dotenv and create a `.env` file with a `BIGQUERY_PROJECT` key containing the name of your BigQuery project. An 'oauth' connection method is assumed for local development. 

Installing pytest & pytest-dotenv can be done using the requirements.txt file. Navigate to the `unit_tests` folder and run 

```
pip install -r requirements.txt
```

To run the folder's suite of tests, navigate to the `unit_tests` folder in the command line and run:

```
python -m pytest .
```

To run a specific test:

```
python -m pytest path/to/test.py
```
