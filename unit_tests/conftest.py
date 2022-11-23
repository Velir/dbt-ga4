import pytest
import os

# Import the standard functional fixtures as a plugin
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    # Set project and keyfile for automated tests
    if os.environ['GITHUB_ACTIONS'] == True:
        return {
            'type': 'bigquery',
            'method': 'service-account',
            'keyfile': os.environ.get("DBT_GOOGLE_BIGQUERY_KEYFILE"),
            'threads': 4,
            'timeout_seconds': 300,
            'project':  os.environ.get("BQ_PROJECT")
        }
    return {
        'type': 'bigquery',
        'method': 'oauth',
        'threads': 4,
        'project':  os.environ.get("BQ_PROJECT")
        # project isn't needed if you configure a default, via 'gcloud config set project'
    }