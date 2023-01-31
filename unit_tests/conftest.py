import pytest
import os

# Import the standard functional fixtures as a plugin
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    # Set project and keyfile for github automated tests
    if os.environ.get('GITHUB_ACTIONS') is not None:
        return {
            'type': 'bigquery',
            'method': 'service-account',
            'keyfile': os.environ.get("GITHUB_WORKSPACE") + "/unit_tests/dbt-service-account.json",
            'threads': 4,
            'timeout_seconds': 300,
            'project':  os.environ.get("BIGQUERY_PROJECT")
        }
    return {
        'type': 'bigquery',
        'method': 'oauth',
        'threads': 4,
        'project':  os.environ.get("BIGQUERY_PROJECT")
    }

@pytest.fixture(scope="class")
def project_config_update():
    return {'name': 'ga4'}