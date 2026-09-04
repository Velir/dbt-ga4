import os

import pytest

# Import the standard functional fixtures as a plugin.
# NOTE: pytest requires `pytest_plugins` to live in the *root* conftest.py.
pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    # `method: oauth` resolves through Google Application Default Credentials, which
    # works both locally (`gcloud auth application-default login`) and in CI, where
    # Workload Identity Federation exports GOOGLE_APPLICATION_CREDENTIALS.
    try:
        bigquery_project = os.environ['BIGQUERY_PROJECT']
    except KeyError:
        raise RuntimeError(
            "BIGQUERY_PROJECT is not set. Copy .env.example to .env and set it to the "
            "GCP project the tests should run against."
        ) from None

    return {
        'type': 'bigquery',
        'method': 'oauth',
        'threads': 4,
        'timeout_seconds': 300,
        'project': bigquery_project,
    }


@pytest.fixture(scope="class")
def project_config_update():
    return {
            'name': 'ga4'
            , 'vars':{'static_incremental_days':3}
            }
