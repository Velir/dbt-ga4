import pytest

# Import the standard functional fixtures as a plugin
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'bigquery',
        'method': 'oauth',
        'threads': 1,
        'project': 'velir-website-analytics'
        # project isn't needed if you configure a default, via 'gcloud config set project'
    }