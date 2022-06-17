import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """url
1111
1111
""".lstrip()

expected_csv = """client_id,first_event,last_event,first_geo,first_device,first_traffic_source,last_geo,last_device,last_traffic_source
1111,event_key_client_1_0,event_key_client_1_1,AL,Computer,Internet,MO,Phone,Dial-Up
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__users_first_last_events.sql')

class TestUsersFirstLastEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "stg_ga4__events.csv": mock_stg_ga4__events_csv,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual,
        }
    
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])
