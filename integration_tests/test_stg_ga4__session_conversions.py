import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """session_key,event_name
AAAA,page_view
AAAA,my_conversion
AAAA,my_conversion
BBBB,my_conversion
CCCC,some_other_event
""".lstrip()

expected_csv = """session_key,my_conversion_count
AAAA,2
BBBB,1
CCCC,0
""".lstrip()

# TODO, need to set the conversion_events variable somehow
actual = read_file('../models/staging/ga4/stg_ga4__session_conversions.sql')

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
        run_dbt(["build", "--vars", "conversion_events: ['my_conversion']"])
        check_relations_equal(project.adapter, ["actual", "expected"])
