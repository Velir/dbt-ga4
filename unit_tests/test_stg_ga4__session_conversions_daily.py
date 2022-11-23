import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """session_key,session_partition_key,event_name,event_date_dt
A,A2022-01-01,page_view,2022-01-01
A,A2022-01-01,my_conversion,2022-01-01
A,A2022-01-01,my_conversion,2022-01-01
B,B2022-01-01,my_conversion,2022-01-01
C,C2022-01-01,some_other_event,2022-01-01
A,A2022-01-02,my_conversion,2022-01-02
""".lstrip()

expected_csv = """session_key,session_partition_key,session_partition_date,my_conversion_count
A,A2022-01-01,2022-01-01,2
B,B2022-01-01,2022-01-01,1
C,C2022-01-01,2022-01-01,0
A,A2022-01-02,2022-01-02,1
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__session_conversions_daily.sql')

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
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
