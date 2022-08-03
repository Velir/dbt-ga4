import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """event_name,event_date_dt,event_timestamp,page_location
page_view,20220623,1655992867599369,www.cnn.com
page_view,20220623,1655992982011685,www.cnn.com
page_view,20220623,1655992867599369,www.google.com
""".lstrip()

expected_csv = """page_key,page_view_count
2022062314www.cnn.com,2
2022062314www.google.com,1
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__page_conversions.sql')

class TestPageConversions():
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
        run_dbt(["build", "--vars", "conversion_events: ['page_view']"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
