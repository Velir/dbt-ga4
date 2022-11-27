import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """event_name,page_key
page_view,A
page_view,A
page_view,B
""".lstrip()

expected_csv = """page_key,page_view_count
A,2
B,1
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
