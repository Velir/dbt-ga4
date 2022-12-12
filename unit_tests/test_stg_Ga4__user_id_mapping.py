import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """user_pseudo_id,user_id,event_timestamp
a1,,100
a1,A,101
b1,B,102
c1,C,103
c2,C,104
c2,,105
d1,,100
""".lstrip()

expected_csv = """last_seen_user_id,user_pseudo_id,last_seen_user_id_timestamp
A,a1,101
B,b1,102
C,c1,103
C,c2,104
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__user_id_mapping.sql')

class TestUserIdMapping():
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
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
