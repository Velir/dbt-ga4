import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
# 1662178591054679 is 2022-09-03
# 1662091373338190 is 2022-09-02
mock_stg_ga4__events_csv = """user_pseudo_id,user_id,event_timestamp
a1,,100
a1,A,101
b1,B,102
c1,C,103
c2,C,104
c2,,105
""".lstrip()

expected_csv = """user_pseudo_id,last_seen_timestamp,last_seen_user_id
a1,101,A
b1,102,B
c1,103,C
c2,105,C
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
        #run_dbt(["build", "--vars", "conversion_events: ['page_view']"])
        run_dbt(["seed", "-m", "stg_ga4__events"])
        run_dbt(["seed", "-m", "expected"])
        run_dbt(["run"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])

        # TODO, update mocked events... somehow. Then run again and compare to a new 'expected' table. 