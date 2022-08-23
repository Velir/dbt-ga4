import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt


PARAMS_CSV = """event_key,page_query_string
aaa,param1=value1&param2=value2
bbb,param1
ccc,param1=
""".lstrip()

EXPECTED_CSV = """event_key,param,value
aaa,param1,value1
aaa,param2,value2
bbb,param1,
ccc,param1,
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__event_to_query_string_params.sql').replace(
    "ref('stg_ga4__events')",
    "ref('params')"
)



class TestEventToQueryStringParams():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "params.csv": PARAMS_CSV,
            "expected.csv": EXPECTED_CSV,
            
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual
        }
    
    def test_mock_run_and_check(self, project):
        #self.upload_json_fixture(project, "source.json", SOURCE_JSON, "SOURCE_JSON" )
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])
