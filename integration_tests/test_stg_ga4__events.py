import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_base_ga4__events_csv = """user_id,event_name,event_timestamp,user_pseudo_id,ga_session_id,stream_id,page_location,page_referrer,source,medium,campaign
user_id_1,pageview,12345,user_pseudo_id_1,ga_session_id_1,stream_id_1,http://www.website.com/?foo=bar,http://www.cnn.com/,google,organic,(organic)
""".lstrip()

expected_csv = """user_id,event_name,event_timestamp,user_pseudo_id,ga_session_id,stream_id,source,user_key,session_key,event_key,medium,campaign,original_page_location,original_page_referrer,page_location,page_referrer,page_hostname,page_query_string
user_id_1,pageview,12345,user_pseudo_id_1,ga_session_id_1,stream_id_1,google,c/nWU/GWhlWiLU0S6R/rwg==,oofQgRkJyisBugwfj6eKVA==,HtZ29M483yK2N0WRkRZo0A==,organic,(organic),http://www.website.com/?foo=bar,http://www.cnn.com/,http://www.website.com/?foo=bar,http://www.cnn.com/,website.com,foo=bar
"""

actual = read_file('../models/staging/ga4/stg_ga4__events.sql')

class TestStgEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base_ga4__events.csv": mock_base_ga4__events_csv,
            "expected.csv": expected_csv
        }

    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": read_file('../macros/url_parsing.sql'),
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual
        }
    
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
