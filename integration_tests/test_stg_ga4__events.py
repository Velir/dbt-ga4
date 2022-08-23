import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_base_ga4__events_csv = """user_id,user_pseudo_id,ga_session_id,stream_id,page_location
user_id_1,user_pseudo_id_1,ga_session_id_1,stream_id_1,http://www.website.com/?foo=bar
""".lstrip()

#TODO
#,user_pseudo_id_2,ga_session_id_1,stream_id_1,http://www.website.com

expected_csv = """user_id,user_pseudo_id,ga_session_id,stream_id,user_key,session_key,session_event_number,event_key,original_page_location,page_location,page_hostname,page_query_string
user_id_1,user_pseudo_id_1,ga_session_id_1,stream_id_1,c/nWU/GWhlWiLU0S6R/rwg==,9fDgaCrbd4ieAj1QpcWDjw==,1,FgBuqiZAGtlzSpZZgrY2VA==,http://www.website.com/?foo=bar,http://www.website.com/?foo=bar,website.com,foo=bar
"""

actual = read_file('../models/staging/ga4/stg_ga4__events.sql')

class TestUsersFirstLastEvents():
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
