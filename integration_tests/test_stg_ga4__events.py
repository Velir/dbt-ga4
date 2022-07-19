import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_base_ga4__events_csv = """user_id,user_pseudo_id,ga_session_id,stream_id,page_location
user_id_1,user_pseudo_id_1,ga_session_id_1,stream_id_1,http://www.website.com
,user_pseudo_id_2,ga_session_id_1,stream_id_1,http://www.website.com
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__events.sql')

class TestUsersFirstLastEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base_ga4__events.csv": mock_base_ga4__events_csv
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
            "actual.sql": actual,
            "expected.sql": """
            
            select
                'user_id_1' as user_id,
                'user_pseudo_id_1' as user_pseudo_id,
                'ga_session_id_1' as ga_session_id,
                'stream_id_1' as stream_id,
                md5('user_id_1') as user_key,
                md5(CONCAT('stream_id_1', CAST(TO_BASE64(md5('user_id_1')) as STRING), cast('ga_session_id_1' as STRING))) as session_key,
                1 as session_event_number,
                md5(CONCAT(CAST(TO_BASE64(md5(CONCAT('stream_id_1', CAST(TO_BASE64(md5('user_id_1')) as STRING), cast('ga_session_id_1' as STRING)))) as STRING), CAST(1 as STRING))) as event_key,
                'http://www.website.com' as original_page_location,
                'http://www.website.com' as page_location,
                'website.com' as page_hostname,
                CAST(null as STRING) as page_query_string
            UNION ALL
            select
                CAST(null as string) as user_id,
                'user_pseudo_id_2' as user_pseudo_id,
                'ga_session_id_1' as ga_session_id,
                'stream_id_1' as stream_id,
                md5('user_pseudo_id_2') as user_key,
                md5(CONCAT('stream_id_1', CAST(TO_BASE64(md5('user_pseudo_id_2')) as STRING), cast('ga_session_id_1' as STRING))) as session_key,
                1 as session_event_number,
                md5(CONCAT(CAST(TO_BASE64(md5(CONCAT('stream_id_1', CAST(TO_BASE64(md5('user_pseudo_id_2')) as STRING), cast('ga_session_id_1' as STRING)))) as STRING), CAST(1 as STRING))) as event_key,
                'http://www.website.com' as original_page_location,
                'http://www.website.com' as page_location,
                'website.com' as page_hostname,
                CAST(null as STRING) as page_query_string
            """
        }
    
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
