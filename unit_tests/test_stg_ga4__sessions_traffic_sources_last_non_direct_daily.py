import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__sessions_traffic_sources_daily_csv = """client_key,session_partition_key,session_partition_date,session_partition_timestamp,session_source,session_medium,session_source_category,session_campaign,session_content,session_term,session_default_channel_grouping,non_direct_session_partition_key
A,A,20230505,1683321359,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a,A
A,B,20230506,1683407759,(direct),,,,,,,
A,C,20230507,1683494159,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a,C
A,D,20230508,1683580559,(direct),,,,,,,
""".lstrip()

expected_csv = """client_key,session_partition_key,session_partition_date,session_source,session_medium,session_source_category,session_campaign,session_content,session_term,session_default_channel_grouping,session_partition_key_last_non_direct,last_non_direct_source,last_non_direct_medium,last_non_direct_source_category,last_non_direct_campaign,last_non_direct_content,last_non_direct_term,last_non_direct_default_channel_grouping
A,A,20230505,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a,A,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a
A,B,20230506,(direct),,,,,,,A,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a
A,C,20230507,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a,C,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a
A,D,20230508,(direct),,,,,,,C,source_a,medium_a,source_category_a,campaign_a,content_a,term_a,default_channel_grouping_a
""".lstrip()

actual = read_file('../models/staging/stg_ga4__sessions_traffic_sources_last_non_direct_daily.sql')

class TestSessionsTrafficSourcesLastNonDirectDaily():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "stg_ga4__sessions_traffic_sources_daily.csv": mock_stg_ga4__sessions_traffic_sources_daily_csv,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            # Hack-y solution to ensure the model is not partitioned. Loading mock data (date columns) from a seed file + partitioning don't work well together. 
            "actual.sql": actual.replace("materialized = 'incremental',","materialized = 'view',"),
        }
    
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])
