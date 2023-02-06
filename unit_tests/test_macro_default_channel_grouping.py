import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
source_medium_input = """source,medium
(direct),(none)
google,cpc
bing,organic
vimeo,video
email,foo
foo,email
something,unknown
cn.bing.com,
43things.com,
alibaba,
alibaba,cpc
,
""".lstrip()

expected_csv = """default_channel_grouping
Direct
Paid Search
Organic Search
Organic Video
Email
Email
(Other)
Organic Search
Organic Social
Organic Shopping
Paid Shopping
Direct
""".lstrip()

actual = """
with input as (
    select * from {{ref('source_medium_input')}}
    left join {{ref('source_category_mapping')}} using (source)
)
select 
{{default_channel_grouping('source', 'medium', 'source_category')}} as default_channel_grouping
from input
"""

class TestUsersFirstLastEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source_category_mapping.csv": read_file('../seeds/ga4_source_categories.csv'),
            "source_medium_input.csv": source_medium_input,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual,
        }
    
    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macro_to_test.sql": read_file('../macros/default_channel_grouping.sql'),
        }
    
    def test_mock_run_and_check(self, project):
        #breakpoint()
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])