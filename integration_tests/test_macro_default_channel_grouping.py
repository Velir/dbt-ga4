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
""".lstrip()

expected_csv = """default_channel_grouping
Direct
Paid Search
Organic Search
Organic Video
Email
Email
(Other)
""".lstrip()

actual = """
select 
{{default_channel_grouping('source', 'medium')}} as default_channel_grouping
from {{ref('source_medium_input')}}
"""

class TestUsersFirstLastEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
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
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])