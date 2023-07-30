import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
urls_to_test_csv = """url
www.website.com/?param1=A
www.website.com/?param1=A&param2=B
www.website.com/?param1=A&param2=B&param3=C
www.website.com/
www.website.com/?
""".lstrip()

expected_csv = """param1,param2,param3
A,,
A,B,
A,B,C
,,
,,
""".lstrip()

actual = """
    select
        {{ extract_query_parameter_value( 'url' , 'param1' ) }} as param1,
        {{ extract_query_parameter_value( 'url' , 'param2' ) }} as param2,
        {{ extract_query_parameter_value( 'url' , 'param3' ) }} as param3
    from {{ref('urls_to_test')}}
"""

class TestUsersFirstLastEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "urls_to_test.csv": urls_to_test_csv,
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
            "macro_to_test.sql": read_file('../macros/url_parsing.sql'),
        }
    
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])
