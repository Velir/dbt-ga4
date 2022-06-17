import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
urls_to_test_csv = """url
www.website.com/?param_to_exclude=1234
www.website.com/?param_to_exclude=
www.website.com/?foo=bar&param_to_exclude=1234
www.website.com/?foo=bar&param_to_exclude=1234&another=parameter
www.website.com/?foo=bar&param_to_exclude=1234&another=parameter&exclude=nope
""".lstrip()

expected_csv = """url
www.website.com/
www.website.com/
www.website.com/?foo=bar
www.website.com/?foo=bar&another=parameter
www.website.com/?foo=bar&another=parameter&exclude=nope
""".lstrip()

actual = """
select 
{{remove_query_parameters('url', ['param_to_exclude'])}} as url
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
