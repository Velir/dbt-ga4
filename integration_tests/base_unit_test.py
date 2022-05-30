import pytest
from dbt.tests.util import read_file

macros = read_file('../macros/url_parsing.sql')

class BaseUnitTestModel:
    def upload_json_fixture(self, project, file_name, json, table_name):
        local_file_path = file_name
        with open(local_file_path, "w") as outfile:
            outfile.write(json)
        project.adapter.upload_file(
            local_file_path = local_file_path,
            database = project.database,
            table_schema = project.test_schema,
            table_name = table_name,
            kwargs = {
                "source_format": "NEWLINE_DELIMITED_JSON",
                "autodetect":"true"
            }
        )  

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "url_parsing.sql" : macros
        }