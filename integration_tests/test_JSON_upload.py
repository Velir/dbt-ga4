### tests/test_random.py
import pytest
from dbt.tests.util import read_file
#from base_unit import BaseUnitTestModel
from dbt.tests.util import run_dbt

MY_JSON_FIXTURE = """
{'dict_key': 'dict_value'}
{'dict_key': 'dict_value2'}
""".lstrip()

models__config_yml = """
version: 2

sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: some_json

models:
  - name: my_model
    columns:
      - name: dict_key
        tests:
          - unique
          - not_null
"""

models__my_model_sql = """
select * from {{ source('fixture', 'some_json') }}
"""

class TestUploadJson:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": models__my_model_sql,
            "config.yml": models__config_yml
        }

    def upload_json_fixture(self, project):
        local_file_path = "my_fixture.json"
        with open(local_file_path, "w") as outfile:
            outfile.write(MY_JSON_FIXTURE)
        project.adapter.upload_file(
            local_file_path = local_file_path,
            database = project.database,
            table_schema = project.test_schema,
            table_name = "some_json",
            kwargs = {
                "source_format": "NEWLINE_DELIMITED_JSON",
                #"schema": json.dumps(TABLE_SCHEMA),
                "autodetect":"true"
            }
        )

    def test_my_nonsense(self, project):
        self.upload_json_fixture(project)
        run_dbt(["build"])