import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt
from definitions import get_test_configs

TEST_CONFIGS = get_test_configs(__file__)

mock_events_json = """
{  "client_key": "AAA",  "user_properties": [{    "key": "my_str_prop",    "value": {      "string_value": "value_a",      "int_value": null,      "float_value": null,      "double_value": null,      "set_timestamp_micros": null    }},  {    "key": "my_int_prop",    "value": {      "string_value": null,      "int_value": 10,      "float_value": null,      "double_value": null,      "set_timestamp_micros": null    }}]}
{  "client_key": "BBB",  "user_properties": [{    "key": "my_str_prop",    "value": {      "string_value": "value_b",      "int_value": null,      "float_value": null,      "double_value": null,      "set_timestamp_micros": null    }},  {    "key": "my_int_prop",    "value": {      "string_value": null,      "int_value": 42,      "float_value": null,      "double_value": null,      "set_timestamp_micros": null    }}]}
""".lstrip()

expected_csv = """client_key,my_str_prop,my_int_prop
AAA,value_a,10
BBB,value_b,42
""".lstrip()

mock_events_rename_json = """
{  "client_key": "AAA",  "user_properties": [{    "key": "original_prop",    "value": {      "string_value": "hello",      "int_value": null,      "float_value": null,      "double_value": null,      "set_timestamp_micros": null    }}]}
{  "client_key": "BBB",  "user_properties": [{    "key": "original_prop",    "value": {      "string_value": "world",      "int_value": null,      "float_value": null,      "double_value": null,      "set_timestamp_micros": null    }}]}
""".lstrip()

expected_rename_csv = """client_key,renamed_prop
AAA,hello
BBB,world
""".lstrip()

models__config_yml = """
version: 2
sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: mock_events_json
"""

models__rename_config_yml = """
version: 2
sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: mock_events_rename_json
"""

actual_sql = """
select
    client_key
    {{ ga4.stage_user_properties(var('default_user_properties')) }}
from {{ source('fixture', 'mock_events_json') }}
"""

actual_rename_sql = """
select
    client_key
    {{ ga4.stage_user_properties(var('default_user_properties')) }}
from {{ source('fixture', 'mock_events_rename_json') }}
"""

class TestStageUserProperties():
    # Update project name to ga4 so we can call macros with ga4.macro_name
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ga4"
        }

    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "config.yml": models__config_yml,
            "actual.sql": actual_sql,
        }

    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "stage_user_properties.sql": read_file(TEST_CONFIGS.get("stage_user_properties")),
            "unnest_key.sql": read_file(TEST_CONFIGS.get("unnest_key")),
        }

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

    def test_mock_run_and_check(self, project):
        self.upload_json_fixture(project, "source.json", mock_events_json, "mock_events_json")
        run_dbt(["build", "--vars", "default_user_properties: [{'user_property_name':'my_str_prop','value_type':'string_value'},{'user_property_name':'my_int_prop','value_type':'int_value'}]"])
        check_relations_equal(project.adapter, ["actual", "expected"])


class TestStageUserPropertiesRenameTo():
    # Update project name to ga4 so we can call macros with ga4.macro_name
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ga4"
        }

    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": expected_rename_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "config.yml": models__rename_config_yml,
            "actual.sql": actual_rename_sql,
        }

    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "stage_user_properties.sql": read_file(TEST_CONFIGS.get("stage_user_properties")),
            "unnest_key.sql": read_file(TEST_CONFIGS.get("unnest_key")),
        }

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

    def test_mock_run_and_check(self, project):
        self.upload_json_fixture(project, "source.json", mock_events_rename_json, "mock_events_rename_json")
        run_dbt(["build", "--vars", "default_user_properties: [{'user_property_name':'original_prop','value_type':'string_value','rename_to':'renamed_prop'}]"])
        check_relations_equal(project.adapter, ["actual", "expected"])
