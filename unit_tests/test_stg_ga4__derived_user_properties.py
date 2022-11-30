import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

mock_stg_ga4__events_json = """
{  "user_pseudo_id": "AAA",  "event_timestamp": "1617691790431476",  "event_name": "first_visit",  "event_params": [{    "key": "my_param",    "value": {      "string_value": null,      "int_value": 1,      "float_value": null,      "double_value": null    }}]}
{  "user_pseudo_id": "AAA",  "event_timestamp": "1617691790431477",  "event_name": "first_visit",  "event_params": [{    "key": "my_param",    "value": {      "string_value": null,      "int_value": 2,      "float_value": null,      "double_value": null    }}]}
{  "user_pseudo_id": "BBB",  "event_timestamp": "1617691790431477",  "event_name": "first_visit",  "event_params": [{    "key": "my_param",    "value": {      "string_value": null,      "int_value": 1,      "float_value": null,      "double_value": null    }}]}
""".lstrip()

expected_csv = """user_pseudo_id,my_derived_property
AAA,2
BBB,1
""".lstrip()

models__config_yml = """
version: 2
sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: mock_stg_ga4__events_json
"""

class TestDerivedUserProperties():
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
            "stg_ga4__events.sql": "select * from {{source('fixture','mock_stg_ga4__events_json')}}",
            "actual.sql": read_file('../models/staging/ga4/stg_ga4__derived_user_properties.sql')
        }

    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "unnest_key.sql": read_file('../macros/unnest_key.sql'),
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
        self.upload_json_fixture(project, "source.json", mock_stg_ga4__events_json, "mock_stg_ga4__events_json" )
        run_dbt(["build", "--vars", "derived_user_properties: [{'event_parameter':'my_param','user_property_name':'my_derived_property','value_type':'int_value'}]"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
