import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

mock_base_ga4__events_json = """
{"event_date": "20230206", "event_timestamp": "1675704514198480", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704510, "float_value": null,"double_value": null } }], "stream_id":"000" }
{"event_date": "20230206", "event_timestamp": "1675704514438900", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704510, "float_value": null,"double_value": null } }], "stream_id":"000" }
{"event_date": "20230206", "event_timestamp": "1675704514198990", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704980, "float_value": null,"double_value": null } }], "stream_id":"000" }
""".lstrip()

mock_multi_site_1_base_ga4__events_json = """
{"event_date": "20230206", "event_timestamp": "1675704514198481", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704511, "float_value": null,"double_value": null } }], "stream_id":"100" }
{"event_date": "20230206", "event_timestamp": "1675704514438901", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704511, "float_value": null,"double_value": null } }], "stream_id":"100" }
{"event_date": "20230206", "event_timestamp": "1675704514198991", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704981, "float_value": null,"double_value": null } }], "stream_id":"100" }
""".lstrip()

mock_multi_site_2_base_ga4__events_json = """
{"event_date": "20230206", "event_timestamp": "1675704514198482", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704512, "float_value": null,"double_value": null } }], "stream_id":"200" }
{"event_date": "20230206", "event_timestamp": "1675704514438902", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704512, "float_value": null,"double_value": null } }], "stream_id":"200" }
{"event_date": "20230206", "event_timestamp": "1675704514198992", "event_name": "page_view", "event_params": [{ "key": "ga_session_id", "value": { "string_value": null, "int_value" : 1675704982, "float_value": null,"double_value": null } }], "stream_id":"200" }
""".lstrip()

single_site_expected_csv = """event_date,event_timestamp,event_name,ga_session_id,stream_id
20230206,1675704514198480,page_view,1675704510,000
20230206,1675704514438900,page_view,1675704510,000
20230206,1675704514198990,page_view,1675704980,000
""".lstrip()


multi_site_expected_csv = """event_date,event_timestamp,event_name,ga_session_id,stream_id
20230206,1675704514198481,page_view,1675704511,100
20230206,1675704514438901,page_view,1675704511,100
20230206,1675704514198991,page_view,1675704981,100
20230206,1675704514198482,page_view,1675704512,200
20230206,1675704514438902,page_view,1675704512,200
20230206,1675704514198992,page_view,1675704982,200
""".lstrip()

models__config_yml = """
version: 2
sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: mock_base_ga4__events_json
sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: mock_multi_site_1_base_ga4__events_json
sources:
  - name: fixture
    schema: "{{ target.schema }}"
    tables:
      - name: mock_multi_site_2_base_ga4__events_json
"""

class TestBaseGa4SingleSite():
    # Update project name to ga4 so we can call macros with ga4.macro_name
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ga4",
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "config.yml": models__config_yml,
        }

    # repoint 'source()' calls to mocks (seeds or models)
    def mock_source(self):
        return {
            "analytics_000000000": "mock_base_ga4__events_json",
        }
    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "base_select.sql": read_file('../macros/base_select.sql'),
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
        self.upload_json_fixture(project, "source.json", mock_base_ga4__events_json, "mock_base_ga4__events_json" )
        run_dbt(["build", "--vars", "dataset: 'analytics_000000000'"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "single_site_expected_csv"])
