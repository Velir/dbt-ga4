### tests/test_random.py
import pytest
from dbt.tests.util import read_file
#from base_unit import BaseUnitTestModel
from dbt.tests.util import run_dbt

MY_JSON_FIXTURE = """
{  "event_date": "20220527",  "event_timestamp": "1653679818347831",  "event_name": "session_start",  "event_params": [{    "key": "ga_session_id",    "value": {      "string_value": null,      "int_value": "1653679817",      "float_value": null,      "double_value": null    }  }, {    "key": "session_engaged",    "value": {      "string_value": null,      "int_value": "1",      "float_value": null,      "double_value": null    }  }, {    "key": "ga_session_number",    "value": {      "string_value": null,      "int_value": "6",      "float_value": null,      "double_value": null    }  }, {    "key": "page_location",    "value": {      "string_value": "https://www.velir.com/ideas/2021/09/22/connecting-sitecore-identity-to-saml-part-1",      "int_value": null,      "float_value": null,      "double_value": null    }  }, {    "key": "page_title",    "value": {      "string_value": "How to Connect Sitecore Identity to SAML for Single Sign-On | Velir",      "int_value": null,      "float_value": null,      "double_value": null    }  }, {    "key": "engaged_session_event",    "value": {      "string_value": null,      "int_value": "1",      "float_value": null,      "double_value": null    }  }, {    "key": "page_referrer",    "value": {      "string_value": "https://www.google.com/",      "int_value": null,      "float_value": null,      "double_value": null    }  }],  "event_previous_timestamp": null,  "event_value_in_usd": null,  "event_bundle_sequence_id": "1445270839",  "event_server_timestamp_offset": null,  "user_id": null,  "user_pseudo_id": "745846450.1644156613",  "privacy_info": {    "analytics_storage": null,    "ads_storage": null,    "uses_transient_token": "No"  },  "user_properties": [],  "user_first_touch_timestamp": "1644156613242332",  "user_ltv": {    "revenue": "0.0",    "currency": "USD"  },  "device": {    "category": "mobile",    "mobile_brand_name": "Samsung",    "mobile_model_name": "SM-M426B",    "mobile_marketing_name": "Galaxy M42 5G",    "mobile_os_hardware_model": null,    "operating_system": "Android",    "operating_system_version": "Android 12",    "vendor_id": null,    "advertising_id": null,    "language": "en-gb",    "is_limited_ad_tracking": "No",    "time_zone_offset_seconds": null,    "browser": null,    "browser_version": null,    "web_info": {      "browser": "Chrome",      "browser_version": "101.0.4951.61",      "hostname": "www.velir.com"    }  },  "geo": {    "continent": "Asia",    "country": "India",    "region": "Uttar Pradesh",    "city": "Noida",    "sub_continent": "Southern Asia",    "metro": "(not set)"  },  "app_info": null,  "traffic_source": {    "name": "(organic)",    "medium": "organic",    "source": "google"  },  "stream_id": "1966637064",  "platform": "WEB",  "event_dimensions": null,  "ecommerce": null,  "items": []}
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
        run_dbt(["run"])