import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """user_pseudo_id,event_key,event_timestamp,geo_continent,geo_country,geo_region,geo_city,geo_sub_continent,geo_metro,device_category,device_mobile_brand_name,device_mobile_model_name,device_mobile_marketing_name,device_mobile_os_hardware_model,device_operating_system,device_operating_system_version,device_vendor_id,device_advertising_id,device_language,device_is_limited_ad_tracking,device_time_zone_offset_seconds,device_browser,device_browser_version,device_web_info_browser,device_web_info_browser_version,device_web_info_hostname,traffic_source_name,traffic_source_medium,traffic_source_source
IX+OyYJBgjwqML19GB/XIQ==,H06dLW6OhNJJ6SoEPFsSyg==,1661339279816517,Asia,India,Maharashtra,Mumbai,Southern Asia,(not set),desktop,Google,Chrome,,,Windows,Windows 10,,,en-us,No,,,,Chrome,104.0.0.0,www.velir.com,,,
IX+OyYJBgjwqML19GB/XIQ==,gt1SoAtrxDv33uDGwVeMVA==,1661339279816518,USA,Massachusetts,Maharashtra,Mumbai,Southern Asia,(not set),mobile,Google,Chrome,,,Windows,Windows 10,,,en-us,No,,,,Chrome,104.0.0.0,www.velir.com,,,
""".lstrip()

expected_csv = """user_pseudo_id,first_event,last_event,first_geo_continent,first_geo_country,first_geo_region,first_geo_city,first_geo_sub_continent,first_geo_metro,first_device_category,first_device_mobile_brand_name,first_device_mobile_model_name,first_device_mobile_marketing_name,first_device_mobile_os_hardware_model,first_device_operating_system,first_device_operating_system_version,first_device_vendor_id,first_device_advertising_id,first_device_language,first_device_is_limited_ad_tracking,first_device_time_zone_offset_seconds,first_device_browser,first_device_browser_version,first_device_web_info_browser,first_device_web_info_browser_version,first_device_web_info_hostname,first_traffic_source_name,first_traffic_source_medium,first_traffic_source_source,last_geo_continent,last_geo_country,last_geo_region,last_geo_city,last_geo_sub_continent,last_geo_metro,last_device_category,last_device_mobile_brand_name,last_device_mobile_model_name,last_device_mobile_marketing_name,last_device_mobile_os_hardware_model,last_device_operating_system,last_device_operating_system_version,last_device_vendor_id,last_device_advertising_id,last_device_language,last_device_is_limited_ad_tracking,last_device_time_zone_offset_seconds,last_device_browser,last_device_browser_version,last_device_web_info_browser,last_device_web_info_browser_version,last_device_web_info_hostname,last_traffic_source_name,last_traffic_source_medium,last_traffic_source_source
IX+OyYJBgjwqML19GB/XIQ==,H06dLW6OhNJJ6SoEPFsSyg==,gt1SoAtrxDv33uDGwVeMVA==,Asia,India,Maharashtra,Mumbai,Southern Asia,(not set),desktop,Google,Chrome,,,Windows,Windows 10,,,en-us,No,,,,Chrome,104.0.0.0,www.velir.com,,,,USA,Massachusetts,Maharashtra,Mumbai,Southern Asia,(not set),mobile,Google,Chrome,,,Windows,Windows 10,,,en-us,No,,,,Chrome,104.0.0.0,www.velir.com,,,
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__user_pseudo_id_first_last_events.sql')

class TestUsersFirstLastEvents():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "stg_ga4__events.csv": mock_stg_ga4__events_csv,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual,
        }
    
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        #breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
