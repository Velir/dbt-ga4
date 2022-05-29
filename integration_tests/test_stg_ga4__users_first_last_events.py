import pytest
from dbt.tests.util import read_file
from base_unit import BaseUnitTestModel

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """client_id,event_key,event_timestamp,geo,device,traffic_source
1111,event_key_client_1_0,1981-05-20T06:46:40,AL,Computer,Internet
1111,event_key_client_1_1,1981-05-20T06:46:50,MO,Phone,Dial-Up
""".lstrip()

expected_csv = """client_id,first_event,last_event,first_geo,first_device,first_traffic_source,last_geo,last_device,last_traffic_source
1111,event_key_client_1_0,event_key_client_1_1,AL,Computer,Internet,MO,Phone,Dial-Up
""".lstrip()

actual = read_file('../models/staging/ga4/stg_ga4__users_first_last_events.sql')

class TestUnitTestComplexModel(BaseUnitTestModel):

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
            #"source_population_persons.sql": mock_source_population_persons,
            "actual.sql": actual,
        }

    # repoint 'source()' calls to mocks (seeds or models)
    #def mock_source(self):
    #    return {
    #        "population__persons": "source_population_persons",
    #    }

    # not necessary, since the mocked model has the same name, but here for illustration
    def mock_ref(self):
        return {
            "stg_ga4__events": "stg_ga4__events",
        }