### tests/test_unit_test_complex_model.py

import pytest
from dbt.tests.util import read_file
from base_unit import BaseUnitTestModel

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_persons_csv = """id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
""".lstrip()

mock_source_population_persons = """
select 1 as id, 'Easton' as name, '1981-05-20T06:46:51' as some_date
union all
select 2 as id, 'Lillian' as name, '1978-09-03T18:10:33' as some_date
"""

expected_csv = """num
2
2
""".lstrip()

actual = read_file('../models/complex_model.sql')

class TestUnitTestComplexModel(BaseUnitTestModel):

    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "stg_persons.csv": mock_stg_persons_csv,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_population_persons.sql": mock_source_population_persons,
            "actual.sql": actual,
        }

    # repoint 'source()' calls to mocks (seeds or models)
    def mock_source(self):
        return {
            "population__persons": "source_population_persons",
        }

    # not necessary, since the mocked model has the same name, but here for illustration
    def mock_ref(self):
        return {
            "stg_persons": "stg_persons",
        }