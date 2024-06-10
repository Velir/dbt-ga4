import pytest
from dbt.tests.util import check_relations_equal, read_file, run_dbt

# Define mocks via CSV (seeds) or SQL (models)
mock_stg_ga4__events_csv = """event_name,page_key
page_view,A
page_view,A
page_view,B
""".lstrip()

mock_stg_ga4__nonstandard_events_csv = """event_name,page_key
page-view,A
page-view,A
page-view,B
""".lstrip()

expected_csv = """page_key,page_view_count
A,2
B,1
""".lstrip()

actual = read_file("../models/staging/stg_ga4__page_conversions.sql")


class TestPageConversions:
    # Update project name to ga4 so we can call macros with ga4.macro_name
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "ga4"}

    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "valid_column_name.sql": read_file("../macros/valid_column_name.sql"),
        }

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
        run_dbt(["build", "--vars", "conversion_events: ['page_view']"])
        # breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])


class TestPageConversionsNonStandardEventName:
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "stg_ga4__events.csv": mock_stg_ga4__nonstandard_events_csv,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "valid_column_name.sql": read_file("../macros/valid_column_name.sql"),
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual,
        }

    def test_mock_run_and_check(self, project):
        run_dbt(["build", "--vars", "conversion_events: ['page-view']"])
        # breakpoint()
        check_relations_equal(project.adapter, ["actual", "expected"])
