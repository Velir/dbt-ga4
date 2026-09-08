import pytest
from dbt.tests.util import read_file, check_relations_equal, run_dbt
from definitions import get_test_configs

TEST_CONFIGS = get_test_configs(__file__)

# =============================================================================
# Test 1: generate_clone_statement
#
# Tests that the macro produces correct BigQuery CLONE syntax for:
# - Daily event tables
# - Intraday event tables
# - Multiple property IDs
# =============================================================================

expected_clone_csv = """clone_sql
CREATE OR REPLACE TABLE `my-target-project.combined_data.events_20240101123456789` CLONE `my-source-project.analytics_123456789.events_20240101`;
CREATE OR REPLACE TABLE `my-target-project.combined_data.events_intraday_20240102123456789` CLONE `my-source-project.analytics_123456789.events_intraday_20240102`;
CREATE OR REPLACE TABLE `my-target-project.combined_data.events_20240103987654321` CLONE `my-source-project.analytics_987654321.events_20240103`;
""".lstrip()

actual_clone_sql = """
select '{{ ga4.generate_clone_statement("my-source-project", "analytics_123456789", "events_20240101", "my-target-project", "combined_data", "events_20240101123456789") }}' as clone_sql
union all
select '{{ ga4.generate_clone_statement("my-source-project", "analytics_123456789", "events_intraday_20240102", "my-target-project", "combined_data", "events_intraday_20240102123456789") }}'
union all
select '{{ ga4.generate_clone_statement("my-source-project", "analytics_987654321", "events_20240103", "my-target-project", "combined_data", "events_20240103987654321") }}'
"""


class TestGenerateCloneStatement:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_clone.csv": expected_clone_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual_clone.sql": actual_clone_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "combine_property_data.sql": read_file(TEST_CONFIGS.get("macro_to_test")),
        }

    def test_generate_clone_statement(self, project):
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual_clone", "expected_clone"])


# =============================================================================
# Test 2: generate_drop_statement
#
# Tests that the macro produces correct BigQuery DROP TABLE IF EXISTS syntax.
# This is used to clean up intraday tables after daily tables are cloned.
# =============================================================================

expected_drop_csv = """drop_sql
DROP TABLE IF EXISTS `my-target-project.combined_data.events_intraday_20240101123456789`;
DROP TABLE IF EXISTS `my-target-project.combined_data.events_intraday_20240102987654321`;
""".lstrip()

actual_drop_sql = """
select '{{ ga4.generate_drop_statement("my-target-project", "combined_data", "events_intraday_20240101123456789") }}' as drop_sql
union all
select '{{ ga4.generate_drop_statement("my-target-project", "combined_data", "events_intraday_20240102987654321") }}'
"""


class TestGenerateDropStatement:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_drop.csv": expected_drop_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual_drop.sql": actual_drop_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "combine_property_data.sql": read_file(TEST_CONFIGS.get("macro_to_test")),
        }

    def test_generate_drop_statement(self, project):
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual_drop", "expected_drop"])


# =============================================================================
# Test 3: generate_create_schema_statement
#
# Tests that the macro produces correct BigQuery CREATE SCHEMA IF NOT EXISTS
# syntax for the combined dataset.
# =============================================================================

expected_schema_csv = """create_schema_sql
CREATE SCHEMA IF NOT EXISTS `my-target-project.combined_data`;
CREATE SCHEMA IF NOT EXISTS `another-project.ga4_combined`;
""".lstrip()

actual_schema_sql = """
select '{{ ga4.generate_create_schema_statement("my-target-project", "combined_data") }}' as create_schema_sql
union all
select '{{ ga4.generate_create_schema_statement("another-project", "ga4_combined") }}'
"""


class TestGenerateCreateSchemaStatement:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_schema.csv": expected_schema_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual_schema.sql": actual_schema_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "combine_property_data.sql": read_file(TEST_CONFIGS.get("macro_to_test")),
        }

    def test_generate_create_schema_statement(self, project):
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual_schema", "expected_schema"])
