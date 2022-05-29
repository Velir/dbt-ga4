# Pulled from https://github.com/dbt-labs/dbt-core/discussions/4455

import pytest
from dbt.tests.util import run_dbt, check_relations_equal

# This is pretty tricky Jinja, but the idea is just to "override" ref/source by repointing
# to the mocked seeds/models defined in the test case. The mapping is handled by
# 'mock_ref()' and 'mock_source()' methods defined on the test case

mock_ref_source = """
{{% macro ref(ref_name) %}}
    {{% set mock_ref = {} %}}
    {{% set mock_name = mock_ref.get('ref_name', ref_name) %}}
    {{% do return(builtins.ref(mock_name)) %}}
{{% endmacro %}}

{{% macro source(source_name, table_name) %}}
    {{% set lookup_name = source_name ~ '__' ~ table_name %}}
    {{% set mock_src = {} %}}
    {{% set mock_name = mock_src[lookup_name] %}}
    {{% do return(builtins.ref(mock_name)) %}}
{{% endmacro %}}
"""

# this isn't a test itself, it's just the "base case" for actual tests to inherit
class BaseUnitTestModel:
    def actual(self):
        return "actual"
    
    def expected(self):
        return "expected"
    
    def mock_ref(self):
        return {}

    def mock_source(self):
        return {}
    
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "overrides.sql": mock_ref_source.format(str(self.mock_ref()), str(self.mock_source()))
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_mock_run_and_check(self, project):
        run_dbt(["build"])
        # this runs a pretty fancy query to validate: same columns, same types, same row values
        check_relations_equal(project.adapter, [self.actual(), self.expected()])