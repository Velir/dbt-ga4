{# ============================================================
   Clone Helper Macros
   ============================================================ #}

{%- macro get_source_tables_to_clone(property_id, earliest_shard, latest_shard, source_project) -%}
    {{ return(adapter.dispatch('get_source_tables_to_clone', 'ga4')(property_id, earliest_shard, latest_shard, source_project)) }}
{%- endmacro -%}

{% macro default__get_source_tables_to_clone(property_id, earliest_shard, latest_shard, source_project) %}
{#
    Discovers GA4 event tables for cloning within a date range.

    Args:
        property_id: GA4 property ID
        earliest_shard: Start date as YYYYMMDD integer
        latest_shard: End date as YYYYMMDD integer
        source_project: GCP project containing source data

    Returns:
        List of dicts, ordered intraday first then daily:
        [
            {'type': 'intraday', 'date_shard': '20240101', 'source_table': 'events_intraday_20240101'},
            {'type': 'daily', 'date_shard': '20240102', 'source_table': 'events_20240102'},
        ]
#}
    {%- set tables = [] -%}
    {%- set schema_name = "analytics_" ~ property_id|string -%}

    {# Discover intraday tables #}
    {%- set intraday_relations = dbt_utils.get_relations_by_pattern(
        schema_pattern=schema_name,
        table_pattern='events_intraday_%',
        database=source_project
    ) -%}
    {% for relation in intraday_relations %}
        {%- set date_shard = relation.identifier|replace('events_intraday_', '') -%}
        {%- if date_shard|int >= earliest_shard|int and date_shard|int <= latest_shard|int -%}
            {%- do tables.append({
                'type': 'intraday',
                'date_shard': date_shard,
                'source_table': relation.identifier
            }) -%}
        {%- endif -%}
    {% endfor %}

    {# Discover daily tables #}
    {%- set daily_relations = dbt_utils.get_relations_by_pattern(
        schema_pattern=schema_name,
        table_pattern='events_%',
        exclude='events_intraday_%',
        database=source_project
    ) -%}
    {% for relation in daily_relations %}
        {%- set date_shard = relation.identifier|replace('events_', '') -%}
        {%- if date_shard|int >= earliest_shard|int and date_shard|int <= latest_shard|int -%}
            {%- do tables.append({
                'type': 'daily',
                'date_shard': date_shard,
                'source_table': relation.identifier
            }) -%}
        {%- endif -%}
    {% endfor %}

    {{ return(tables) }}
{% endmacro %}

{%- macro generate_clone_statement(source_project, source_schema, source_table, dest_project, dest_schema, dest_table) -%}
    {{ return(adapter.dispatch('generate_clone_statement', 'ga4')(source_project, source_schema, source_table, dest_project, dest_schema, dest_table)) }}
{%- endmacro -%}

{% macro default__generate_clone_statement(source_project, source_schema, source_table, dest_project, dest_schema, dest_table) -%}
CREATE OR REPLACE TABLE `{{ dest_project }}.{{ dest_schema }}.{{ dest_table }}` CLONE `{{ source_project }}.{{ source_schema }}.{{ source_table }}`;
{%- endmacro %}

{%- macro generate_drop_statement(project, schema, table) -%}
    {{ return(adapter.dispatch('generate_drop_statement', 'ga4')(project, schema, table)) }}
{%- endmacro -%}

{% macro default__generate_drop_statement(project, schema, table) -%}
DROP TABLE IF EXISTS `{{ project }}.{{ schema }}.{{ table }}`;
{%- endmacro %}

{%- macro generate_create_schema_statement(project, schema) -%}
    {{ return(adapter.dispatch('generate_create_schema_statement', 'ga4')(project, schema)) }}
{%- endmacro -%}

{% macro default__generate_create_schema_statement(project, schema) -%}
CREATE SCHEMA IF NOT EXISTS `{{ project }}.{{ schema }}`;
{%- endmacro %}

{# ============================================================
   Main Macro: combine_property_data
   ============================================================ #}

{%- macro combine_property_data() -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')()) }}
{%- endmacro -%}

{# ============================================================
   Run-Operation: clone_backfill
   ============================================================ #}

{%- macro clone_backfill(clone_property_id, clone_start_date, clone_end_date=none) -%}
    {{ return(adapter.dispatch('clone_backfill', 'ga4')(clone_property_id, clone_start_date, clone_end_date)) }}
{%- endmacro -%}

{% macro default__clone_backfill(clone_property_id, clone_start_date, clone_end_date) %}
{#
    Clones GA4 event tables one at a time via run_query(), avoiding the timeout
    that occurs when batching all clones into a single query.

    Each clone is an individual run_query() call with natural round-trip latency,
    so no explicit delay is needed between operations.

    Usage:
        dbt run-operation clone_backfill --args '{
            clone_property_id: 123456789,
            clone_start_date: 20240101,
            clone_end_date: 20241231
        }'

    Args:
        clone_property_id (required): GA4 property ID
        clone_start_date (required): YYYYMMDD integer
        clone_end_date (optional): YYYYMMDD integer, defaults to today
#}
    {%- set earliest_shard = clone_start_date|int -%}
    {%- set latest_shard = (clone_end_date if clone_end_date else modules.datetime.date.today()|string|replace("-", "")|int)|int -%}

    {%- set tables = ga4.get_source_tables_to_clone(clone_property_id, earliest_shard, latest_shard, var('source_project')) -%}
    {%- set source_schema = "analytics_" ~ clone_property_id|string -%}
    {%- set total = tables | length -%}

    {% if execute %}
        {{ log("", True) }}
        {{ log("============================================================", True) }}
        {{ log("GA4 Clone Backfill", True) }}
        {{ log("============================================================", True) }}
        {{ log("Property:    " ~ clone_property_id, True) }}
        {{ log("Date Range:  " ~ earliest_shard ~ " to " ~ latest_shard, True) }}
        {{ log("Tables:      " ~ total, True) }}
        {{ log("============================================================", True) }}
        {{ log("", True) }}
    {% endif %}

    {% if total == 0 %}
        {{ log("WARNING: No tables found for property " ~ clone_property_id ~ " in date range " ~ earliest_shard ~ " to " ~ latest_shard, True) }}
        {{ log("Please verify:", True) }}
        {{ log("  1. The clone_property_id is correct", True) }}
        {{ log("  2. The source_project variable is set correctly (current: " ~ var('source_project') ~ ")", True) }}
        {{ log("  3. Tables exist in the specified date range", True) }}
    {% else %}
        {# Create destination schema #}
        {%- set create_schema_sql = ga4.generate_create_schema_statement(target.project, var('combined_dataset')) -%}
        {% do run_query(create_schema_sql) %}

        {% for table in tables %}
            {%- set dest_table_suffix = table.date_shard ~ clone_property_id -%}

            {% if table.type == 'intraday' %}
                {%- set clone_sql = ga4.generate_clone_statement(
                    var('source_project'), source_schema, table.source_table,
                    target.project, var('combined_dataset'), 'events_intraday_' ~ dest_table_suffix
                ) -%}
                {% do run_query(clone_sql) %}
            {% elif table.type == 'daily' %}
                {%- set clone_sql = ga4.generate_clone_statement(
                    var('source_project'), source_schema, table.source_table,
                    target.project, var('combined_dataset'), 'events_' ~ dest_table_suffix
                ) -%}
                {% do run_query(clone_sql) %}

                {# Drop corresponding intraday clone #}
                {%- set drop_sql = ga4.generate_drop_statement(
                    target.project, var('combined_dataset'), 'events_intraday_' ~ dest_table_suffix
                ) -%}
                {% do run_query(drop_sql) %}
            {% endif %}

            {% if execute %}
                {{ log("[" ~ loop.index ~ "/" ~ total ~ "] Cloned " ~ table.type ~ ": " ~ table.date_shard, True) }}
            {% endif %}
        {% endfor %}

        {% if execute %}
            {{ log("", True) }}
            {{ log("Clone backfill complete. Processed " ~ total ~ " tables for property " ~ clone_property_id ~ ".", True) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro default__combine_property_data() %}
    {% if not should_full_refresh() %}
        {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
        {%- set earliest_shard = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int -%}
    {% else %}
        {# Otherwise use 'start_date' variable #}
        {%- set earliest_shard = var('start_date')|int -%}
    {% endif %}
    {# For the pre-hook, latest_shard is always today #}
    {%- set latest_shard = modules.datetime.date.today()|string|replace("-", "")|int -%}

    {% for property_id in var('property_ids') %}
        {%- set tables = ga4.get_source_tables_to_clone(property_id, earliest_shard, latest_shard, var('source_project')) -%}
        {%- set source_schema = "analytics_" ~ property_id|string -%}

        {%- set combine_specified_property_data_query -%}
            {{ ga4.generate_create_schema_statement(target.project, var('combined_dataset')) }}

            {% for table in tables %}
                {%- set dest_table_suffix = table.date_shard ~ property_id -%}
                {% if table.type == 'intraday' %}
                    {{ ga4.generate_clone_statement(
                        var('source_project'), source_schema, table.source_table,
                        target.project, var('combined_dataset'), 'events_intraday_' ~ dest_table_suffix
                    ) }}
                {% elif table.type == 'daily' %}
                    {{ ga4.generate_clone_statement(
                        var('source_project'), source_schema, table.source_table,
                        target.project, var('combined_dataset'), 'events_' ~ dest_table_suffix
                    ) }}
                    {{ ga4.generate_drop_statement(
                        target.project, var('combined_dataset'), 'events_intraday_' ~ dest_table_suffix
                    ) }}
                {% endif %}
            {% endfor %}
        {%- endset -%}

        {% do run_query(combine_specified_property_data_query) %}

        {% if execute %}
            {# Calculate counts and date range for logging #}
            {%- set daily_count = tables | selectattr('type', 'equalto', 'daily') | list | length -%}
            {%- set intraday_count = tables | selectattr('type', 'equalto', 'intraday') | list | length -%}
            {%- set date_shards = tables | map(attribute='date_shard') | list | sort -%}
            {%- set min_date = date_shards | first if date_shards else 'none' -%}
            {%- set max_date = date_shards | last if date_shards else 'none' -%}
            {{ log("Cloned " ~ daily_count ~ " daily + " ~ intraday_count ~ " intraday tables (" ~ min_date ~ " to " ~ max_date ~ ") from `" ~ var('source_project') ~ ".analytics_" ~ property_id ~ "` to `" ~ target.project ~ "." ~ var('combined_dataset') ~ "`", True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
