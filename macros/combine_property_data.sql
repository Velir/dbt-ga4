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
            {{ log("Cloned from `" ~ var('source_project') ~ ".analytics_" ~ property_id ~ ".events_*` to `" ~ target.project ~ "." ~ var('combined_dataset') ~ ".events_YYYYMMDD" ~ property_id ~ "`.", True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
