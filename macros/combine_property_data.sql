{%- macro combine_property_data(property_id) -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')(property_id)) }}
{%- endmacro -%}

{% macro default__combine_property_data(property_id) %}
    create schema if not exists `{{target.project}}.{{var('combined_dataset')}}`;

    {% if not should_full_refresh() %}
        {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
        {%- set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int -%}
    {% else %}
        {# Otherwise use 'start_date' variable #}
        {%- set earliest_shard_to_retrieve = var('start_date')|int -%}
    {% endif %}

    {%- set latest_shard_to_retrieve = var('end_date', modules.datetime.date.today()|string|replace("-", ""))|int -%}
    {%- set schema_name = "analytics_" + property_id|string -%}

    {# Copy intraday tables #}
    {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_intraday_%', database=var('source_project')) -%}
    {% for relation in relations %}
        {%- set relation_suffix = relation.identifier|replace('events_intraday_', '') -%}
        {%- if earliest_shard_to_retrieve|int <= relation_suffix|int <= latest_shard_to_retrieve|int -%}
            create or replace table `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}` clone `{{var('source_project')}}.analytics_{{property_id}}.events_intraday_{{relation_suffix}}`;
        {%- endif -%}
    {% endfor %}

    {# Copy daily tables and drop old intraday table #}
    {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_%', exclude='events_intraday_%', database=var('source_project')) -%}
    {% for relation in relations %}
        {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
        {%- if earliest_shard_to_retrieve|int <= relation_suffix|int <= latest_shard_to_retrieve|int -%}
            create or replace table `{{target.project}}.{{var('combined_dataset')}}.events_{{relation_suffix}}{{property_id}}` clone `{{var('source_project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
            drop table if exists `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}`;
        {%- endif -%}
    {% endfor %}
{% endmacro %}
