{%- macro combine_specified_property_data(property_id, start_date, end_date) -%}
    {{ return(adapter.dispatch('combine_specified_property_data', 'ga4')(property_id, start_date, end_date)) }}
{%- endmacro -%}

{% macro default__combine_specified_property_data(property_id, start_date, end_date) %}
    {% if not property_id %}
        {{ exceptions.raise_compiler_error("Error: argument `property_id` is required for `combine_specified_property_data` macro.") }}
    {% endif %}

    {% if var('combined_dataset', false) == false %}
        {{ exceptions.raise_compiler_error("Error: `combined_dataset` variable is required for `combine_specified_property_data` macro.") }}
    {% endif %}

    {% if start_date %}
        {# If the 'start_date' argument exists, use it. #}
        {%- set earliest_shard_to_retrieve = start_date|int -%}
    {% elif not should_full_refresh() %}
        {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
        {%- set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int -%}
    {% else %}
        {# Otherwise use 'start_date' variable #}
        {%- set earliest_shard_to_retrieve = var('start_date')|int -%}
    {% endif %}

    {% if end_date %}
        {# If the 'end_date' argument exists, use it. #}
        {%- set latest_shard_to_retrieve = end_date|int -%}
    {% else %}
        {# Otherwise use 'end_date' variable #}
        {%- set latest_shard_to_retrieve = var('end_date', modules.datetime.date.today()|string|replace("-", ""))|int -%}
    {% endif %}

    {%- set schema_name = "analytics_" + property_id|string -%}

    {%- set combine_specified_property_data_query -%}
        create schema if not exists `{{target.project}}.{{var('combined_dataset')}}`;

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
    {%- endset -%}

    {% do run_query(combine_specified_property_data_query) %}

    {% if execute %}
        {{ log("Cloned from `" ~ var('source_project') ~ ".analytics_" ~ property_id ~ ".events_*[" ~ earliest_shard_to_retrieve ~ "-" ~ latest_shard_to_retrieve ~ "]` to `" ~ target.project ~ "." ~ var('combined_dataset') ~ ".events_YYYYMMDD" ~ property_id ~ "`.", True) }}
    {% endif %}
{% endmacro %}
