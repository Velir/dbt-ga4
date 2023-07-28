{%- macro combine_property_data() -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')()) }}
{%- endmacro -%}

{% macro default__combine_property_data() %}

    create schema if not exists `{{var('project')}}.{{var('dataset')}}`;

    {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
    {% if not should_full_refresh() %}
        {% set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int %}
    {% else %}
    {# Otherwise use 'start_date' variable #}

        {% set earliest_shard_to_retrieve = var('start_date')|int %}
    {% endif %}

    {% for property_id in var('property_ids') %}
        {%- set schema_name = "analytics_" + property_id|string -%}
            {# Copy intraday tables #}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_intraday_%', database=var('project')) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_intraday_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}` CLONE `{{var('project')}}.analytics_{{property_id}}.events_intraday_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}
            {# Copy daily tables and drop old intraday table #}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_%', exclude='events_intraday_%', database=var('project')) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_{{relation_suffix}}{{property_id}}` CLONE `{{var('project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                    DROP TABLE IF EXISTS `{{var('project')}}.{{var('dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}`;
                {%- endif -%}
            {% endfor %}
    {% endfor %}
{% endmacro %}