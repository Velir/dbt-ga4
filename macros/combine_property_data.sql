{%- macro combine_property_data() -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')()) }}
{%- endmacro -%}

{% macro default__combine_property_data() %}

    create schema if not exists `{{target.project}}.{{var('combined_dataset')}}`;

    {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
    {% if not should_full_refresh() %}
        {% set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int %}
    {% else %}
    {# Otherwise use 'start_date' variable #}

        {% set earliest_shard_to_retrieve = var('start_date')|int %}
    {% endif %}
    {% for property_id in var('property_ids') %}
        {%- set schema_name = "analytics_" + property_id|string -%}
            {% if this == 'base_ga4__events' %}
                {# Copy intraday tables #}
                {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_intraday_%', database=var('source_project')) -%}
                {% for relation in relations %}
                    {%- set relation_suffix = relation.identifier|replace('events_intraday_', '') -%}
                    {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                        CREATE OR REPLACE TABLE `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}` CLONE `{{var('source_project')}}.analytics_{{property_id}}.events_intraday_{{relation_suffix}}`;
                    {%- endif -%}
                {% endfor %}
                {# Copy daily tables and drop old intraday table #}
                {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_%', exclude='events_intraday_%', database=var('source_project')) -%}
                {% for relation in relations %}
                    {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                    {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                        CREATE OR REPLACE TABLE `{{target.project}}.{{var('combined_dataset')}}.events_{{relation_suffix}}{{property_id}}` CLONE `{{var('source_project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                        DROP TABLE IF EXISTS `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}`;
                    {%- endif -%}
                {% endfor %}
            {% elif this == 'base_ga4__pseudonymous_users' %}
                {# Copy pseudonymous_users tables #}
                {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='pseudonymous_users_%', database=var('source_project')) -%}
                {% for relation in relations %}
                    {%- set relation_suffix = relation.identifier|replace('pseudonymous_users_', '') -%}
                    {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                        CREATE OR REPLACE TABLE `{{target.project}}.{{var('combined_dataset')}}.pseudonymous_users_{{relation_suffix}}{{property_id}}` CLONE `{{var('source_project')}}.analytics_{{property_id}}.pseudonymous_users_{{relation_suffix}}`;
                    {%- endif -%}
                {% endfor %}
            {% elif this == 'base_ga4__users' %}
                {# Copy users tables #}
                {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='users_%', exclude='pseudonymous_users_%', database=var('source_project')) -%}
                {% for relation in relations %}
                    {%- set relation_suffix = relation.identifier|replace('users_', '') -%}
                    {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                        CREATE OR REPLACE TABLE `{{target.project}}.{{var('combined_dataset')}}.users_{{relation_suffix}}{{property_id}}` CLONE `{{var('source_project')}}.analytics_{{property_id}}.users_{{relation_suffix}}`;
                    {%- endif -%}
                {% endfor %}
            {% endif %}
    {% endfor %}
{% endmacro %}