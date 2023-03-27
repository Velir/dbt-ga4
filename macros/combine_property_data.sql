{% macro combine_property_data() %}

    create schema if not exists `{{var('project')}}.{{var('dataset')}}`;

    -- If is_incremental, then use static_incremental_days variable to find earliest shard to copy
    {% if not should_full_refresh() %}
        {% set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "") %}
        
        {%- for property_id in var('property_ids') -%}
            {%- set schema_name = "analytics_" + property_id|string -%}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_name, 'events_%') -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_{{relation_suffix}}{{property_id}}` CLONE `{{var('project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}
        {%- endfor -%}
    -- Else is full refresh. Filter to only source tables with a suffix > start_date
    {% else %}
        {%- for property_id in var('property_ids') -%}
            {%- set schema_name = "analytics_" + property_id|string -%}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_name, table_pattern='events_%', exclude='events_intraday_%') -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                {%- if relation_suffix|int >= var('start_date')|int -%}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_{{relation_suffix}}{{property_id}}` CLONE `{{var('project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}
        {%- endfor -%}
    {% endif %}

{% endmacro %}
