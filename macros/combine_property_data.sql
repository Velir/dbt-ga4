{%- macro combine_property_data() -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')()) }}
{%- endmacro -%}

{% macro default__combine_property_data() %}

    create schema if not exists `{{var('project')}}.{{var('dataset')}}`;

    {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
    {% if not should_full_refresh() %}
        {% set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int %}
    {% else %}
    -- Otherwise use 'start_date' variable

        {% set earliest_shard_to_retrieve = var('start_date')|int %}
    {% endif %}

    {# 
        Multiple-project support
    
        If simple multi-property variable ([111111, 222222])
        the following will recreate the variable to match the multiple-project configuration
        [{'project': 'gcp_project_1', 'property_id': 111111},{'project': 'gcp_project_1', 'property_id': 222222}]

    #}
    {% if 'project' not in var('property_ids')[0] %}
        {% set property_dict_lst = [] %}
        {% for property_id in var('property_ids') %}
            {% set _ = property_dict_lst.append({'project': var('project'), 'property_id': property_id}) %}
        {% endfor %}
    {# Else we use the property specified by the configuration file #}
    {% else %}
        {% set property_dict_lst = var('property_ids') %}
    {% endif%}

    {# Will iterate over a list of the following format 
        [{'project': 'gcp_project_1', 'property_id': 111111},{'project': 'gcp_project_1', 'property_id': 222222}]
    #}
    {% for property_item in property_dict_lst %}
        {%- set schema_name = "analytics_" + property_item['property_id']|string -%}
        {%- if var('frequency', daily) == 'streaming' -%}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_intraday_%', database=property_item['project']) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_intraday_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_intraday_{{relation_suffix}}{{property_item['property_id']}}` CLONE `{{property_item['project']}}.analytics_{{property_item['property_id']}}.events_intraday_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}
        {%- else -%}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_%', exclude='events_intraday_%', database=property_item['project']) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_{{relation_suffix}}{{property_item['property_id']}}` CLONE `{{property_item['project']}}.analytics_{{property_item['property_id']}}.events_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}
        {%- endif -%}
    {% endfor %}
{% endmacro %}