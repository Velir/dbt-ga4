{% macro combine_property_data() %}

    create schema if not exists `{{var('project')}}.{{var('dataset')}}`;

    -- Use static_incremental_days if is_incremental, otherwise pull all shards
    {% if is_incremental() %}
        {% set shards_to_retrieve = [] %}
        {% for i in range(var('static_incremental_days')) %}
            -- use modules.datetime to get current date, subtract N days, remove dashes in the date
            -- for now, starting with 2 days ago because I'm working with a client where the data appears late
            {% 
                set shards_to_retrieve = shards_to_retrieve.append(
                    (modules.datetime.date.today() - modules.datetime.timedelta(days=i+2))|string|replace("-", "")
                ) 
            %}
        {% endfor %}
        
        -- For each date and each property id, copy table to target dataset
        {% for shard in shards_to_retrieve %}
            {% for property_id in var('property_ids') %}
                CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_{{shard}}{{property_id}}` CLONE `{{var('project')}}.analytics_{{property_id}}.events_{{shard}}`;
            {% endfor %}
        {% endfor %}

    -- Else is full refresh
    {% else %}
        {% for property_id in var('property_ids') %}
            {% set schema_name = "analytics_" + property_id|string %}
            {% set relations = dbt_utils.get_relations_by_pattern(schema_name, 'events_%') %}
            {% for relation in relations %}
                {{ log(relation.identifier) }}
                {% set relation_suffix = relation.identifier|replace('events_', '') %}

                -- filter relations to those > start_date
                {% if relation_suffix|int >= var('start_date')|int %}
                    CREATE OR REPLACE TABLE `{{var('project')}}.{{var('dataset')}}.events_{{relation_suffix}}{{property_id}}` CLONE `{{var('project')}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                {% endif %}
            {% endfor %}
        {% endfor %}
    {% endif %}

{% endmacro %}
