{% macro get_max_date_partition( target_table ) %}
    {% set sql -%}
        select 
            max(partition_id)
        from {{target.project}}.{{target.dataset}}.INFORMATION_SCHEMA.PARTITIONS
        where table_name = '{{target_table}}'
    {%- endset %}
    {{ print('Run query get_max_date_partition') }}
    {% set results = run_query(sql) %}

    {% if execute %}
        {# Return the first column #}
        {{ print('Execute get_max_date_partition') }}
        {% set results_list = results.columns[0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}
    {# Query only returns a single value so get the first value #}
    {{ return(results_list[0]) }}
{% endmacro %}
{# Source is the next upstream, partitioned model (views won't work) #}
{% macro get_updated_since_last_modified( source_project, source_dataset, source_table, max_date_partition ) %}
    {{ print('max_date_partition: ' + max_date_partition) }}
    {% set sql -%}
        select 
            -- need to extract shard identifier from table_name for sharded tables
            -- assuming that non-partitioned tables are sharded; would be nice to detect view vs sharded
            min(case when partition_id is null then right(table_name, 8) else partition_id end) as min_partition
            -- could potentially get all partitions instead of min_partition and then use a where in list(partitions) condition
        from {{source_project}}.{{source_dataset}}.INFORMATION_SCHEMA.PARTITIONS
        where table_name = '{{source_table}}'
        and last_modified_time >= timestamp(parse_date( '%Y%m%d' , cast({{max_date_partition}} as string))) 
    {%- endset %}
    {{ print('Run query get_updated_since_last_modified') }}
    {% set results = run_query(sql) %}
    {% if execute %}
        {# Return the first column #}
        {{ print('Execute get_updated_since_last_modified') }}
        {% set results_list = results.columns[0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}
    {# Need to add logic for when nothing matches #}
    {{ print('Return get_updated_since_last_modified: ' + results_list[0]) }}
    {# Query only returns a single value so get the first value #}
    {{ return(results_list[0]) }}
{% endmacro %}