{% macro partitions_to_replace() %}
    {% set partitions_to_replace = ['current_date'] %}
    {% if var('static_incremental_days', false) %}
        {% for i in range(var('static_incremental_days')) %}
            {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro incremental_header( partition_by_field = 'event_date_dt' ) %}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": partition_by_field,
            "data_type": "date",
        },
        if var('static_incremental_days', false) partitions = {{ partitions_to_replace }}, endif
    )
}}
{% endmacro %}

{% macro incremental_and( partition_by_field = 'event_date_dt', where_start = false ) %}
    {% if is_incremental() %}
            {% if var('static_incremental_days', false ) %}
                {%- if var('static_incremental_days' = true ) -%}where{%- else -%}and{%- endif -%} event_date_dt in ({{ partitions_to_replace | join(',') }})
            {% else %}
                -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
                -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
                where event_date_dt >= _dbt_max_partition
            {% endif %}
    {% endif %}
{% endmacro %}