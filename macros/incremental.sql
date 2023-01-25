{% macro incremental_header( partition_by_field = 'event_date_dt' ) %}
    {% set partitions_to_replace = ['current_date'] %}
    {% if var('static_incremental_days', false) %}
        {% for i in range(var('static_incremental_days')) %}
            {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
        {% endfor %}
    {% endif %}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": partition_by_field,
            "data_type": "date",
        },
        {% if var('static_incremental_days', false) %} partitions = partitions_to_replace, {% endif %}
    )
}}
{% endmacro %}