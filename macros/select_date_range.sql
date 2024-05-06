{% macro select_date_range(start_date, end_date, date_column) %}
    {% if start_date is not none and end_date is not none %}
        REPLACE(CAST({{date_column}} AS STRING), "-", "") >= CAST({{ start_date }} AS STRING) and REPLACE(CAST({{date_column}} AS STRING), "-", "") <=  CAST({{ end_date }} AS STRING)
    {% else %}
        {{ date_column }} >= CURRENT_DATE - {{ var("lookback_window") }}
    {% endif %}
{% endmacro %}
