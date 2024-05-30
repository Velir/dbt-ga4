{% macro select_date_range(start_date, end_date, date_column) %}
    {% if start_date is not none and end_date is not none %}
        CAST({{ date_column }} AS DATE) BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    {% else %}
        CAST({{ date_column }} AS DATE) >= CURRENT_DATE() - INTERVAL {{ var("lookback_window") }} DAY
    {% endif %}
{% endmacro %}