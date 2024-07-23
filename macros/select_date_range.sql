{% macro select_date_range(start_date, end_date, date_column, parse=true) %}
    {% if start_date is not none and end_date is not none %}
    {% if parse %}
        {{ date_column }} between 
        PARSE_DATE('%Y%m%d', '{{ start_date }}') and 
        PARSE_DATE('%Y%m%d', '{{ end_date }}')
        {% else %}
        {{ date_column }} between '{{ start_date }}' and '{{ end_date }}'
        {% endif %}
    {% else %}
        {{ date_column }} >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var("lookback_window") }} DAY )
    {% endif %}
{% endmacro %}
