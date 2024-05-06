{% macro select_date_range(start_date, end_date, date_column) %}
    {% if start_date is not none and end_date is not none %}
        date_column >= start_date and date_column <=  end_date 
    {% else %}
        date_column >= CURRENT_DATE - var("lookback_window")
    {% endif %}
{% endmacro %}