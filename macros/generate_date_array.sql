{% macro generate_date_array(start_date_str, end_date_str) %}
    {% set date_format_input = '%Y%m%d' %}
    {% set date_format_output = '%Y-%m-%d' %}
    {% set dates = [] %}
    {% set start_date = modules.datetime.datetime.strptime(start_date_str, date_format_input) %}
    {% set end_date = modules.datetime.datetime.strptime(end_date_str, date_format_input) %}
    {% set diff_days = (end_date - start_date).days %}

    {% for i in range(diff_days + 1) %}
        {% set current_date = start_date + modules.datetime.timedelta(days=i) %}
        {% set dates = dates.append(current_date.strftime(date_format_output)) %}
    {% endfor %}

    {{ return(dates) }}
{% endmacro %}