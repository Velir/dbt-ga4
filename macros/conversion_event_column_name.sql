{% macro conversion_event_column_name(event_name, prefix, suffix) %}
    {% if var('conversion_event_column_renamers', false) and var('conversion_event_column_renamers')[event_name] %}
        {{prefix}}{{var('conversion_event_column_renamers')[event_name]}}{{suffix}}
    {% else %}
        {{prefix}}{{event_name}}{{suffix}}
    {% endif %}
{% endmacro %}
