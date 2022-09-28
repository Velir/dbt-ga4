{% macro session_event_count_metrics( event_name_array ) %}
    {% for en in event_name_array %}
        countif( event_name = '{{en}}' ) as count_{{en}},
    {% endfor %}
{% endmacro %}