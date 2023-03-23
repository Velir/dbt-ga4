{% macro stream_name( stream_id ) %}
    {% if var('ga4.stream_names', false) %}
        {% for name in var('ga4.stream_names') %}
            {% if name[0] = stream_id %} name[1] as  {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}