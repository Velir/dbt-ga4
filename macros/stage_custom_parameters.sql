{% macro stage_custom_parameters(custom_parameters ) %}
    {% for cp in custom_parameters %}
        ,{{ unnest_key('event_params',  cp.name ,  cp.value_type ) }}
    {% endfor %}
{% endmacro %}

