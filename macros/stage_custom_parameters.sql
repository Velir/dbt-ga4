{% macro stage_custom_parameters(custom_parameters ) %}
    {% for cp in custom_parameters %}
        ,{{ ga4.unnest_key('event_params',  cp.name ,  cp.value_type, cp.rename_to or "default" ) }}
    {% endfor %}
{% endmacro %}

