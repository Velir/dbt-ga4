{% macro stage_custom_parameters(custom_parameters ) %}
    {% for cp in custom_parameters %}
        ,{{ ga4.unnest_key('event_params',  cp.name ,  cp.value_type, cp.rename_to or "default" ) }}
    {% endfor %}
{% endmacro %}

{% macro stage_custom_item_parameters(custom_item_parameters) %}
    {% for cip in custom_item_parameters %}
        ,{{ ga4.unnest_key('i.item_params',  cp.name ,  cp.value_type, cp.rename_to or "default" ) }}
    {% endfor %}
{% endmacro %}
