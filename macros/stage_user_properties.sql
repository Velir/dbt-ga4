{% macro stage_user_properties(user_properties) %}
    {% for up in user_properties %}
        ,{{ ga4.unnest_key('user_properties', up.user_property_name, up.value_type, up.rename_to or "default") }}
    {% endfor %}
{% endmacro %}
