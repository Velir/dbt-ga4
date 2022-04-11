{%- macro unpack_struct(column_to_unpack, fields) -%}
{% for field in fields %}
{{column_to_unpack}}.{{field}} as {{column_to_unpack}}_{{field}} {% if not loop.last %},{% endif %}
{% endfor %}
{%- endmacro -%}