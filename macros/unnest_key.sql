-- Unnests a single key's value from an array. Use value_type = 'lower_string_value' to produce a lowercase version of the string value

{%- macro unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}
    {{ return(adapter.dispatch('unnest_key', 'ga4')(column_to_unnest, key_to_extract, value_type, rename_column)) }}
{%- endmacro -%}

{%- macro default__unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}
    (select 
        {% if value_type == "lower_string_value" %}
            lower(value.string_value)   
        {% else %}
            value.{{value_type}}    
        {% endif %}
    from unnest({{column_to_unnest}}) where key = '{{key_to_extract}}') as 
    {% if  rename_column == "default" %}
    {{ key_to_extract }}
    {% else %}
    {{rename_column}}
    {% endif %}
{%- endmacro -%}