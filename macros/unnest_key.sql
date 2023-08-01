-- Unnests a single key's value from an array. Use value_type = 'lower_string_value' to produce a lowercase version of the string value

{%- macro unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}
    {{ return(adapter.dispatch('unnest_key', 'ga4')(column_to_unnest, key_to_extract, value_type, rename_column)) }}
{%- endmacro -%}

{%- macro default__unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}
    (select 
        {% if value_type == "lower_string_value" %}
            lower(value.string_value)   
        {% elif value_type == "multiple_values_to_string" %}
            COALESCE(CAST(value.int_value AS STRING), CAST(value.float_value AS STRING), CAST(value.double_value AS STRING), value.string_value) 
        {% elif value_type == "multiple_number_values" %}
            COALESCE(value.int_value , value.float_value, value.double_value) 
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