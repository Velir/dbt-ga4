-- Unnests a single key's value from an array

{%- macro unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}

(select value.{{value_type}} from unnest({{column_to_unnest}}) where key = '{{key_to_extract}}') as 

    {% if  rename_column == "default" -%}
    {{ key_to_extract }}
    {%- else -%}
    {{rename_column}}
    {%- endif -%}

{%- endmacro -%}