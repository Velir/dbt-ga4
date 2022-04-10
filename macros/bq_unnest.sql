{%- macro bq_unnest(column_to_unnest, key_to_extract, rename_column = "default") -%}
    case
        when
            {{column_to_unnest}}.key = '{{key_to_extract}}'
        then 
            {{ merge_value_columns(
                            ['event_params.value.string_value',
                            'cast(event_params.value.int_value as string)',
                            'cast(event_params.value.float_value as string)',
                            'cast(event_params.value.double_value as string)'])}}
    end as 
    {% if  rename_column == "default" -%}
    {{ column_to_unnest }}__{{ key_to_extract }}
    {%- else -%}
    {{rename_column}}
    {%- endif -%}
{%- endmacro -%}