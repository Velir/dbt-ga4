-- Takes the 4 possible event parameter value columns as an input (string, int, float, double) and returns a single value as a string

{%- macro merge_value_columns(columns_to_merge) %}
    case
        {% for curent_column in columns_to_merge -%}
        when {{ curent_column }} is not null then {{ curent_column }}
        {% endfor -%}
        else {{ columns_to_merge[0] }}
    end
{%- endmacro -%}