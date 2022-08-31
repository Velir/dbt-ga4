{%- macro mart_custom_parameters(custom_parameters , prefix="" ) -%} 
    {%- for cp in custom_parameters -%}
        {%- if (not cp.aggregation) or (cp.aggregation == 'group') -%}
            ,{{ cp.name }} as {{ prefix }}{{ cp.name }} 
        {%- elif (cp.aggregation == 'count_distinct' )  -%}
            ,count(distinct {{cp.name}} ) as {{ prefix }}count_distinct_{{cp.name}}
        {%- else -%}
            ,{{ cp.aggregation}}({{cp.name}} ) as {{ prefix }}{{ cp.aggregation}}_{{cp.name}}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}
{%- macro mart_group_by_custom_parameters(custom_parameters, prefix="" ) -%}
    {%- for cp in custom_parameters -%}
        {%- if cp.aggregation == 'group' -%}
            ,{{ prefix }}{{ cp.name }}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}