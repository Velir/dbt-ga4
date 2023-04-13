{%- macro create_custom_event(event_name) -%}
    {{ return(adapter.dispatch('create_custom_event', 'ga4')(event_name)) }}
{%- endmacro -%}

{%- macro default__create_custom_event(event_name) -%}
    select *
        {% if var("default_custom_parameters", "none") != "none" %}
            {{ ga4.stage_custom_parameters( var("default_custom_parameters", "none") )}}
        {% endif %}
        {% if var(event_name+"_custom_parameters", "none") != "none" %}
            {{ ga4.stage_custom_parameters( var(event_name+"_custom_parameters") )}}
        {% endif %}
    from {{ref('stg_ga4__events')}}
    where event_name = '{{event_name}}'
{%- endmacro -%}