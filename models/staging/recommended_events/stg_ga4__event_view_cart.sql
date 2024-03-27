{{
  config(
      enabled = false,
  )
}}
with view_cart_with_params as (
  select *,
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value', 'float_value') }}
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("view_cart_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("view_cart_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'view_cart'
)

select * from view_cart_with_params