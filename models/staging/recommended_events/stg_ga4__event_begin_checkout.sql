{{
  config(
      enabled = false,
  )
}}
 with begin_checkout_with_params as (
   select *
   , {{ ga4.unnest_key('event_params', 'currency') }}
   , {{ ga4.unnest_key('event_params', 'value', 'double_value') }}
   , {{ ga4.unnest_key('event_params', 'coupon') }}
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("begin_checkout_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("begin_checkout_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'begin_checkout'
)

select * from begin_checkout_with_params