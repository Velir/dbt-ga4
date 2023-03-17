{{
  config(
      enabled = false,
  )
}}

 with add_payment_info_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'coupon') }},
      {{ ga4.unnest_key('event_params', 'currency') }},
      {{ ga4.unnest_key('event_params', 'payment_type') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("add_payment_info_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("add_payment_info_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'add_payment_info'
)

select * from add_payment_info_with_params