  {% if var('ecommerce', false ) ==  false %}
     {{
        config(
            enabled = false,
        )
    }}
 {% endif %}
 with add_shipping_info_with_params as (
   select * except(ecommerce),
      {{ ga4.unnest_key('event_params', 'coupon') }},
      {{ ga4.unnest_key('event_params', 'currency') }},
      {{ ga4.unnest_key('event_params', 'shipping_tier') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }}
      {% if var("add_shipping_info_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("add_shipping_info_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'add_shipping_info'
)

select * from add_shipping_info_with_params