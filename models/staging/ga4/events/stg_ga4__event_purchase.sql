{% if var('ecommerce', false ) ==  false %}
    {{
      config(
          enabled = false,
      )
  }}
{% endif %}
with purchase_with_params as (
  select * except (ecommerce),
    (select ecommerce FROM UNNEST(ecommerce) ecommerce LIMIT 1) as ecommerce
    {{ ga4.unnest_key('event_params', 'coupon') }},
    {{ ga4.unnest_key('event_params', 'transaction_id') }},
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
    {{ ga4.unnest_key('event_params', 'tax', 'float_value') }},
    {{ ga4.unnest_key('event_params', 'shipping', 'float_value') }},
    {{ ga4.unnest_key('event_params', 'affiliation') }},
    {% if var("purchase_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("purchase_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}},
  unnest (ecommerce)
 where event_name = 'purchase'
)

select * from purchase_with_params