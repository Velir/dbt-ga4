{{
  config(
      enabled = false,
  )
}}
with purchase_with_params as (
  select * except (ecommerce),
    ecommerce.total_item_quantity,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.shipping_value_in_usd,
    ecommerce.shipping_value,
    ecommerce.tax_value_in_usd,
    ecommerce.tax_value,
    ecommerce.unique_items,
    {{ ga4.unnest_key('event_params', 'coupon') }},
    {{ ga4.unnest_key('event_params', 'transaction_id') }},
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'tax', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'shipping', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'affiliation') }}
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("purchase_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("purchase_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'purchase'
)

select * from purchase_with_params