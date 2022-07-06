{% if var('ecommerce', false ) ==  false %}
    {{
      config(
          enabled = false,
      )
  }}
{% endif %}
with refund_with_params as (
  select *,
    {{ ga4.unnest_key('event_params', 'coupon') }},
    {{ ga4.unnest_key('event_params', 'transaction_id') }},
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
    {{ ga4.unnest_key('event_params', 'tax', 'float_value') }},
    {{ ga4.unnest_key('event_params', 'shipping', 'float_value') }},
    {{ ga4.unnest_key('event_params', 'affiliation') }},
    {% if var("refund_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("refund_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'refund'
)

select * from refund_with_params