 with purchase_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'coupon') }},
      {{ ga4.unnest_key('event_params', 'transaction_id') }},
      {{ ga4.unnest_key('event_params', 'currency') }},
      {{ ga4.unnest_key('event_params', 'payment_type') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }}
      {% if var("purchase_custom_parameters", "none") != "none" %}
        {{ stage_custom_parameters( var("purchase_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'purchase'
)

select * from purchase_with_params