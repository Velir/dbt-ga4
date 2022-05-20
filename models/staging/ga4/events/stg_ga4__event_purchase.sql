 with purchase_with_params as (
   select *,
      {{ unnest_key('event_params', 'coupon') }},
      {{ unnest_key('event_params', 'transaction_id') }},
      {{ unnest_key('event_params', 'currency') }},
      {{ unnest_key('event_params', 'payment_type') }},
      {{ unnest_key('event_params', 'value', 'float_value') }}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'purchase'
)

select * from purchase_with_params