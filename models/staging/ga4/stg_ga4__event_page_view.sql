 with page_view_with_params as (
   select *,
      {{ unnest_key('event_params', 'entrances',  'int_value') }},
      {{ unnest_key('event_params', 'page_title') }},
      {{ unnest_key('event_params', 'page_referrer') }},
      {{ unnest_key('event_params', 'value', 'float_value') }}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'page_view'
)

select * from page_view_with_params