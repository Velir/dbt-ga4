 with scroll_with_params as (
   select *,
      {{ unnest_key('event_params', 'percent_scrolled', 'int_value') }}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'scroll'
)

select * from scroll_with_params