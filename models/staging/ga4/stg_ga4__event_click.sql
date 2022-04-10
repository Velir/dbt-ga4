 with click_with_params as (
   select *,
      {{ unnest_key('event_params', 'entrances',  'int_value') }},
      {{ unnest_key('event_params', 'page_title') }},
      {{ unnest_key('event_params', 'page_referrer') }},
      {{ unnest_key('event_params', 'outbound') }},
      {{ unnest_key('event_params', 'link_domain') }},
      {{ unnest_key('event_params', 'link_url') }},
      {{ unnest_key('event_params', 'click_element') }},
      {{ unnest_key('event_params', 'click_id') }},
      {{ unnest_key('event_params', 'click_region') }},
      {{ unnest_key('event_params', 'click_tag_name') }},
      {{ unnest_key('event_params', 'click_url') }},
      {{ unnest_key('event_params', 'file_name') }}
 from {{ref('stg_ga4__events')}}
 where event_name = 'click'
)

select * from click_with_params