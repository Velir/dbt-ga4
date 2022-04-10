 with page_view_with_params as (
   select *,
      {{ unnest_key('event_params', 'ga_session_number',  'int_value') }},
      {{ unnest_key('event_params', 'page_location') }},
      {{ unnest_key('event_params', 'entrances',  'int_value') }},
      {{ unnest_key('event_params', 'page_title') }},
      {{ unnest_key('event_params', 'page_referrer') }},
      {{ unnest_key('event_params', 'value', 'float_value') }}
 from {{ref('stg_ga4__events')}}    
)

select 
    *,
    {{extract_hostname_from_url('page_location')}} as page_hostname,
    case
        when ga_session_number = 1 then TRUE
        else FALSE
    end as is_new_user
from page_view_with_params