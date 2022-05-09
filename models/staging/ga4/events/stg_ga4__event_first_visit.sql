-- TODO: Unclear why there are first_visit events firing when the ga_session_number is >1. This might cause confusion.

with first_visit_with_params as (
 select 
    *,
    {{ unnest_key('event_params', 'page_location', 'string_value', 'landing_page') }} 
 from {{ref('stg_ga4__events')}}    
 where event_name = 'first_visit'
)

select * from first_visit_with_params