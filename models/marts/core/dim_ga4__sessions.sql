-- Dimension table for sessions
-- TODO - convert traffic source to channel grouping

with session_start_dims as (
    select 
        session_key,
        traffic_source_name,
        traffic_source_source,
        traffic_source_medium,
        ga_session_number,
        page_location as landing_page,
        page_hostname as landing_page_hostname,
        geo,
        device,
        row_number() over (partition by session_key order by session_event_number asc) as row_num
    from {{ref("stg_ga4__event_session_start")}}
),
-- Arbitrarily pull the first session_start event to remove duplicates
remove_dupes as 
(
    select * from session_start_dims
    where row_num = 1
)

select * from remove_dupes