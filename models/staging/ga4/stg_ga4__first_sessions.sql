{{ config(
    materialized= 'incremental',
    unique_key='session_key',
)
}}
with events as (
    select 
        session_key,
        user_key,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp
    from {{ref('stg_ga4__events')}}
    where ga_session_number = 1
    group by 1,2
), session_starts as (
    select
        session_key,
        geo as first_geo,
        device as first_device,
        traffic_source as first_traffic_source,
        page_referrer as first_page_referrer,
        page_location as first_page_location,
        page_hostname as first_page_hostname,
        events.* except(session_key)
    from {{ref('stg_ga4__events')}}
    right join events using (session_key) -- if the session_start event is missing
    where event_name = "session_start" 
    and ga_session_number = 1
)

select * from session_starts