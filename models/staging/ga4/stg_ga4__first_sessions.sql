{{ config(
    materialized= 'incremental',
    unique_key='session_key',
)
}}
with first_events as (
    select
        first_page_view_event_key as event_key,
    from {{ ref('stg_ga4__sessions_first_last_pageviews') }}
    where ga_session_number = 1
)
select
    session_key,
    user_key,
    event_date_dt as session_start_date,
    event_timestamp as session_start_timestamp,
    geo as first_geo,
    device as first_device,
    traffic_source as first_traffic_source,
    page_referrer as first_page_referrer,
    page_location as first_page_location,
    page_hostname as first_page_hostname,
from {{ ref('stg_ga4__events') }}
right join first_events using (event_key)
