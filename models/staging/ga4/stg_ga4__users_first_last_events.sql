{{
    config(materialized = "table")
}}

with first_last_event as (
    select
        user_key,
        FIRST_VALUE(event_key) OVER (PARTITION BY user_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event,
        LAST_VALUE(event_key) OVER (PARTITION BY user_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event
    from {{ref('stg_ga4__events')}}
    where user_key is not null --remove users with privacy settings enabled
),
events_by_user_key as (
    select distinct
        user_key,
        first_event,
        last_event
    from first_last_event
),
events_joined as (
    select
        events_by_user_key.*,
        events_first.geo as first_geo,
        events_first.device as first_device,
        events_first.traffic_source as first_traffic_source,
        events_last.geo as last_geo,
        events_last.device as last_device,
        events_last.traffic_source as last_traffic_source
    from events_by_user_key
    left join {{ref('stg_ga4__events')}} events_first
        on events_by_user_key.first_event = events_first.event_key
    left join {{ref('stg_ga4__events')}} events_last
        on events_by_user_key.last_event = events_last.event_key
)

select * from events_joined