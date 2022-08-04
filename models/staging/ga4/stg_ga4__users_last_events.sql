with last_event as (
    select
        user_key,
        last_value(event_key) OVER (PARTITION BY user_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_event
    from {{ref('stg_ga4__events')}}
    where user_key is not null --remove users with privacy settings enabled
),
events_by_user_key as (
    select distinct
        user_key,
        last_event
    from last_event
),
events_joined as (
    select
        events_by_user_key.*,
        events_last.geo as last_geo,
        events_last.device as last_device,
        events_last.traffic_source as last_traffic_source,
        events_last.ga_session_number as num_sessions
    from events_by_user_key
    left join {{ref('stg_ga4__events')}} events_last
        on events_by_user_key.last_event = events_last.event_key
)
select * from events_joined