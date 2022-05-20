with first_last_event as (
    select
        client_id,
        FIRST_VALUE(event_key) OVER (PARTITION BY client_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event,
        LAST_VALUE(event_key) OVER (PARTITION BY client_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event
    from {{ref('stg_ga4__events')}}
),
events_by_client_id as (
    select distinct
        client_id,
        first_event,
        last_event
    from first_last_event
),
events_joined as (
    select
        events_by_client_id.*,
        events_first.geo as first_geo,
        events_first.device as first_device,
        events_first.traffic_source as first_traffic_source,
        events_last.geo as last_geo,
        events_last.device as last_device,
        events_last.traffic_source as last_traffic_source
    from events_by_client_id
    left join {{ref('stg_ga4__events')}} events_first
        on events_by_client_id.first_event = events_first.event_key
    left join {{ref('stg_ga4__events')}} events_last
        on events_by_client_id.last_event = events_last.event_key
)

select * from events_joined