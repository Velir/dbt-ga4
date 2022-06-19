with session_events as (
    select 
        session_key,
        event_timestamp,
        source,
        medium,
        event_default_channel_grouping
    from {{ref('stg_ga4__events')}}
    -- default channel grouping is only null if both soure and medium are null
    where event_default_channel_grouping is not null
),
session_source as (
    select    
        session_key,
        FIRST_VALUE(source) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_source,
        FIRST_VALUE(medium) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_medium,
        FIRST_VALUE(event_default_channel_grouping) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_default_channel_grouping
    from session_events
)

select distinct * from session_source