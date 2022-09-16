with events_first_last as (
    select
        session_key,
        user_key,
        FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_timestamp,
        FIRST_VALUE(event_date_dt) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_date_dt,
        FIRST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_key,
        LAST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_key,
    from {{ref('stg_ga4__events')}}
),
events_by_session_key as (
    select distinct
        session_key,
        user_key,
        first_event_timestamp,
        first_event_date_dt,
        first_event_key,
        last_event_key
    from events_first_last
)

select * from events_by_session_key