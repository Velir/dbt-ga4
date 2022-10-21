with session_start_events as 
(
    select * from {{ref('stg_ga4__event_session_start')}}
),
session_start_row_number as 
(
    select *,
    row_number() over(partition by session_key order by event_timestamp) as session_event_number
    from session_start_events
)

select * from session_start_row_number where session_event_number = 1