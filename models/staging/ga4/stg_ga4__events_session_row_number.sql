with events as 
(
    select * from {{ref('stg_ga4__events')}}
),
session_event_row_number as 
(
    select *,
    row_number() over(partition by session_key order by event_timestamp) as session_event_number
    from events
)

select * session_event_row_number