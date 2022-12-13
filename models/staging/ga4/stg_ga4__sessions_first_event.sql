with session_events as 
(
    select * from {{ref('stg_ga4__events')}}
),
session_first_row_number as 
(
    select *
    from session_events
    qualify row_number() over(partition by session_key order by event_timestamp) = 1
)

select * from session_first_row_number