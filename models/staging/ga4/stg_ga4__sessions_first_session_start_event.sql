with session_start_events as 
(
    select * from {{ref('stg_ga4__event_session_start')}}
),
session_start_first_row_number as 
(
    select *
    from session_start_events
    qualify row_number() over(partition by session_key order by event_timestamp) = 1
    
)

select * from session_start_first_row_number