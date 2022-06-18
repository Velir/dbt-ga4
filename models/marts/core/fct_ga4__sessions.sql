-- Fact table for sessions. Join on session_key

with session_metrics as 
(
    select 
        session_key,
        client_id,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged
    from {{ref('stg_ga4__events')}}
    group by 1,2
),
session_conversions as (
    select * from {{ref('stg_ga4__session_conversions')}}
),
join_conversions as (
    select 
        *
    from session_metrics
    inner join session_conversions using (session_key)
)


select * from join_conversions