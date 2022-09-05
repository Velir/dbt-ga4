{{
    config(
        materialized = 'incremental',
        tags = ["incremental"],
        unique_key='session_key'
    )
}}

{% if is_incremental() %}
    -- Gather all sessions that started (as opposed to continued) in the date range we care about.
    -- Give 1 extra day to account for sessions we have only partially prossed
    with max_date_cte as (
        select DATE_SUB(max(session_start_date), INTERVAL 1 DAY) as max_session_start_date_less_1 
        from {{this}}
    ), 
    sessions_to_process as (
        select 
            session_key 
        from {{ref('stg_ga4__events')}}
        where event_date_dt >= 
            (select max_session_start_date_less_1 from max_date_cte)
        -- Use 'session_start' event to determine which new sessions to process
        and event_name = 'session_start'    
    ),
    session_metrics as (
        select 
            session_key,
            user_key,
            min(event_date_dt) as session_start_date,
            min(event_timestamp) as session_start_timestamp,
            countif(event_name = 'page_view') as count_page_views,
            sum(event_value_in_usd) as sum_event_value_in_usd,
            ifnull(max(session_engaged), 0) as session_engaged,
            sum(engagement_time_msec) as sum_engagement_time_msec
        from {{ref('stg_ga4__events')}}
        where event_date_dt >= (select max_session_start_date_less_1 from max_date_cte)
        and session_key in (select session_key from sessions_to_process)
        group by 1,2
    )
{% else %}

    with session_metrics as (
    select 
        session_key,
        user_key,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    group by 1,2
    )

{% endif %}

select * from session_metrics

