{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
        "field": "session_start_date",
        "data_type": "date",
        "granularity": "day"
        }
    )
}}

-- TODO: incorporate session conversions. requires refactoring that model to partition on session start date

{% if is_incremental() %}
    with session_metrics as (
        select 
            session_key,
            user_pseudo_id,
            min(event_date_dt) as session_start_date,
            min(event_timestamp) as session_start_timestamp,
            countif(event_name = 'page_view') as count_page_views,
            sum(event_value_in_usd) as sum_event_value_in_usd,
            ifnull(max(session_engaged), 0) as session_engaged,
            sum(engagement_time_msec) as sum_engagement_time_msec
        from {{ref('stg_ga4__events')}}
        -- Give 1 extra day to ensure we beging aggregation at the start of a session
        where event_date_dt >= DATE_SUB(_dbt_max_partition, INTERVAL 1 DAY)
        and session_key is not null
        group by 1,2
    )
{% else %}

    with session_metrics as (
    select 
        session_key,
        user_pseudo_id,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    where session_key is not null
    group by 1,2
    )

{% endif %}

select * from session_metrics

