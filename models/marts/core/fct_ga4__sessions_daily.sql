{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
        "field": "session_partition_date",
        "data_type": "date",
        "granularity": "day"
        }
    )
}}

with session_metrics as (
    select 
        session_key,
        session_partition_key,
        user_pseudo_id,
        min(event_date_dt) as session_partition_date, -- Used only as a method of partitioning sessions within this incremental table. Does not represent the true session start date
        min(event_timestamp) as session_partition_min_timestamp,
        countif(event_name = 'page_view') as session_partition_count_page_views,
        sum(event_value_in_usd) as session_partition_sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_partition_max_session_engaged,
        sum(engagement_time_msec) as session_partition_sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    -- Give 1 extra day to ensure we beging aggregation at the start of a session
    where session_key is not null
    {% if is_incremental() %}
        and event_date_dt >= DATE_SUB(_dbt_max_partition, INTERVAL 1 DAY)
    {% endif %}
    group by 1,2,3
)
{% if var('conversion_events', false) == false %}
    select * from session_metrics
{% else %}
    ,
    session_conversions as (
    select * from {{ref('stg_ga4__session_conversions_daily')}}
    {% if is_incremental() %}
        where session_partition_date >= DATE_SUB(_dbt_max_partition, INTERVAL 1 DAY)
    {% endif %}
    ),
    join_metrics_and_conversions as (
        select 
            session_metrics.user_pseudo_id,
            session_metrics.session_partition_min_timestamp,
            session_metrics.session_partition_count_page_views,
            session_metrics.session_partition_sum_event_value_in_usd,
            session_metrics.session_partition_max_session_engaged,
            session_metrics.session_partition_sum_engagement_time_msec,
            session_conversions.*
        from session_metrics left join session_conversions using (session_partition_key)
    )

    select * from join_metrics_and_conversions
{% endif %}

