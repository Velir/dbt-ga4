--FULL REFRESH: Bytes processed, 34.39 MB, Bytes billed 35 MB 
--INCREMENTAL: Bytes processed 1.62 MB, Bytes billed  60 MB 
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

{% if is_incremental() %}
    -- TODO: Give 1 extra day to account for sessions we have only partially prossed
    with session_metrics as (
        select 
            session_key,
            min(event_date_dt) as session_start_date,
            --min(event_timestamp) as session_start_timestamp,
            --countif(event_name = 'page_view') as count_page_views,
            --sum(event_value_in_usd) as sum_event_value_in_usd,
            --ifnull(max(session_engaged), 0) as session_engaged,
            --sum(engagement_time_msec) as sum_engagement_time_msec
        from {{ref('stg_ga4__events')}}
        -- This documentation states that using a dynamic value to prune partitions will not work:
        -- https://cloud.google.com/bigquery/docs/querying-partitioned-tables#better_performance_with_pseudo-columns
        where event_date_dt >= _dbt_max_partition
        and session_key is not null
        group by 1
    )
{% else %}

    with session_metrics as (
    select 
        session_key,
        min(event_date_dt) as session_start_date,
        --min(event_timestamp) as session_start_timestamp,
        --countif(event_name = 'page_view') as count_page_views,
        --sum(event_value_in_usd) as sum_event_value_in_usd,
        --ifnull(max(session_engaged), 0) as session_engaged,
        --sum(engagement_time_msec) as sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    where session_key is not null
    group by 1
    )

{% endif %}

select * from session_metrics

