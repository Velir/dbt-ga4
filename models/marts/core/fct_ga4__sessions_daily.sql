{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
            "field": "session_partition_date",
            "data_type": "date",
            "granularity": "day"
        },
        partitions = partitions_to_replace
    )
}}


with session_metrics as (
    select 
        session_key,
        session_partition_key,
        client_key,
        stream_id,
        max(user_id) as user_id, -- user_id can be null at the start and end of a session and still be set in the middle
        min(event_date_dt) as session_partition_date, -- Date of the session partition, does not represent the true session start date which, in GA4, can span multiple days
        min(event_timestamp) as session_partition_min_timestamp,
        countif(event_name = 'page_view') as session_partition_count_page_views,
        sum(event_value_in_usd) as session_partition_sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_partition_max_session_engaged,
        sum(engagement_time_msec) as session_partition_sum_engagement_time_msec,
        min(session_number) as session_number
    from {{ref('stg_ga4__events')}}
    where session_key is not null
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    group by 1,2,3,4
)
{% if var('conversion_events', false) == false %}
    select * from session_metrics
{% else %}
    ,
    session_conversions as (
    select * from {{ref('stg_ga4__session_conversions_daily')}}
    where 1=1
    {% if is_incremental() %}
            and session_partition_date in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    ),
    join_metrics_and_conversions as (
        select 
            session_metrics.client_key,
            session_metrics.stream_id,
            session_metrics.user_id,
            session_metrics.session_partition_min_timestamp,
            session_metrics.session_partition_count_page_views,
            session_metrics.session_partition_sum_event_value_in_usd,
            session_metrics.session_partition_max_session_engaged,
            session_metrics.session_partition_sum_engagement_time_msec,
            session_metrics.session_number,
            session_conversions.*
        from session_metrics left join session_conversions using (session_partition_key)
    )

    select * from join_metrics_and_conversions
{% endif %}

