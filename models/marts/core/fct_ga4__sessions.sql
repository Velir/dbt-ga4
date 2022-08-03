-- Fact table for sessions. Join on session_key
{% if var('static_incremental_days', false ) %}
    {% set partitions_to_replace = [] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "session_start_date",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "session_start_date",
                "data_type": "date",
            },
        )
    }}
{% endif %}

with session_metrics as 
(
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
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            and session_start_date in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by 1,2
)
{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from session_metrics
    left join {{ref('stg_ga4__session_conversions')}} using (session_key)
)
select * from join_conversions
{% else %}
select * from session_metrics
{% endif %}

