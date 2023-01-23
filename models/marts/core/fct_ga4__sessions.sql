-- Stay mindful of performance/cost when leavin this model enabled. Making this model incremental on date is not possible because there's no way to create a single record per session AND partition on date. 

select
    user_pseudo_id,
    session_key,
    {% if var('ga4_datasets', false) %}stream_name,{% endif %}
    min(session_partition_min_timestamp) as session_start_timestamp,
    min(session_partition_date) as session_start_date,
    sum(session_partition_count_page_views) as count_pageviews,
    sum(session_partition_count_purchases) as count_purchases,
    sum(session_partition_sum_event_value_in_usd) as sum_event_value_in_usd,
    max(session_partition_max_session_engaged) as is_session_engaged,
    sum(session_partition_sum_engagement_time_msec) as sum_engaged_time_msec
    {% if var('conversion_events', false) %}
        {% for ce in var('conversion_events',[]) %}
            , sum({{ce}}_count) as count_{{ce}}
        {% endfor %}
    {% endif %}
from {{ref('fct_ga4__sessions_daily')}}
group by 1,2{% if var('ga4_datasets', false) %},3{% endif %}

