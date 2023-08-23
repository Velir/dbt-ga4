select
    client_key,
    stream_id,
    stream_name,
    min(session_partition_min_timestamp) as first_seen_timestamp,
    min(session_partition_date) as first_seen_start_date,
    sum(session_partition_count_page_views) as count_pageviews,
    sum(session_partition_max_session_engaged) as count_engaged_sessions,
    sum(session_partition_sum_event_value_in_usd) as sum_event_value_in_usd,
    sum(session_partition_sum_engagement_time_msec) as sum_engaged_time_msec,
    count(distinct session_key)  as count_sessions
    {% if var('conversion_events', false) %}
        {% for ce in var('conversion_events',[]) %}
            , sum({{ce}}_count) as count_{{ce}}
        {% endfor %}
    {% endif %}
from {{ref('fct_ga4__sessions_daily')}}
group by 1, 2

