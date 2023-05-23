select
    client_key,
    stream_id,
    min(session_start_timestamp) as first_seen_timestamp,
    min(session_start_date) as first_seen_start_date,
    sum(count_pageviews) as count_pageviews,
    sum(is_session_engaged) as count_engaged_sessions,
    sum(sum_event_value_in_usd) as sum_event_value_in_usd,
    sum(sum_engaged_time_msec) as sum_engaged_time_msec,
    count(distinct session_key)  as count_sessions
    {% if var('conversion_events', false) %}
        {% for ce in var('conversion_events',[]) %}
            , sum(count_{{ce}}) as count_{{ce}}
        {% endfor %}
    {% endif %}
from {{ref('fct_ga4__sessions')}}
group by 1, 2

