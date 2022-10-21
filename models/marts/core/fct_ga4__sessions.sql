-- Fact table for sessions. Join on session_key

with session_metrics as 
(
    select distinct
        session_key,
        last_value(user_key) over (session_window) as user_key,
        first_value(event_date_dt) over (session_window) as session_start_date,
        first_value(event_timestamp) over (session_window) as session_start_timestamp,
        countif(event_name = 'page_view') over (session_window) as count_page_views,
        sum(event_value_in_usd) over (session_window) as sum_event_value_in_usd,
        ifnull(last_value(session_engaged) over (session_window), 0) as session_engaged,
        sum(engagement_time_msec) over (session_window) as sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    window session_window as (partition by session_key order by event_timestamp rows between unbounded preceding and unbounded following)
),

include_session_properties as (
    select * from session_metrics
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} using (session_key)
    {% endif %}
)

{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from include_session_properties
    left join {{ref('stg_ga4__session_conversions')}} using (session_key)
)
select * from join_conversions
{% else %}
select * from include_session_properties
{% endif %}

