-- Fact table for sessions. Join on session_key

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
    group by 1,2
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

