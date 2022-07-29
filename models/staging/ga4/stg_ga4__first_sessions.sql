{{ config(
    materialized= 'incremental',
    unique_key='session_key'
)
}}
with all_page_views as (
    select
        session_key,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec,
    from {{ref('stg_ga4__events')}}
    group by session_key
),
first_page_view as (
    select
        session_key,
        user_key,
        event_date_dt,
        event_timestamp,
        event_value_in_usd,
        traffic_source,
        ga_session_number,
        page_location as landing_page,
        page_hostname as landing_page_hostname,
        entrances,
        geo,
        device,
        event_key as first_page_view_event_key
    from {{ref('stg_ga4__events')}}
    left join all_page_views using (session_key)
    where event_name = "page_view" and ga_session_number = 1 and entrances = 1
),
last_page_view as (
    select
        session_key,
        event_key as last_page_view_event_key
    from {{ref('stg_ga4__events')}}
    where event_timestamp in (select max(event_timestamp) from {{ref('stg_ga4__events')}} group by session_key) and ga_session_number = 1
)


{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from first_page_view
    left join {{ref('stg_ga4__session_conversions')}} using (session_key)
    left join last_page_view using (session_key)
)
select * from join_conversions
{% else %}
select * from last_page_view
left join last_page_view using (session_key)
{% endif %}