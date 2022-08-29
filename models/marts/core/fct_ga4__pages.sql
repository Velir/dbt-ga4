{{ config(
    materialized= 'incremental',
    unique_key=['page_key']
)
}}
with page_view as (
    select
        page_key,
        event_date_dt,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        count(event_name) as page_views,
        count(distinct user_key ) as users,
        sum( if(ga_session_number = 1,1,0)) as new_users,
        sum(entrances) as entrances,
        sum(exits) as exits,
        sum(engagement_time_msec) as total_time_on_page 
from {{ref('stg_ga4__event_page_view')}}
    group by 1,2,3
), scroll as (
    select
        page_key,
        event_date_dt,
        page_location, 
        count(event_name) as scroll_events
    from {{ref('stg_ga4__event_scroll')}}
    group by 1,2,3
)
{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from page_view
    left join {{ ref('stg_ga4__page_conversions') }} using (page_key)
)
select
    join_conversions.*,
    ifnull(scroll.scroll_events, 0) as scroll_events
from join_conversions
left join scroll using (page_key)
{% else %}
select
    page_view.*,
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_view
left join scroll using (page_key)
{% endif %}