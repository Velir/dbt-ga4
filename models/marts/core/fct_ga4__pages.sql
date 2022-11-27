{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
        "field": "event_date_dt",
        "data_type": "date",
        "granularity": "day"
        }
    )
}}

with page_view as (
    select
        event_date_dt,
        extract( hour from (select  timestamp_micros(event_timestamp))) as hour,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        page_key,
        page_title,  -- would like to move this to dim_ga4__pages but need to think how to handle page_title changing over time
        count(event_name) as page_views,
        count(distinct user_key ) as users,
        sum( if(ga_session_number = 1,1,0)) as new_users,
        sum(entrances) as entrances,
        sum(engagement_time_msec) as total_time_on_page 
        
from {{ref('stg_ga4__event_page_view')}}
    group by 1,2,3,4,5
), scroll as (
    select
        event_date_dt,
        extract( hour from (select timestamp_micros(event_timestamp))) as hour,
        page_location, 
        page_title,
        count(event_name) as scroll_events
    from {{ref('stg_ga4__event_scroll')}}
    group by 1,2,3,4
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
    join_conversions.*  except(page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from join_conversions
left join scroll using (event_date_dt, hour, page_location, page_title)
{% else %}
select
    page_view.* except(page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_view
left join scroll using (event_date_dt, hour, page_location, page_title)
{% endif %}