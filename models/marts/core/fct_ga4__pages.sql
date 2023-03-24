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
        stream_id,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        page_key,
        page_path,
        page_title,  -- would like to move this to dim_ga4__pages but need to think how to handle page_title changing over time
        {% if var('ga4.stream_names', false) %}stream_name,{% endif %}
        page_engagement_key,
        count(event_name) as page_views,
        count(distinct user_pseudo_id ) as distinct_user_pseudo_ids,
        sum( if(session_number = 1,1,0)) as new_user_pseudo_ids,
        sum(entrances) as entrances,
    from {{ref('stg_ga4__event_page_view')}}
    group by 1,2,3,4,5,6,7{% if var('ga4.stream_names', false) %},8{% endif %}
), page_engagement as (
    select
        page_view.* except(page_engagement_key),
        sum(page_engagement_time_msec) as total_engagement_time_msec,
        sum( page_engagement_denominator) as avg_engagement_time_denominator
    from {{ ref('stg_ga4__page_engaged_time') }}
    right join page_view using (page_engagement_key)
    group by 1,2,3,4,5,6,7,8,9,10
), scroll as (
    select
        event_date_dt,
        page_location, 
        page_title,
        count(event_name) as scroll_events
    from {{ref('stg_ga4__event_scroll')}}
    group by 1,2,3
)
{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from page_engagement
    left join {{ ref('stg_ga4__page_conversions') }} using (page_key)
)
select
    join_conversions.*  except (page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from join_conversions
left join scroll using (event_date_dt, page_location, page_title)
{% else %}
select
    page_engagement.* except (page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_engagement
left join scroll using (event_date_dt, page_location, page_title)
{% endif %}