{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['event_date_dt', 'stream_id' , 'page_location'],
        tags = ["incremental"],
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
            "granularity": "day"
        },
        on_schema_change = 'sync_all_columns',
    )
}}

with page_view as (
    select
        event_date_dt,
        stream_id,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        page_key,
        page_engagement_key,
        count(event_name) as page_views,
        count(distinct client_key ) as distinct_client_keys,
        sum( if(session_number = 1,1,0)) as new_client_keys,
        sum(entrances) as entrances,
from {{ref('stg_ga4__event_page_view')}}
{% if is_incremental() %}
        where event_date_dt >= date_sub(current_date, interval {{var('static_incremental_days',3)}} day)
{% endif %}
    group by 1,2,3,4,5
), page_engagement as (
    select
        page_view.event_date_dt,
        page_view.stream_id,
        page_view.page_location,
        page_view.page_key,
        sum(page_view.page_views) as page_views,  -- page_engagement_key references the page_referrer; need to re-aggregate metrics
        sum(page_view.distinct_client_keys) as distinct_client_keys,
        sum(page_view.new_client_keys) as new_client_keys,
        sum(page_view.entrances) as entrances,
        sum(page_engagement_time_msec) as total_engagement_time_msec,
        sum( page_engagement_denominator) as avg_engagement_time_denominator
    from {{ ref('stg_ga4__page_engaged_time') }}
    right join page_view using (page_engagement_key)
    group by 1,2,3,4
), scroll as (
    select
        event_date_dt,
        page_location, 
        count(event_name) as scroll_events
    from {{ref('stg_ga4__event_scroll')}}
    {% if is_incremental() %}
            where event_date_dt >= date_sub(current_date, interval {{var('static_incremental_days',3)}} day)
    {% endif %}
    group by 1,2
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
left join scroll using (event_date_dt, page_location)
{% else %}
select
    page_engagement.* except (page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_engagement
left join scroll using (event_date_dt, page_location)
{% endif %}