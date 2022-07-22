with page_view as (
    select
        event_date_dt,
        extract( hour from (select  timestamp_micros(event_timestamp))) as hour,
        page_location,  -- does not include query string; disable this model, copy this to your project and switch to original_page_location to segment by page_location with query string
        concat( cast(event_date_dt as string), cast(extract( hour from (select  timestamp_micros(event_timestamp))) as string), page_location ) as page_key,
        count(event_name) as visits,
        count(distinct user_key ) as users,
        sum( if(ga_session_number = 1,1,0)) as new_users,
        sum(entrances) as entrances,
        sum(exits) as exits,
        avg(engagement_time_msec) as average_time_on_page
from {{ref('stg_ga4__event_page_view')}}
    group by 1,2,3,4
), scroll as (
    select
        event_date_dt,
        extract( hour from (select timestamp_micros(event_timestamp))) as hour,
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
    join_conversions.*  except(page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from join_conversions
left join scroll using (event_date_dt, hour, page_location)
{% else %}
select
    page_view.* except(page_key),
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_view
left join scroll using (event_date_dt, hour, page_location)
{% endif %}