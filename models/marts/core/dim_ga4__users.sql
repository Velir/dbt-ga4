-- User dimensions: first geo, first device, last geo, last device, first seen, last seen
{{ config(
    materialized= 'incremental',
    unique_key='user_key'
)
}}

with last_event_values as (  -- get the last session start event for each user
    select
        *
    from {{ref('stg_ga4__users_last_events')}} 
),
last_pageview_values as (  -- get the last session start event for each user
    select
        *
    from {{ref('stg_ga4__users_last_pageviews')}} 
    left join last_event_values using (user_key)
),
first_session as (
    select
        *
    from {{ref('stg_ga4__first_sessions')}}
    right join last_pageview_values using (user_key) -- limit to users with sessions in the window of time that we are working on
)
{% if is_incremental() %} -- get current values and new values 
,current_values as (
    select
        user_key
        num_page_views,
        num_purchases
    from {{this}}
    where user_key in users.user_key
),
last_modified as (  -- is there a better way to do this?
    select
        max(last_seen_timestamp) as last_modified
    from {{this}}
),

new_values as (
    select 
        user_key,
        sum(is_page_view) as num_page_views,
        sum(is_purchase) as num_purchases,
    from {{ref('stg_ga4__events')}}
    where user_key is not null -- Remove users with privacy settings enabled
    and event_timestamp > last_modified.last_modified
    group by 1
)
select
    new_values.user_key,
    sum(current_values.num_page_views + new_values.num_page_views) as num_page_views,
    sum(current_values.num_page_purchases + new_values.num_purchases) as num_purchases
from new_values
left join current_values using (user_key)
left join first_sessions using (user_key)

{% else %} -- build the table from scratch
select 
    user_key,
    max(event_timestamp) as last_seen_timestamp,
    max(event_date_dt) as last_seen_dt,
    max(ga_session_number) as num_sessions,
    sum(is_page_view) as num_page_views,
    sum(is_purchase) as num_purchases
from {{ref('stg_ga4__events')}}
left join first_sessions using (user_key)
where user_key is not null -- Remove users with privacy settings enabled
group by 1
{% endif %}