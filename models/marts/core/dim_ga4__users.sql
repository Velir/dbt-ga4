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
first_session as (
    select
        *
    from {{ref('stg_ga4__first_sessions')}}
    right join last_event_values using (user_key) -- limit to users with sessions in the window of time that we are working on
    {% if var('derived_user_properties', false) %}
        -- If derived user properties have been assigned as variables, join them on the user_key
        left join {{ref('stg_ga4__derived_user_properties')}} using (user_key)
    {% endif %}
    {% if var('user_properties', false) %}
        -- If user properties have been assigned as variables, join them on the user_key
        left join {{ref('stg_ga4__user_properties')}} using (user_key)
    {% endif %}
),
{% if is_incremental() %} -- get current values and new values 
current_values as (
    select
        user_key,
        num_page_views,
        num_purchases
    from {{this}}
    where user_key in (
        select 
            user_key
        from first_session
    )
),
new_values as (
    select 
        user_key,
        sum(is_page_view) as num_page_views,
        sum(is_purchase) as num_purchases
    from {{ref('stg_ga4__events')}}
    where user_key is not null -- Remove users with privacy settings enabled
    and event_timestamp > (select max(last_seen_timestamp) from {{this}})  -- Only calculate users with new data
    group by 1
)
select
    new_values.user_key,
    ifnull(current_values.num_page_views + new_values.num_page_views,0) as num_page_views,
    ifnull(current_values.num_purchases + new_values.num_purchases,0) as num_purchases,
    first_session.* except(user_key)
from new_values
left join current_values using (user_key)
left join first_session using (user_key)

{% else %} -- build the table from scratch
user_events as (
    select 
        user_key,
        sum(is_page_view) as num_page_views,
        sum(is_purchase) as num_purchases
    from {{ref('stg_ga4__events')}}
    group by 1
)

select 
    user_events.*,
    first_session.* except(user_key)
from user_events
left join first_session using (user_key)
where user_key is not null -- Remove users with privacy settings enabled
{% endif %}