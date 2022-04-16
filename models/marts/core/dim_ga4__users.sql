-- User dimensions: first geo, first device, last geo, last device, first seen, last seen

with users as (
    select 
        client_id,
        min(event_timestamp) as first_seen_timestamp,
        min(event_date_dt) as first_seen_dt,
        max(event_timestamp) as last_seen_timestamp,
        max(event_date_dt) as last_seen_dt,
        count(distinct session_key) as num_sessions,
        sum(is_page_view) as num_page_views,
        sum(is_purchase) as num_purchases
    from {{ref('base_ga4__events')}}
    group by 1

),
include_first_last_events as (
    select 
        users.*,
        first_last_events.first_geo,
        first_last_events.first_device,
        first_last_events.last_geo,
        first_last_events.last_device
    from users 
    left join {{ref('stg_ga4__users_first_last_events')}} as first_last_events on
        users.client_id = first_last_events.client_id
),
include_first_last_page_views as (
    select 
        include_first_last_events.*,
        first_last_page_views.first_page_location,
        first_last_page_views.first_page_hostname,
        first_last_page_views.first_page_referrer,
        first_last_page_views.last_page_location,
        first_last_page_views.last_page_hostname,
        first_last_page_views.last_page_referrer
    from include_first_last_events 
    left join {{ref('stg_ga4__users_first_last_pageviews')}} as first_last_page_views on
        include_first_last_events.client_id = first_last_page_views.client_id
)

select * from include_first_last_page_views