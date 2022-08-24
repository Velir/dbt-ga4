-- User dimensions: first geo, first device, last geo, last device, first seen, last seen

with users as (
    select 
        user_key,
        min(event_timestamp) as first_seen_timestamp,
        min(event_date_dt) as first_seen_dt,
        max(event_timestamp) as last_seen_timestamp,
        max(event_date_dt) as last_seen_dt,
        count(distinct session_key) as num_sessions,
        sum(is_page_view) as num_page_views,
        sum(is_purchase) as num_purchases
    from {{ref('stg_ga4__events')}}
    where user_key is not null -- Remove users with privacy settings enabled
    group by 1

),
include_first_last_events as (
    select 
        users.*,
        first_last_events.first_geo_continent,
        first_last_events.first_geo_country,
        first_last_events.first_geo_region,
        first_last_events.first_geo_city,
        first_last_events.first_geo_sub_continent,
        first_last_events.first_geo_metro,
        first_last_events.first_device_category,
        first_last_events.first_device_mobile_brand_name,
        first_last_events.first_device_mobile_model_name,
        first_last_events.first_device_mobile_marketing_name,
        first_last_events.first_device_mobile_os_hardware_model,
        first_last_events.first_device_operating_system,
        first_last_events.first_device_operating_system_version,
        first_last_events.first_device_vendor_id,
        first_last_events.first_device_advertising_id,
        first_last_events.first_device_language,
        first_last_events.first_device_is_limited_ad_tracking,
        first_last_events.first_device_time_zone_offset_seconds,
        first_last_events.first_device_browser,
        first_last_events.first_device_browser_version,
        first_last_events.first_device_web_info_browser,
        first_last_events.first_device_web_info_browser_version,
        first_last_events.first_device_web_info_hostname,
        first_last_events.first_traffic_source_name,
        first_last_events.first_traffic_source_medium,
        first_last_events.first_traffic_source_source,
        first_last_events.last_geo_continent,
        first_last_events.last_geo_country,
        first_last_events.last_geo_region,
        first_last_events.last_geo_city,
        first_last_events.last_geo_sub_continent,
        first_last_events.last_geo_metro,
        first_last_events.last_device_category,
        first_last_events.last_device_mobile_brand_name,
        first_last_events.last_device_mobile_model_name,
        first_last_events.last_device_mobile_marketing_name,
        first_last_events.last_device_mobile_os_hardware_model,
        first_last_events.last_device_operating_system,
        first_last_events.last_device_operating_system_version,
        first_last_events.last_device_vendor_id,
        first_last_events.last_device_advertising_id,
        first_last_events.last_device_language,
        first_last_events.last_device_is_limited_ad_tracking,
        first_last_events.last_device_time_zone_offset_seconds,
        first_last_events.last_device_browser,
        first_last_events.last_device_browser_version,
        first_last_events.last_device_web_info_browser,
        first_last_events.last_device_web_info_browser_version,
        first_last_events.last_device_web_info_hostname,
        first_last_events.last_traffic_source_name,
        first_last_events.last_traffic_source_medium,
        first_last_events.last_traffic_source_source,
    from users 
    left join {{ref('stg_ga4__users_first_last_events')}} as first_last_events on
        users.user_key = first_last_events.user_key
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
        include_first_last_events.user_key = first_last_page_views.user_key
),
include_user_properties as (
    

select * from include_first_last_page_views
{% if var('derived_user_properties', false) %}
-- If derived user properties have been assigned as variables, join them on the user_key
left join {{ref('stg_ga4__derived_user_properties')}} using (user_key)
{% endif %}
{% if var('user_properties', false) %}
-- If user properties have been assigned as variables, join them on the user_key
left join {{ref('stg_ga4__user_properties')}} using (user_key)
{% endif %}

)

select * from include_user_properties