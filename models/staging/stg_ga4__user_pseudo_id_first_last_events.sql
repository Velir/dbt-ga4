{{
    config(materialized = "table")
}}

with first_last_event as (
    select
        user_pseudo_id,
        FIRST_VALUE(event_key) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event,
        LAST_VALUE(event_key) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event
    from {{ref('stg_ga4__events')}}
    where user_pseudo_id is not null --remove users with privacy settings enabled
),
events_by_user_pseudo_id as (
    select distinct
        user_pseudo_id,
        first_event,
        last_event
    from first_last_event
),
events_joined as (
    select
        events_by_user_pseudo_id.*,
        events_first.geo_continent as first_geo_continent,
        events_first.geo_country as first_geo_country,
        events_first.geo_region as first_geo_region,
        events_first.geo_city as first_geo_city,
        events_first.geo_sub_continent as first_geo_sub_continent,
        events_first.geo_metro as first_geo_metro,
        events_first.device_category as first_device_category,
        events_first.device_mobile_brand_name as first_device_mobile_brand_name,
        events_first.device_mobile_model_name as first_device_mobile_model_name,
        events_first.device_mobile_marketing_name as first_device_mobile_marketing_name,
        events_first.device_mobile_os_hardware_model as first_device_mobile_os_hardware_model,
        events_first.device_operating_system as first_device_operating_system,
        events_first.device_operating_system_version as first_device_operating_system_version,
        events_first.device_vendor_id as first_device_vendor_id,
        events_first.device_advertising_id as first_device_advertising_id,
        events_first.device_language as first_device_language,
        events_first.device_is_limited_ad_tracking as first_device_is_limited_ad_tracking,
        events_first.device_time_zone_offset_seconds as first_device_time_zone_offset_seconds,
        events_first.device_browser as first_device_browser,
        events_first.device_browser_version as first_device_browser_version,
        events_first.device_web_info_browser as first_device_web_info_browser,
        events_first.device_web_info_browser_version as first_device_web_info_browser_version,
        events_first.device_web_info_hostname as first_device_web_info_hostname,
        events_first.traffic_source_name as first_traffic_source_name,
        events_first.traffic_source_medium as first_traffic_source_medium,
        events_first.traffic_source_source as first_traffic_source_source,
        events_last.geo_continent as last_geo_continent,
        events_last.geo_country as last_geo_country,
        events_last.geo_region as last_geo_region,
        events_last.geo_city as last_geo_city,
        events_last.geo_sub_continent as last_geo_sub_continent,
        events_last.geo_metro as last_geo_metro,
        events_last.device_category as last_device_category,
        events_last.device_mobile_brand_name as last_device_mobile_brand_name,
        events_last.device_mobile_model_name as last_device_mobile_model_name,
        events_last.device_mobile_marketing_name as last_device_mobile_marketing_name,
        events_last.device_mobile_os_hardware_model as last_device_mobile_os_hardware_model,
        events_last.device_operating_system as last_device_operating_system,
        events_last.device_operating_system_version as last_device_operating_system_version,
        events_last.device_vendor_id as last_device_vendor_id,
        events_last.device_advertising_id as last_device_advertising_id,
        events_last.device_language as last_device_language,
        events_last.device_is_limited_ad_tracking as last_device_is_limited_ad_tracking,
        events_last.device_time_zone_offset_seconds as last_device_time_zone_offset_seconds,
        events_last.device_browser as last_device_browser,
        events_last.device_browser_version as last_device_browser_version,
        events_last.device_web_info_browser as last_device_web_info_browser,
        events_last.device_web_info_browser_version as last_device_web_info_browser_version,
        events_last.device_web_info_hostname as last_device_web_info_hostname,
        events_last.traffic_source_name as last_traffic_source_name,
        events_last.traffic_source_medium as last_traffic_source_medium,
        events_last.traffic_source_source as last_traffic_source_source,
    from events_by_user_pseudo_id
    left join {{ref('stg_ga4__events')}} events_first
        on events_by_user_pseudo_id.first_event = events_first.event_key
    left join {{ref('stg_ga4__events')}} events_last
        on events_by_user_pseudo_id.last_event = events_last.event_key
)

select * from events_joined