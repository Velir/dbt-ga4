{{ config(
    materialized= 'incremental',
    unique_key='first_session_key',
)
}}
with first_events as (
    select
        first_page_view_event_key as event_key,
    from {{ ref('stg_ga4__sessions_first_last_pageviews') }}
    where ga_session_number = 1
)
select
    session_key as first_session_key,
    user_key,
    event_date_dt as first_session_start_date,
    event_timestamp as first_session_start_timestamp,
    geo_country as first_geo_country,
    geo_region as first_geo_region,
    geo_city as first_geo_city,    
    device_category as first_device_category,
    device_mobile_brand_name as first_device_mobile_brand_name,
    device_mobile_model_name as first_device_mobile_model_name,
    device_mobile_marketing_name as first_device_mobile_marketing_name,
    device_mobile_os_hardware_model as first_device_mobile_os_hardware_model,
    device_operating_system as first_device_operating_system,
    device_operating_system_version as first_device_operating_system_version,
    device_vendor_id as first_device_vendor_id,
    device_advertising_id as first_device_advertising_id,
    device_language as first_device_language,
    device_is_limited_ad_tracking as first_device_is_limited_ad_tracking,
    device_time_zone_offset_seconds as first_device_time_zone_offset_seconds,
    device_browser as first_device_browser,
    device_browser_version as first_device_browser_version,
    device_web_info_browser as first_device_web_info_browser,
    device_web_info_browser_version as first_device_web_info_browser_version,
    device_web_info_hostname as first_device_web_info_hostname,
    traffic_source_name as first_traffic_source_name,
    traffic_source_medium as first_traffic_source_medium,
    traffic_source_source as first_traffic_source_source,
    page_referrer as first_page_referrer,
    page_location as first_page_location,
    page_hostname as first_page_hostname,
from {{ ref('stg_ga4__events') }}
right join first_events using (event_key)