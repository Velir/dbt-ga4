-- Dimension table for sessions based on the session_start event.

with session_start_dims as (
    select 
        session_key,
        page_location as landing_page,
        page_hostname as landing_page_hostname,
        page_referrer as landing_page_referrer,
        geo_continent,
        geo_country,
        geo_region,
        geo_city,
        geo_sub_continent,
        geo_metro,
        stream_id,
        platform,
        device_category,
        device_mobile_brand_name,
        device_mobile_model_name,
        device_mobile_marketing_name,
        device_mobile_os_hardware_model,
        device_operating_system,
        device_operating_system_version,
        device_vendor_id,
        device_advertising_id,
        device_language,
        device_is_limited_ad_tracking,
        device_time_zone_offset_seconds,
        device_browser,
        device_web_info_browser,
        device_web_info_browser_version,
        device_web_info_hostname,
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
    from {{ref('stg_ga4__sessions_first_session_start_event')}}
),
join_traffic_source as (
    select 
        session_start_dims.*,
        session_source as source,
        session_medium as medium,
        session_campaign as campaign,
        session_default_channel_grouping as default_channel_grouping
    from session_start_dims
    left join {{ref('stg_ga4__sessions_traffic_sources')}} using (session_key)
)

select * from join_traffic_source