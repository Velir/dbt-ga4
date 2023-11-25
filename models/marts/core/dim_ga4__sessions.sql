-- Dimension table for sessions based on the first event that isn't session_start or first_visit.
with session_first_event as 
(
    select *
    from {{ref('stg_ga4__events')}}
    where event_name != 'first_visit' 
    and event_name != 'session_start'
    qualify row_number() over(partition by session_key order by event_timestamp) = 1
),
 session_start_dims as (
    select 
        session_key,
        event_date_dt as session_start_date,
        event_timestamp as session_start_timestamp,
        page_path as landing_page_path,
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
        session_number,
        session_number = 1 as is_first_session,
        event_campaign,
        event_medium,
        event_source,
    from session_first_event
),
join_traffic_source as (
    select 
        session_start_dims.*,
        sessions_traffic_sources.session_source,
        sessions_traffic_sources.session_medium,
        sessions_traffic_sources.session_campaign,
        sessions_traffic_sources.session_content,
        sessions_traffic_sources.session_term,
        sessions_traffic_sources.session_default_channel_grouping,
        sessions_traffic_sources.session_source_category
    from session_start_dims
    left join {{ref('stg_ga4__sessions_traffic_sources')}} sessions_traffic_sources using (session_key)
),
include_session_properties as (
    select 
        * 
    from join_traffic_source
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} using (session_key)
    {% endif %}
)

select * from include_session_properties