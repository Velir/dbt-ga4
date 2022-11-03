-- incremental with no key combines with where user_key not in {{this}} to only add sessions with new user_keys
{{ config(
    materialized= 'incremental',
)
}}
with first_events as (  -- get only the first event in the partions being updated
    select distinct
        first_value(first_event_key) over (partition by user_key order by first_event_timestamp asc) as event_key
    from {{ ref('stg_ga4__sessions_first_last_events') }}
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
    device_mobile_os_hardware_model as first_device_mobile_os_hardware_model,
    device_operating_system as first_device_operating_system,
    device_operating_system_version as first_device_operating_system_version,
    device_language as first_device_language,
    device_is_limited_ad_tracking as first_device_is_limited_ad_tracking,
    device_web_info_browser as first_device_web_info_browser,
    device_web_info_browser_version as first_device_web_info_browser_version,
    device_web_info_hostname as first_device_web_info_hostname,
    traffic_source_name as first_traffic_source_name,
    traffic_source_medium as first_traffic_source_medium,
    traffic_source_source as first_traffic_source_source,
    page_referrer as first_page_referrer,
    page_location as first_page_location,
    page_hostname as first_page_hostname
    {% if var("stg_ga4__first_sessions_custom_parameters", "none") != "none" %}
        {{ ga4.mart_custom_parameters( var("stg_ga4__first_sessions_custom_parameters"), 'first_' )}}
    {% endif %}
from {{ ref('stg_ga4__events') }}
right join first_events using (event_key) -- right join to only get first_events in session
{% if is_incremental() %}
    where user_key not in (select user_key from {{ this }})
{% endif %}