{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
            "field": "session_partition_date",
            "data_type": "date",
            "granularity": "day"
        },
        partitions = partitions_to_replace
    )
}}


with event_dimensions as 
(
    select 
        client_key,
        session_key,
        session_partition_key,
        event_date_dt as session_partition_date,
        event_timestamp,
        page_path,
        page_location,
        page_hostname,
        page_referrer,
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
        event_campaign,
        event_medium,
        event_source,
    from {{ref('stg_ga4__events')}}
    where event_name != 'first_visit' 
    and event_name != 'session_start'
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
)
,traffic_sources as (
    select 
        session_partition_key,
        session_source,
        session_medium,
        session_campaign,
        session_content,
        session_term,
        session_default_channel_grouping,
        session_source_category,
        -- last non-direct traffic sources
        last_non_direct_source,
        last_non_direct_medium,
        last_non_direct_campaign,
        last_non_direct_content,
        last_non_direct_term,
        last_non_direct_default_channel_grouping,
        last_non_direct_source_category
    from {{ref('stg_ga4__sessions_traffic_sources_last_non_direct_daily')}}
    where 1=1
    {% if is_incremental() %}
            and session_partition_date in ({{ partitions_to_replace | join(',') }})
    {% endif %} 
)
{% if var('derived_session_properties', false) %}
,session_properties as (
    select 
        * except (session_partition_date)
    from {{ref('stg_ga4__derived_session_properties_daily')}}
    where 1=1
    {% if is_incremental() %}
           and session_partition_date in ({{ partitions_to_replace | join(',') }})
    {% endif %}     
)
{% endif %}
,session_dimensions as 
(
    select    
        distinct -- Distinct call will, in effect, group by session_partition_key
        stream_id
        ,session_key
        ,session_partition_key
        ,session_partition_date
        ,FIRST_VALUE(event_timestamp IGNORE NULLS) OVER (session_partition_window) AS session_partition_start_timestamp
        ,FIRST_VALUE(page_path IGNORE NULLS) OVER (session_partition_window) AS landing_page_path
        ,FIRST_VALUE(page_location IGNORE NULLS) OVER (session_partition_window) AS landing_page_location
        ,FIRST_VALUE(page_hostname IGNORE NULLS) OVER (session_partition_window) AS landing_page_hostname
        ,FIRST_VALUE(page_referrer IGNORE NULLS) OVER (session_partition_window) AS referrer
        ,FIRST_VALUE(geo_continent IGNORE NULLS) OVER (session_partition_window) AS geo_continent
        ,FIRST_VALUE(geo_country IGNORE NULLS) OVER (session_partition_window) AS geo_country
        ,FIRST_VALUE(geo_region IGNORE NULLS) OVER (session_partition_window) AS geo_region
        ,FIRST_VALUE(geo_city IGNORE NULLS) OVER (session_partition_window) AS geo_city
        ,FIRST_VALUE(geo_sub_continent IGNORE NULLS) OVER (session_partition_window) AS geo_sub_continent
        ,FIRST_VALUE(geo_metro IGNORE NULLS) OVER (session_partition_window) AS geo_metro
        ,FIRST_VALUE(platform IGNORE NULLS) OVER (session_partition_window) AS platform
        ,FIRST_VALUE(device_category IGNORE NULLS) OVER (session_partition_window) AS device_category
        ,FIRST_VALUE(device_mobile_brand_name IGNORE NULLS) OVER (session_partition_window) AS device_mobile_brand_name
        ,FIRST_VALUE(device_mobile_model_name IGNORE NULLS) OVER (session_partition_window) AS device_mobile_model_name
        ,FIRST_VALUE(device_mobile_marketing_name IGNORE NULLS) OVER (session_partition_window) AS device_mobile_marketing_name
        ,FIRST_VALUE(device_mobile_os_hardware_model IGNORE NULLS) OVER (session_partition_window) AS device_mobile_os_hardware_model
        ,FIRST_VALUE(device_operating_system IGNORE NULLS) OVER (session_partition_window) AS device_operating_system
        ,FIRST_VALUE(device_operating_system_version IGNORE NULLS) OVER (session_partition_window) AS device_operating_system_version
        ,FIRST_VALUE(device_vendor_id IGNORE NULLS) OVER (session_partition_window) AS device_vendor_id
        ,FIRST_VALUE(device_advertising_id IGNORE NULLS) OVER (session_partition_window) AS device_advertising_id
        ,FIRST_VALUE(device_language IGNORE NULLS) OVER (session_partition_window) AS device_language
        ,FIRST_VALUE(device_is_limited_ad_tracking IGNORE NULLS) OVER (session_partition_window) AS device_is_limited_ad_tracking
        ,FIRST_VALUE(device_time_zone_offset_seconds IGNORE NULLS) OVER (session_partition_window) AS device_time_zone_offset_seconds
        ,FIRST_VALUE(device_browser IGNORE NULLS) OVER (session_partition_window) AS device_browser
        ,FIRST_VALUE(device_web_info_browser IGNORE NULLS) OVER (session_partition_window) AS device_web_info_browser
        ,FIRST_VALUE(device_web_info_browser_version IGNORE NULLS) OVER (session_partition_window) AS device_web_info_browser_version
        ,FIRST_VALUE(device_web_info_hostname IGNORE NULLS) OVER (session_partition_window) AS device_web_info_hostname
        ,FIRST_VALUE(event_campaign IGNORE NULLS) OVER (session_partition_window) AS user_campaign
        ,FIRST_VALUE(event_medium IGNORE NULLS) OVER (session_partition_window) AS user_medium
        ,FIRST_VALUE(event_source IGNORE NULLS) OVER (session_partition_window) AS user_source
        from event_dimensions
    WINDOW session_partition_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
,join_traffic_source as (
    select 
        session_dimensions.*,
        session_source,
        session_medium,
        session_campaign,
        session_content,
        session_term,
        session_default_channel_grouping,
        session_source_category,
        -- last non-direct traffic sources
        last_non_direct_source,
        last_non_direct_medium,
        last_non_direct_campaign,
        last_non_direct_content,
        last_non_direct_term,
        last_non_direct_default_channel_grouping,
        last_non_direct_source_category
    from session_dimensions
    left join traffic_sources sessions_traffic_sources using (session_partition_key)
)
,join_session_properties as (
    select 
        * 
    from join_traffic_source
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_partition_key
    left join session_properties using (session_partition_key)
    {% endif %}
)

-- Collapse 
select distinct * from join_session_properties