{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% do partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
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
        client_key
        , session_key
        , session_partition_key
        , event_date_dt as session_partition_date
        , event_timestamp
        , page_path
        , page_location
        , page_hostname
        , page_referrer
        , geo_continent
        , geo_country
        , geo_region
        , geo_city
        , geo_sub_continent
        , geo_metro
        , stream_id
        , platform
        , device_category
        , device_mobile_brand_name
        , device_mobile_model_name
        , device_mobile_marketing_name
        , device_mobile_os_hardware_model
        , device_operating_system
        , device_operating_system_version
        , device_vendor_id
        , device_advertising_id
        , device_language
        , device_is_limited_ad_tracking
        , device_time_zone_offset_seconds
        , device_browser
        , device_web_info_browser
        , device_web_info_browser_version
        , device_web_info_hostname
        , user_campaign
        , user_medium
        , user_source
    from {{ref('stg_ga4__events')}}
    where event_name != 'first_visit' 
    and event_name != 'session_start'
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
)
,traffic_sources as (
    select
        session_partition_key
        , session_source
        , session_medium
        , session_campaign
        , session_content
        , session_term
        , session_default_channel_grouping
        , session_source_category
        -- last non-direct traffic sources
        , last_non_direct_source
        , last_non_direct_medium
        , last_non_direct_campaign
        , last_non_direct_content
        , last_non_direct_term
        , last_non_direct_default_channel_grouping
        , last_non_direct_source_category
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
        stream_id
        , session_key
        , session_partition_key
        , session_partition_date
        , event_timestamp as session_partition_start_timestamp
        , page_path as landing_page_path
        , page_location as landing_page_location
        , page_hostname as landing_page_hostname
        , page_referrer as referrer
        , geo_continent
        , geo_country
        , geo_region
        , geo_city
        , geo_sub_continent
        , geo_metro
        , platform
        , device_category
        , device_mobile_brand_name
        , device_mobile_model_name
        , device_mobile_marketing_name
        , device_mobile_os_hardware_model
        , device_operating_system
        , device_operating_system_version
        , device_vendor_id
        , device_advertising_id
        , device_language
        , device_is_limited_ad_tracking
        , device_time_zone_offset_seconds
        , device_browser
        , device_web_info_browser
        , device_web_info_browser_version
        , device_web_info_hostname
        , user_campaign
        , user_medium
        , user_source
    from event_dimensions
    qualify row_number() over (partition by session_partition_key order by event_timestamp asc) = 1
)
,join_traffic_source as (
    select
        session_dimensions.*
        , session_source
        , session_medium
        , session_campaign
        , session_content
        , session_term
        , session_default_channel_grouping
        , session_source_category
        -- last non-direct traffic sources
        , last_non_direct_source
        , last_non_direct_medium
        , last_non_direct_campaign
        , last_non_direct_content
        , last_non_direct_term
        , last_non_direct_default_channel_grouping
        , last_non_direct_source_category
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

select * from join_session_properties