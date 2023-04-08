{% if var('static_incremental_days', false ) %}
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
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            tags = ["incremental"],
            partition_by={
                "field": "session_partition_date",
                "data_type": "date",
                "granularity": "day"
            }
        )
    }}
{% endif %}

with event_dimensions as 
(
    select 
        session_key,
        session_partition_key,
        event_date_dt as session_partition_date,
        event_timestamp as session_partition_start_timestamp,
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
        session_number,
        session_number = 1 as is_first_session,
        user_campaign,
        user_medium,
        user_source,
    from {{ref('stg_ga4__events')}}
    where event_name != 'first_visit' 
    and event_name != 'session_start'
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            and event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
),
session_dimensions as 
(
    select    
        session_key,
        session_partition_key,
        session_partition_date,
        session_partition_start_timestamp,
        FIRST_VALUE(page_path) IGNORE NULLS) OVER (session_partition_window) AS landing_page_path,,
        landing_page,
        landing_page_hostname,
        landing_page_referrer,
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
        user_campaign,
        user_medium,
        user_source,
        
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_medium, '(none)') END) IGNORE NULLS) OVER (session_partition_window), '(none)') AS session_medium,
        from set_default_channel_grouping
    WINDOW session_partition_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)


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
    left join {{ref('stg_ga4__sessions_traffic_sources_daily')}} sessions_traffic_sources using (session_partition_key)
),
include_session_properties as (
    select 
        * 
    from join_traffic_source
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties_daily')}} using (session_partition_key)
    {% endif %}
)

select * from include_session_properties