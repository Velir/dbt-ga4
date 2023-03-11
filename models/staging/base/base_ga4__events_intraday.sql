{% if var('frequency', 'daily') == 'daily+streaming' %}
    {{ config(
    enabled = true 
    ) }}
{% else %}
    {{ config(
    enabled = false 
    ) }}
{% endif %}

-- This model will be unioned with `base_ga4__events` which means that their columns must match
with source as (
    select 
        parse_date('%Y%m%d',event_date) as event_date_dt,
        event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,
        user_first_touch_timestamp,
        user_ltv,
        device,
        geo,
        app_info,
        traffic_source,
        stream_id,
        platform,
        ecommerce,
        items,
    from {{ source('ga4', 'events_intraday') }}
),
renamed as (
    select 
        event_date_dt,
        event_timestamp,
        lower(replace(trim(event_name), " ", "_")) as event_name, -- Clean up all event names to be snake cased
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info.analytics_storage as privacy_info_analytics_storage,
        privacy_info.ads_storage as privacy_info_ads_storage,
        privacy_info.uses_transient_token as privacy_info_uses_transient_token,
        user_properties,
        user_first_touch_timestamp,
        user_ltv.revenue as user_ltv_revenue,
        user_ltv.currency as user_ltv_currency,
        device.category as device_category,
        device.mobile_brand_name as device_mobile_brand_name,
        device.mobile_model_name as device_mobile_model_name,
        device.mobile_marketing_name as device_mobile_marketing_name,
        device.mobile_os_hardware_model as device_mobile_os_hardware_model,
        device.operating_system as device_operating_system,
        device.operating_system_version as device_operating_system_version,
        device.vendor_id as device_vendor_id,
        device.advertising_id as device_advertising_id,
        device.language as device_language,
        device.is_limited_ad_tracking as device_is_limited_ad_tracking,
        device.time_zone_offset_seconds as device_time_zone_offset_seconds,
        device.browser as device_browser,
        device.browser_version as device_browser_version,
        device.web_info.browser as device_web_info_browser,
        device.web_info.browser_version as device_web_info_browser_version,
        device.web_info.hostname as device_web_info_hostname,
        geo.continent as geo_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city,
        geo.sub_continent as geo_sub_continent,
        geo.metro as geo_metro,
        app_info.id as app_info_id,
        app_info.version as app_info_version,
        app_info.install_store as app_info_install_store,
        app_info.firebase_app_id as app_info_firebase_app_id,
        app_info.install_source as app_info_install_source,
        traffic_source.name as traffic_source_name,
        traffic_source.medium as traffic_source_medium,
        traffic_source.source as traffic_source_source,
        stream_id,
        platform,
        ecommerce,
        items,
        {{ ga4.unnest_key('event_params', 'ga_session_id', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'page_location') }},
        COALESCE(
            (SELECT value.int_value FROM unnest(event_params) WHERE key = "session_engaged"),
            (CASE WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" THEN 1 END)
        ) as session_engaged,
        {{ ga4.unnest_key('event_params', 'ga_session_number',  'int_value', 'session_number') }},
        {{ ga4.unnest_key('event_params', 'engagement_time_msec', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'page_title') }},
        {{ ga4.unnest_key('event_params', 'page_referrer') }},
        {{ ga4.unnest_key('event_params', 'source') }},
        {{ ga4.unnest_key('event_params', 'medium') }},
        {{ ga4.unnest_key('event_params', 'campaign') }},
        {{ ga4.unnest_key('event_params', 'content') }},
        {{ ga4.unnest_key('event_params', 'term') }},
        CASE 
            WHEN event_name = 'page_view' THEN 1
            ELSE 0
        END AS is_page_view,
        CASE 
            WHEN event_name = 'purchase' THEN 1
            ELSE 0
        END AS is_purchase
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, ga_session_id, event_name, event_timestamp, to_json_string(event_params)) = 1
