{% macro base_select_source() %}
    {{ return(adapter.dispatch('base_select_source', 'ga4')()) }}
{% endmacro %}

{% macro default__base_select_source() %}
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
{% endmacro %}

{% macro base_select_renamed() %}
    {{ return(adapter.dispatch('base_select_renamed', 'ga4')()) }}
{% endmacro %}

{% macro default__base_select_renamed() %}
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
    traffic_source.name as user_campaign,
    traffic_source.medium as user_medium,
    traffic_source.source as user_source,
    stream_id,
    platform,
    ecommerce,
    items,
    {{ ga4.unnest_key('event_params', 'ga_session_id', 'int_value', 'session_id') }},
    {{ ga4.unnest_key('event_params', 'page_location') }},
    {{ ga4.unnest_key('event_params', 'ga_session_number',  'int_value', 'session_number') }},
    COALESCE(
        (SELECT value.int_value FROM unnest(event_params) WHERE key = "session_engaged"),
        (CASE WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" THEN 1 END)
    ) as session_engaged,
    {{ ga4.unnest_key('event_params', 'engagement_time_msec', 'int_value') }},
    {{ ga4.unnest_key('event_params', 'page_title') }},
    {{ ga4.unnest_key('event_params', 'page_referrer') }},
    {{ ga4.unnest_key('event_params', 'source', 'lower_string_value', 'event_source') }},
    {{ ga4.unnest_key('event_params', 'medium', 'lower_string_value', 'event_medium') }},
    {{ ga4.unnest_key('event_params', 'campaign', 'lower_string_value', 'event_campaign') }},
    {{ ga4.unnest_key('event_params', 'content', 'lower_string_value', 'event_content') }},
    {{ ga4.unnest_key('event_params', 'term', 'lower_string_value', 'event_term') }},
    CASE 
        WHEN event_name = 'page_view' THEN 1
        ELSE 0
    END AS is_page_view,
    CASE 
        WHEN event_name = 'purchase' THEN 1
        ELSE 0
    END AS is_purchase
{% endmacro %}