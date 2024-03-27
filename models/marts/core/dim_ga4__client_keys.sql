{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['client_key'],
        tags = ["incremental"],
        partition_by={
            "field": "last_seen_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        on_schema_change = 'sync_all_columns',
        merge_update_columns = [
            'last_geo_continent',
            'last_geo_country',
            'last_geo_region',
            'last_geo_city',
            'last_geo_sub_continent',
            'last_geo_metro',
            'last_device_category',
            'last_device_mobile_brand_name',
            'last_device_mobile_model_name',
            'last_device_mobile_marketing_name',
            'last_device_mobile_os_hardware_model',
            'last_device_operating_system',
            'last_device_operating_system_version',
            'last_device_vendor_id',
            'last_device_advertising_id',
            'last_device_language',
            'last_device_is_limited_ad_tracking',
            'last_device_time_zone_offset_seconds',
            'last_device_browser',
            'last_device_browser_version',
            'last_device_web_info_browser',
            'last_device_web_info_browser_version',
            'last_device_web_info_hostname',
            'last_user_campaign',
            'last_user_medium',
            'last_user_source',
            'last_seen_at',
            'last_page_location',
            'last_page_hostname',
            'last_page_referrer',
        ],  
    )
}}

with include_first_last_events as (
    select 
        *
    from {{ref('stg_ga4__client_key_first_last_events')}}
    {% if is_incremental() %}
      where date(last_seen_at) >= date_sub(current_date, interval {{var('static_incremental_days',3) | int}} day)
    {% endif %}
),
include_first_last_page_views as (
    select 
        include_first_last_events.*,
        first_last_page_views.first_page_location,
        first_last_page_views.first_page_hostname,
        first_last_page_views.first_page_referrer,
        first_last_page_views.last_page_location,
        first_last_page_views.last_page_hostname,
        first_last_page_views.last_page_referrer,
    from include_first_last_events 
    left join {{ref('stg_ga4__client_key_first_last_pageviews')}} as first_last_page_views using (client_key)
    {% if is_incremental() %}
      where date(first_last_page_views.last_seen_at) >= date_sub(current_date, interval {{var('static_incremental_days',3) | int}} day)
    {% endif %}
),
include_user_properties as (

select * from include_first_last_page_views
{% if var('derived_user_properties', false) %}
-- If derived user properties have been assigned as variables, join them on the client_key
left join {{ref('stg_ga4__derived_user_properties')}} using (client_key)
{% endif %}
{% if var('user_properties', false) %}
-- If user properties have been assigned as variables, join them on the client_key
left join {{ref('stg_ga4__user_properties')}} using (client_key)
{% endif %}

)

select * from include_user_properties