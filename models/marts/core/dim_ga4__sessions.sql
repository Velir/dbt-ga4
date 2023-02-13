-- Dimension table for sessions based on the session_start event.
{% if is_incremental %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            schema = 'analytics',
            partition_by={
                "field": "session_start_date",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            schema = 'analytics',
            partition_by={
                "field": "session_start_date",
                "data_type": "date",
            },
        )
    }}
{% endif %}

with session_start_dims as (
    select 
        session_key,
        ga_session_number,
        event_date_dt as session_start_date,
        page_location as landing_page,
        page_hostname as landing_page_hostname,
        mv_region,
        geo_continent,
        geo_country,
        geo_region,
        geo_city,
        geo_sub_continent,
        geo_metro,
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
        row_number() over (partition by session_key order by session_event_number asc) as row_num
        {% if var("dim_ga4__sessions_custom_parameters", "none") != "none" %}
            {{ ga4.mart_custom_parameters( var("dim_ga4__sessions_custom_parameters") )}}
        {% endif %}
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
),
-- Arbitrarily pull the first session_start event to remove duplicates
remove_dupes as 
(
    select * from session_start_dims
    where row_num = 1
),
join_traffic_source as (
    select 
        *
    from remove_dupes
    left join {{ref('stg_ga4__sessions_traffic_sources')}} using (session_key)
)

select * from join_traffic_source