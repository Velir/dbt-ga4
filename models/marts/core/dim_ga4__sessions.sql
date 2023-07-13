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
with session_first_event as 
(
    select
        session_key,
        user_key,
        ga_session_number,
        event_date_dt as session_start_date,
        event_timestamp as session_start_timestamp,
        page_location as landing_page,
        page_hostname as landing_page_hostname,
        page_referrer as session_referrer,
        original_page_referrer as original_session_referrer,
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
        device_language,
        device_is_limited_ad_tracking,
        device_web_info_browser,
        device_web_info_browser_version,
        device_web_info_hostname,
        traffic_source_name as user_campaign,
        traffic_source_medium as user_medium,
        traffic_source_source as user_source,
    from {{ref('stg_ga4__events')}}
    where event_name != 'first_visit' 
    and event_name != 'session_start'
    {% if is_incremental() %}
        {% if var('static_incremental_days', 1 ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    qualify row_number() over(partition by session_key order by event_timestamp) = 1
),
session_metrics as (
    select
        session_key,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by 1
),

join_traffic_source as (
    select 
        session_first_event.*,
        session_metrics.* except(session_key),
        session_source,
        session_medium,
        session_source_category,
        session_campaign,
        session_content,
        session_term,
        session_channel,
        mv_author_session_status,
        last_non_direct_source,
        last_non_direct_medium,
        last_non_direct_source_category,
        last_non_direct_campaign,
        last_non_direct_content,
        last_non_direct_term,
        last_non_direct_channel
    from session_first_event
    left join session_metrics using (session_key)
    left join {{ref('stg_ga4__sessions_traffic_sources')}} using (session_key)
    left join {{ ref ('stg_ga4__last_non_direct_attribution')}} using (session_key)
)
{% if var('conversion_events',false) == false %}
    select * from join_traffic_source
{% else %}
,
join_conversions as (
    select 
        *
    from join_traffic_source
    left join {{ref('stg_ga4__session_conversions')}} using (session_key)
)
select * from join_conversions
{% endif %}