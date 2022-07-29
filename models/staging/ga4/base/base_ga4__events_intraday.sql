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
        {{ ga4.unnest_key('event_params', 'ga_session_id', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'page_location') }},
        {{ ga4.unnest_key('event_params', 'ga_session_number',  'int_value') }},
        (case when (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" then 1 end) as session_engaged,
        {{ ga4.unnest_key('event_params', 'engagement_time_msec', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'page_title') }},
        {{ ga4.unnest_key('event_params', 'page_referrer') }},
        {{ ga4.unnest_key('event_params', 'source') }},
        {{ ga4.unnest_key('event_params', 'medium') }},
        {{ ga4.unnest_key('event_params', 'campaign') }},
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