-- Break out struct data types and add unique keys for sessions and events

with renamed as (
    select 
        event_date_dt,
        event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        client_id,
        privacy_info,
        user_properties,
        user_first_touch_timestamp,
        user_ltv,
        device,
        geo.continent as geo_continent,
        geo.sub_continent as geo_sub_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city,
        geo.metro as geo_metro,
        app_info,
        traffic_source.name as traffic_source_campaign_name,
        traffic_source.source as traffic_source_source,
        traffic_source.medium as traffic_source_medium,
        stream_id,
        platform,
        --event_dimensions, -- This is present in the sample dataset, but not the GA4 BQ export spec https://support.google.com/firebase/answer/7029846?hl=en
        ecommerce,
        items,
        {{ unnest_key('event_params', 'ga_session_id', 'int_value') }},
        {{ unnest_key('event_params', 'page_location') }},
        {{ unnest_key('event_params', 'ga_session_number',  'int_value') }}
    from {{ref('base_ga4__events')}}
),
include_session_key as (
    select 
        renamed.*,
        md5(CONCAT(stream_id, client_id, cast(ga_session_id as STRING))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from renamed
),
include_event_key as (
    select 
        include_session_key.*,
        md5(CONCAT(event_name, CAST(event_timestamp as STRING), CAST(TO_BASE64(session_key) as STRING))) as event_key -- Surrogate key for unique events
    from include_session_key
),
enrich_params as (
    select 
        include_event_key.*,
        {{extract_hostname_from_url('page_location')}} as page_hostname,
        case
            when ga_session_number = 1 then TRUE
            else FALSE
        end as is_new_user
    from include_event_key
)

select * from enrich_params