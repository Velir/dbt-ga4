-- Break out struct data types and include ga_session_id in all records

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
        items 
    from {{ref('base_ga4__events')}}
),
include_session_id as (
    select 
        renamed.*,
        params.value.int_value as ga_session_id,
        md5(CONCAT(stream_id, client_id, cast(params.value.int_value as STRING))) as session_key -- Surrogate key to determine unique session across streams and users
    from renamed,
        UNNEST(event_params) as params
    where params.key = 'ga_session_id'
)

select * from include_session_id