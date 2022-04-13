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
        geo,
        app_info,
        {{ unpack_struct('traffic_source', ['name', 'source', 'medium']) }},
        stream_id,
        platform,
        --event_dimensions, -- This is present in the sample dataset, but not the GA4 BQ export spec https://support.google.com/firebase/answer/7029846?hl=en
        ecommerce,
        items,
        {{ unnest_key('event_params', 'ga_session_id', 'int_value') }},
        {{ unnest_key('event_params', 'page_location') }},
        {{ unnest_key('event_params', 'ga_session_number',  'int_value') }},
        {{ unnest_key('event_params', 'session_engaged', 'int_value') }},
        CASE 
            WHEN event_name = 'page_view' THEN 1
            ELSE 0
        END AS is_page_view,
        CASE 
            WHEN event_name = 'purchase' THEN 1
            ELSE 0
        END AS is_purchase
    from {{ref('base_ga4__events')}}
),
include_session_key as (
    select 
        renamed.*,
        md5(CONCAT(stream_id, client_id, cast(ga_session_id as STRING))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from renamed
),
include_event_number as (
    select include_session_key.*,
        row_number() over(partition by session_key) as session_event_number -- Number each event within a session to help generate a uniqu event key
    from include_session_key
),
include_event_key as (
    select 
        include_event_number.*,
        md5(CONCAT(CAST(TO_BASE64(session_key) as STRING), CAST(session_event_number as STRING))) as event_key -- Surrogate key for unique events
    from include_event_number
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