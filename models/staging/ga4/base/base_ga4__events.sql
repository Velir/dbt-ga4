-- If jobs are running daily, it may make sense to avoid the scanning necessary to determine the max partition date. Instead, a static incremental range can be set and this data will be overwritten/inserted at every incremental run.

{% set partitions_to_replace = [
  'current_date()',
  'date_sub(current_date(), interval 1 day)'
] %}

--BigQuery does not cache wildcard queries that scan across sharded tables which means it's best to materialize the raw event data as a partitioned table so that future queries benefit from caching
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
        "field": "event_date_dt",
        "data_type": "date",
        }
    )
}}

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
        user_pseudo_id as client_id,
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
        items
    from {{ source('ga4', 'events') }}
    where _table_suffix not like '%intraday%' and -- TODO: support blending intraday events as well
        cast(_table_suffix as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        -- recalculate yesterday + today
        and parse_date('%Y%m%d',_TABLE_SUFFIX) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
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
        client_id,
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
    from source
),
-- Add unique keys for sessions and events
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
