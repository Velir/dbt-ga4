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
        -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
        -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
        and parse_date('%Y%m%d',_TABLE_SUFFIX) >= _dbt_max_partition 
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
        {{ unnest_key('event_params', 'page_title') }},
        {{ unnest_key('event_params', 'page_referrer') }},
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