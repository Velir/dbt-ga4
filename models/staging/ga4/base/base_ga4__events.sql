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
        --event_dimensions, -- This is present in the sample dataset, but not the GA4 BQ export spec https://support.google.com/firebase/answer/7029846?hl=en
        ecommerce,
        items
    from {{ source('ga4', 'events') }}
    where _table_suffix not like '%intraday%' and -- TODO: support blending intraday events as well
        cast(_table_suffix as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        -- recalculate yesterday + today
        and parse_date('%Y%m%d',_TABLE_SUFFIX) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
)

select * from source
