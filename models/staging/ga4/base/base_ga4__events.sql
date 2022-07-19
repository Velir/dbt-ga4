{% if var('static_incremental_days', false ) %}
    {% set partitions_to_replace = [] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
            labels = {'static_incremental_days': 'is_set'}
        )
    }}
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            labels = {'static_incremental_days': 'not_set'}
        )
    }}
{% endif %}
--BigQuery does not cache wildcard queries that scan across sharded tables which means it's best to materialize the raw event data as a partitioned table so that future queries benefit from caching
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
    from {{ source('ga4', 'events') }}
    where _table_suffix not like '%intraday%' -- intraday events are supported through the project variable: include_intraday_events
    and cast(_table_suffix as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            and parse_date('%Y%m%d', event_date) in ({{ partitions_to_replace | join(',') }})
            and platform not in ("static_incremental_days is set")
        {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
            -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            and parse_date('%Y%m%d',_TABLE_SUFFIX) >= _dbt_max_partition
            and platform not in ("static_incremental_days is not set")
        {% endif %}
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
        END AS is_purchase,
    from source
)

select
    *
from renamed