{% if var('use_static_partition', false ) ==  true %}
   {% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval ' + var('static_partition_lower_bound')|string + ' day)'
    ] %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
            "field": "event_date_dt",
            "data_type": "date",
            },
            partitions = partitions_to_replace
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
            }
        )
    }}
{% endif %}

with unioned_datasets as (

{% for dataset in var('datasets') %}
    select *,_TABLE_SUFFIX as event_table_suffix, '{{dataset}}' as ga4_dataset from `{{dataset}}.events_*`
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}

),

source as (
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
        items,
        ga4_dataset
    from unioned_datasets
    where event_table_suffix not like '%intraday%' -- intraday events are supported through the project variable: include_intraday_events
    and cast(event_table_suffix as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        {% if var('use_static_partition', false ) ==  true %}
            and parse_date('%Y%m%d', event_date) in ({{ partitions_to_replace | join(',') }})
        {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
            -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            and parse_date('%Y%m%d',event_table_suffix) >= _dbt_max_partition
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
        ga4_dataset,
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
        END AS is_purchase,
    from source
)

select
    *
from renamed