{% set ds = 'ga4_' %} -- This should match the *numeric* portion of the GA4 source dataset prefixed with 'ga4_' and needs to be configured separately for each dataset
{% if var('ga4_datasets', false)  == false %}
    {{ config(enabled = 'false') }}
{% elif var('static_incremental_days', false ) %}
    {% set partitions_to_replace = ['current_date'] %}
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
        )
    }}
{% endif %}
with source as (
    select
    {{ ga4.base_select_source() }}
    {%  if var('frequency', 'daily') == 'streaming' %}
        from {{ source(ds, 'events_intraday') }}
        where cast( _table_suffix as int64) >= {{var('start_date')}}
    {% else %}
        from {{ source(ds, 'events') }}
        where _table_suffix not like '%intraday%'
        and cast( _table_suffix as int64) >= {{var('start_date')}}
    {% endif %}
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            and parse_date('%Y%m%d', _TABLE_SUFFIX) in ({{ partitions_to_replace | join(',') }})
        {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
            -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            and parse_date('%Y%m%d',_TABLE_SUFFIX) >= _dbt_max_partition
        {% endif %}
    {% endif %}
),
renamed as (
    select
    {{ ga4.base_select_renamed() }}
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, ga_session_id, event_name, event_timestamp, to_json_string(event_params)) = 1