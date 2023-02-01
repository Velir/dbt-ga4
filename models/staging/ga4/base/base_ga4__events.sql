{% if var('static_incremental_days', false ) %}
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
-- if the multi-site datasets variable is set, then we union the models in the datasets variable
-- multi-site requires additional manual configuration beyond setting the datasets variable as detailed in the multi_site.md
{% if var('ga4_datasets') is defined  %}
    {% for ds in var('ga4_datasets') %}
        select
            *
        {%  if var('frequency', 'daily') == 'streaming' %}
            from {{ ref('base_ga4__multisite_events_intraday_'~ds) }}
        {% else %}
            from {{ ref('base_ga4__multisite_events_'~ds) }}
        {% endif %}
        {% if is_incremental() %}
            {% if var('static_incremental_days', false ) %}
                where event_date_dt in ({{ partitions_to_replace | join(',') }})
            {% else %}
                -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
                -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
                where event_date_dt >= _dbt_max_partition
            {% endif %}
        {% endif %}
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
{% else %}
-- If multi-site is not configured, then we get the base settings
--BigQuery does not cache wildcard queries that scan across sharded tables which means it's best to materialize the raw event data as a partitioned table so that future queries benefit from caching
with source as (
    select
    {{ base_select_source() }}
    {%  if var('frequency', 'daily') == 'streaming' %}
        from {{ source('ga4', 'events_intraday') }}
        where cast( _table_suffix as int64) >= {{var('start_date')}}
    {% else %}
        from {{ source('ga4', 'events') }}
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
    {{ base_select_renamed() }}
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, ga_session_id, event_name, event_timestamp, to_json_string(event_params)) = 1
{% endif %}