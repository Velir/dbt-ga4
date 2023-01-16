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
    )
}}
--BigQuery does not cache wildcard queries that scan across sharded tables which means it's best to materialize the raw event data as a partitioned table so that future queries benefit from caching
with source as (
    select
    {{ base_select_source() }}
    from ref('base_ga4__multisite.sql')
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            where parse_date('%Y%m%d', _TABLE_SUFFIX) in ({{ partitions_to_replace | join(',') }})
        {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
            -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            where parse_date('%Y%m%d',_TABLE_SUFFIX) >= _dbt_max_partition
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
