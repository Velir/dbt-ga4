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
    select * 
    from {{ source('ga4', 'events') }}
    where cast(_table_suffix as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found
        -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
        and parse_date('%Y%m%d',_TABLE_SUFFIX) >= _dbt_max_partition 
    {% endif %}
),
renamed as (
    select 
        parse_date('%Y%m%d',event_date) as event_date_dt, 
        * 
        EXCEPT (event_date) -- remove event date to ensure usage of event_date_dt which is partitioned
    from source
)

select * from renamed
