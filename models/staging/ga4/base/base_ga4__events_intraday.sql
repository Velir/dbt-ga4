{{ ga4.incremental_header() }}

{% if var('ga4_datasets') is defined  %}
    {% for ds in var('ga4_datasets') %}
        select
            *
        from {{ ref('base_ga4__multisite_events_intraday_'~ds) }}
        -- On sites configured for both streaming and batch, the intraday tables remain on days that go over the daily 1,000,000 event limit
        -- To avoid reprocessing those tables, we need the below logic
        {% if is_incremental() %}
            {% if var('static_incremental_days', false ) %}
                where event_date_dt in ({{ partitions_to_replace | join(',') }})
            {% else %}
                where event_date_dt  >= DATE_SUB(_dbt_max_partition, INTERVAL 1 DAY)
            {% endif %}
        {% endif %}
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
{% else %}
with source as (
    select
        *
    from {{ source('ga4', 'events_intraday') }}
    where cast( _table_suffix as int64) >= {{var('start_date')}}
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