{% set date_range = (range(
    (end_date|date).toordinal() - (start_date|date).toordinal() + 1
)) %}

{% for i in date_range %}
    {% set partition_date = (start_date|date + timedelta(days=i)).strftime('%Y-%m-%d') %}
    {% set partitions_to_replace = partitions_to_replace.append(partition_date) %}
    {{ log("Adding partition: " ~ partition_date, info=True) }}
{% endfor %}

{{ log("Partitions to replace: " ~ partitions_to_replace | join(', '), info=True) }}


{{
    config(
        pre_hook="{{ ga4.combine_property_data() }}" if var('combined_dataset', false) else "",
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions = partitions_to_replace,
        cluster_by=['event_name', 'stream_id']
    )
}}

with source as (
    select
        {{ ga4.base_select_source() }}
    from {{ source('ga4', 'events') }}
    where cast(left(replace(_table_suffix, 'intraday_', ''), 8) as int64) >= {{var('start_date')}}
    {% if var('end_date') is not none %}
        and cast(left(replace(_table_suffix, 'intraday_', ''), 8) as int64) <= {{ var('end_date')}}
    {% endif %}
    {% if is_incremental() and var('end_date') is none %}
        and parse_date('%Y%m%d', left(replace(_table_suffix, 'intraday_', ''), 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
),
renamed as (
    select
        {{ ga4.base_select_renamed() }}
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, session_id, event_name, event_timestamp, to_json_string(ARRAY(SELECT params FROM UNNEST(event_params) AS params ORDER BY key))) = 1
