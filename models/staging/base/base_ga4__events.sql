{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}

{{
    config(
        pre_hook="{{ ga4.combine_property_data() }}" if var('property_ids', false) else "",
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions = partitions_to_replace,
        cluster_by=['event_name']
    )
}}

with source as (
    select 
        {{ ga4.base_select_source() }}
        from {{ source('ga4', 'events') }}
        where cast( replace(_table_suffix, 'intraday_', '') as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        and parse_date('%Y%m%d', left( replace(_table_suffix, 'intraday_', ''), 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
),
renamed as (
    select 
        {{ ga4.base_select_renamed() }}
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, session_id, event_name, event_timestamp, to_json_string(ARRAY(SELECT params FROM UNNEST(event_params) AS params ORDER BY key))) = 1
