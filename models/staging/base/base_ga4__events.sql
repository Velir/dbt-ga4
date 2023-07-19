{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{% if var('property_ids', false) == false %}
    {% set relations_intraday = dbt_utils.get_relations_by_pattern(schema_pattern=var('dataset'), table_pattern='events_intraday_%', database=var('project')) %} 
{% endif %}
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

with source_daily as (
    select 
        {{ ga4.base_select_source() }}
        from {{ source('ga4', 'events') }}
        where _table_suffix not like '%intraday%'
        and cast( _table_suffix as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        and parse_date('%Y%m%d', left(_TABLE_SUFFIX, 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
),
-- Include intraday data if using a single-property configuration and the events_intraday_* table exists 
{% if var('property_ids', false) == false and relations_intraday|length > 0 %}
    source_intraday as (
        select 
            {{ ga4.base_select_source() }}
            from {{ source('ga4', 'events_intraday') }}
            where cast( _table_suffix as int64) >= {{var('start_date')}}
        {% if is_incremental() %}
            and parse_date('%Y%m%d', left(_TABLE_SUFFIX, 8)) in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    ),
    unioned as (
        select * from source_daily
            union all
        select * from source_intraday
    ),
    renamed as (
        select 
            {{ ga4.base_select_renamed() }}
        from unioned
    )
{% else %}
    renamed as (
        select 
            {{ ga4.base_select_renamed() }}
        from source_daily
    )
{% endif%}

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, session_id, event_name, event_timestamp, to_json_string(event_params)) = 1
