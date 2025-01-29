{% set partitions_to_replace = ['current_date'] %}
{% for i in range(env_var('GA4_INCREMENTAL_DAYS')|int if env_var('GA4_INCREMENTAL_DAYS', false) else var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{{
    config(
        pre_hook="{{ ga4.combine_property_data() }}" if var('combined_dataset', false) else "",
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        enabled=false,
        partition_by={
            "field": "occurrence_date",
            "data_type": "date",
        },
        partitions = partitions_to_replace,
    )
}}

with source as (
    select
        user_id
        {{ ga4.base_select_usr_source() }}
    from {{ source('ga4', 'users') }}
    {% if is_incremental() %}
        where parse_date('%Y%m%d', left(_table_suffix, 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
)

select * from source
