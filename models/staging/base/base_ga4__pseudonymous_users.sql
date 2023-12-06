{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}

{{
    config(
        pre_hook="{{ ga4.combine_property_data() }}" if var('property_ids', false) else "", -- need to replicate combine_property_data for user models
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "occurrence_date",
            "data_type": "date",
        },
        partitions = partitions_to_replace
    )
}}

with source as (
    select
        pseudo_user_id
        , stream_id
        {{ ga4.base_select_user_source() }}
    from {{source('ga4', 'pseudo_users')}}
)
select * from source