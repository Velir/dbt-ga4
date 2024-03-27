{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{{
    config(
        enabled= var('conversion_events', false) != false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
            "field": "session_partition_date",
            "data_type": "date",
            "granularity": "day"
        },
        partitions = partitions_to_replace
    )
}}



with event_counts as (
    select 
        session_key,
        session_partition_key,
        min(event_date_dt) as session_partition_date -- The date of this session partition
        {% for ce in var('conversion_events',[]) %}
        , countif(event_name = '{{ce}}') as {{ce}}_count
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    where 1=1
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    group by 1,2
)

select * from event_counts
