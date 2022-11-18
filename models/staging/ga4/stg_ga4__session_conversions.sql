{{ config(
    enabled= var('conversion_events', false) != false,
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    tags = ["incremental"],
    partition_by={
    "field": "session_start_date",
    "data_type": "date",
    "granularity": "day"
    }
) }}


with event_counts as (
    select 
        session_key,
        session_start_date
        {% for ce in var('conversion_events',[]) %}
        , countif(event_name = '{{ce}}') as {{ce}}_count
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    group by session_key
)

select * from event_counts
