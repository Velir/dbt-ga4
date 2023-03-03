{{ config(
    enabled= var('conversion_events', false) != false,
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    tags = ["incremental"],
    partition_by={
    "field": "session_partition_date",
    "data_type": "date",
    "granularity": "day"
    }
) }}


with event_counts as (
    select 
        session_key,
        session_partition_key,
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        min(event_date_dt) as session_partition_date -- The date of this partition, not necessarily the session start date given that sessions can span multiple days
        {% for ce in var('conversion_events',[]) %}
        , countif(event_name = '{{ce}}') as {{ce}}_count
        {% endfor %}
        {% for ce in var('conversion_values', []) %}
            , SUM(IF(event_name = '{{ce}}', CAST( {{ce}} as FLOAT64), 0)) as {{ce}}_value
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    LEFT JOIN  {{ref("stg_ga4__derived_session_properties")}} USING (session_key)
    group by 1,2,3,4,5
)

select * from event_counts
