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

with events as (
    select 
        1 as event_count, 
        session_key, 
        event_name,
        event_date_dt
    from {{ref('stg_ga4__events')}}
    -- Give 1 extra day to ensure we beging aggregation at the start of a session
    where session_key is not null
    {% if is_incremental() %}
        and event_date_dt >= DATE_SUB(_dbt_max_partition, INTERVAL 1 DAY)
    {% endif %}
     
),
sessions as (
    select 
        session_key, 
        min(event_date_dt) as session_start_date
    from {{ref('stg_ga4__events')}}
    -- Give 1 extra day to ensure we beging aggregation at the start of a session
    where session_key is not null
    {% if is_incremental() %}
        and event_date_dt >= DATE_SUB(_dbt_max_partition, INTERVAL 1 DAY)
    {% endif %}
    group by 1
)
-- For loop that creates 1 cte per conversions, grouped by session key
{% for ce in var('conversion_events',[]) %}
,
conversion_{{ce}} as (
    select
        session_key,
        sum(event_count) as conversion_count
    from events
    where event_name = '{{ce}}'
    group by session_key
)

{% endfor %}
,
-- Finally, join in each conversion count as a new column
final_pivot as (
    select 
        session_key,
        session_start_date
        {% for ce in var('conversion_events',[]) %}
        , ifnull(conversion_{{ce}}.conversion_count,0) as {{ce}}_count
        {% endfor %}
    from sessions
    {% for ce in var('conversion_events',[]) %}
    left join conversion_{{ce}} using (session_key)
    {% endfor %}
)

select * from final_pivot

