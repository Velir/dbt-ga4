{% if var('static_incremental_days', false ) %}
    {% set partitions_to_replace = [] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            enabled= var('conversion_events', false) != false,
            tags=["incremental"],
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}
    {{
        config(
            enabled= var('conversion_events', false) != false,
            materialized = 'incremental',
            tags=["incremental"],
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}

with events as (
    select 
        1 as event_count,
        event_date_dt,
        event_name,
        page_key
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            where event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
),
pk as (
    select
        distinct page_key,event_date_dt
    from events
)
-- For loop that creates 1 cte per conversions, grouped by page_key
{% for ce in var('conversion_events',[]) %}
,
conversion_{{ce}} as (
    select
        page_key,
        sum(event_count) as conversion_count,
    from events
    where event_name = '{{ce}}'
    group by 1
)
{% endfor %}
,
-- Finally, join in each conversion count as a new column
final_pivot as (
    select 
        pk.page_key,
        pk.event_date_dt
        {% for ce in var('conversion_events',[]) %}
        , ifnull(conversion_{{ce}}.conversion_count,0) as {{ce}}_count
        {% endfor %}
    from pk
    {% for ce in var('conversion_events',[]) %}
    left join conversion_{{ce}} using (page_key)
    {% endfor %}
)

select * from final_pivot

