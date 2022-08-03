{{ config(
  enabled= var('conversion_events', false) != false
) }}
with events as (
    select 
        1 as event_count,
        event_name,
        event_date_dt,
        extract( hour from (select  timestamp_micros(event_timestamp))) as hour,
        page_location
    from {{ref('stg_ga4__events')}}
),
pk as (
    select
        distinct (concat( cast(event_date_dt as string), cast(hour as string), page_location )) as page_key,
    from events
)
-- For loop that creates 1 cte per conversions, grouped by page_location
{% for ce in var('conversion_events',[]) %}
,
conversion_{{ce}} as (
    select
        distinct (concat( cast(event_date_dt as string), cast(hour as string), page_location )) as page_key,
        sum(event_count) as conversion_count,
    from events
    where event_name = '{{ce}}'
    group by page_key
)

{% endfor %}

,
-- Finally, join in each conversion count as a new column
final_pivot as (
    select 
        page_key
        {% for ce in var('conversion_events',[]) %}
        , ifnull(conversion_{{ce}}.conversion_count,0) as {{ce}}_count
        {% endfor %}
    from pk
    {% for ce in var('conversion_events',[]) %}
    left join conversion_{{ce}} using (page_key)
    {% endfor %}
)

select * from final_pivot

