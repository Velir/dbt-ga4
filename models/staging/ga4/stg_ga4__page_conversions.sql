{{ config(
  enabled= var('conversion_events', false) != false
) }}
with events as (
    select 
        page_key,
        event_name,
        session_key,
        1 as event_count,
    from {{ref('stg_ga4__events')}}
)
-- For loop that creates 1 cte per conversions, grouped by page_location
{% for ce in var('conversion_events',[]) %}
,
conversion_{{ce}} as (
    select
        page_key,
        sum(event_count) as conversion_count,
        count(distinct session_key) as distinct_conversion_count
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
        , ifnull(conversion_{{ce}}.distinct_conversion_count,0) as {{ce}}_count_distinct
        {% endfor %}
    from events
    {% for ce in var('conversion_events',[]) %}
    left join conversion_{{ce}} using (page_key)
    {% endfor %}
)

select * from final_pivot

