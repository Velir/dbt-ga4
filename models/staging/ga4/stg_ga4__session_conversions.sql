{{ config(
  enabled= var('conversion_events', false) != false
) }}

with events as (
    select 
        1 as event_count, 
        session_key, 
        event_name
    from {{ref('stg_ga4__events')}}
),
sessions as (
    select 
        distinct session_key
    from events
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
        session_key
        {% for ce in var('conversion_events',[]) %}
        , ifnull(conversion_{{ce}}.conversion_count,0) as {{ce}}_count
        {% endfor %}
    from sessions
    {% for ce in var('conversion_events',[]) %}
    left join conversion_{{ce}} using (session_key)
    {% endfor %}
)

select * from final_pivot

