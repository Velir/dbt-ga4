{{ config(
  enabled= var('conversion_events', false) != false
) }}

with event_counts as (
    select 
        session_key
        {% for ce in var('conversion_events',[]) %}
        , countif(event_name = '{{ce}}') as {{ce}}_count
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    group by session_key
)

select * from event_counts
