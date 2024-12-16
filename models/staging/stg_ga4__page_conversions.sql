{{ config(
  enabled= var('conversion_events', false) != false
) }}

select 
    page_key
    {% for ce in var('conversion_events',[]) %}
    , countif(event_name = '{{ce}}') as {{ga4.valid_column_name(ce)}}_count
    {% endfor %}
from {{ref('stg_ga4__events')}}
group by 1