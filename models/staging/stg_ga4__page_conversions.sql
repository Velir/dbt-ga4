{{ config(
  enabled= var('conversion_events', false) != false,
  materialized='table'
) }}

select 
    page_key
    {% for ce in var('conversion_events',[]) %}
    , countif(event_name = '{{ce}}') as {{ce}}_count
    {% endfor %}
from {{ref('stg_ga4__events')}}
{% if is_incremental() %}
  where event_date_dt >= CURRENT_DATE() - 7
{% endif %}
group by 1