-- TODO: Unclear why there are first_visit events firing when the ga_session_number is >1. This might cause confusion.

{{
  config(
       materialized='incremental',
        
        incremental_strategy='insert_overwrite',
        partition_by={
                        "field": "event_date_dt",
                        "data_type": "date",
                        "granularity": "day"
                    },
  )
}}

with first_visit_with_params as (
 select 
    *,
    {{ ga4.unnest_key('event_params', 'page_location', 'string_value', 'landing_page') }} 
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
      {% if var("first_visit_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("first_visit_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'first_visit'
 {% if is_incremental() %}
  and event_date_dt >= CURRENT_DATE() - 7
 {% endif %}
)

select * from first_visit_with_params