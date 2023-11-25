-- reference here: https://support.google.com/analytics/answer/9216061?hl=en 

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

 with event_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'search_term') }},
      {{ ga4.unnest_key('event_params', 'unique_search_term') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("view_search_results_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("view_search_results_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'view_search_results'
 {% if is_incremental() %}
  and event_date_dt >= CURRENT_DATE() - 7
 {% endif %}
)

select * from event_with_params