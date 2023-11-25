-- Event defined as "when the app is in the foreground or webpage is in focus for at least one second."
 
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

 with user_engagement_with_params as (
   select *
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("user_engagement_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("user_engagement_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'user_engagement'
 {% if is_incremental() %}
  and event_date_dt >= CURRENT_DATE() - 7
 {% endif %}
)

select * from user_engagement_with_params